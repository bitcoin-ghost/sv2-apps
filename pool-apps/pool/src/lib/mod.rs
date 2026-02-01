use std::{sync::Arc, thread::JoinHandle};

use async_channel::unbounded;

use bitcoin_core_sv2::CancellationToken;
use stratum_apps::{
    stratum_core::bitcoin::consensus::Encodable, task_manager::TaskManager,
    tp_type::TemplateProviderType, SHUTDOWN_BROADCAST_CAPACITY,
};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::{
    channel_manager::ChannelManager,
    config::PoolConfig,
    error::PoolErrorKind,
    share_webhook::{ShareWebhookSender, ShareWebhookWorker},
    status::State,
    template_receiver::{
        bitcoin_core::{connect_to_bitcoin_core, BitcoinCoreSv2Config},
        sv2_tp::Sv2Tp,
    },
    utils::ShutdownMessage,
};

pub mod channel_manager;
pub mod config;
pub mod downstream;
pub mod error;
mod io_task;
mod monitoring;
pub mod share_webhook;
pub mod status;
pub mod template_receiver;
pub mod utils;

#[derive(Debug, Clone)]
pub struct PoolSv2 {
    config: PoolConfig,
    notify_shutdown: broadcast::Sender<ShutdownMessage>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl PoolSv2 {
    pub fn new(config: PoolConfig) -> Self {
        let (notify_shutdown, _) =
            tokio::sync::broadcast::channel::<ShutdownMessage>(SHUTDOWN_BROADCAST_CAPACITY);
        Self {
            config,
            notify_shutdown,
        }
    }

    /// Starts the Pool main loop.
    pub async fn start(&self) -> Result<(), PoolErrorKind> {
        let coinbase_outputs = vec![self.config.get_txout()];
        let mut encoded_outputs = vec![];

        coinbase_outputs
            .consensus_encode(&mut encoded_outputs)
            .expect("Invalid coinbase output in config");

        let notify_shutdown = self.notify_shutdown.clone();

        let task_manager = Arc::new(TaskManager::new());

        let (status_sender, status_receiver) = unbounded();

        let (channel_manager_to_downstream_sender, _channel_manager_to_downstream_receiver) =
            broadcast::channel(10);
        let (downstream_to_channel_manager_sender, downstream_to_channel_manager_receiver) =
            unbounded();

        let (channel_manager_to_tp_sender, channel_manager_to_tp_receiver) = unbounded();
        let (tp_to_channel_manager_sender, tp_to_channel_manager_receiver) = unbounded();

        debug!("Channels initialized.");

        // Initialize share webhook if configured
        let share_webhook_sender: Option<ShareWebhookSender> =
            if let Some(webhook_config) = self.config.share_webhook() {
                let (sender, worker) =
                    ShareWebhookWorker::new(webhook_config.clone(), self.config.server_id());
                info!(
                    url = %webhook_config.url,
                    batch_size = webhook_config.batch_size,
                    "Share webhook configured"
                );
                let shutdown_rx = notify_shutdown.subscribe();
                task_manager.spawn(async move {
                    worker.run(shutdown_rx).await;
                });
                Some(sender)
            } else {
                None
            };

        let channel_manager = ChannelManager::new(
            self.config.clone(),
            channel_manager_to_tp_sender.clone(),
            tp_to_channel_manager_receiver,
            channel_manager_to_downstream_sender.clone(),
            downstream_to_channel_manager_receiver,
            encoded_outputs.clone(),
            share_webhook_sender,
        )
        .await?;

        // Start monitoring server if configured
        if let Some(monitoring_addr) = self.config.monitoring_address() {
            info!(
                "Initializing monitoring server on http://{}",
                monitoring_addr
            );

            let monitoring_server = stratum_apps::monitoring::MonitoringServer::new(
                monitoring_addr,
                None, // Pool doesn't have channels opened with servers
                Some(Arc::new(channel_manager.clone())), // channels opened with clients
            )
            .expect("Failed to initialize monitoring server");

            // Create shutdown signal that waits for ShutdownAll
            let mut notify_shutdown_monitoring = notify_shutdown.subscribe();
            let shutdown_signal = async move {
                loop {
                    match notify_shutdown_monitoring.recv().await {
                        Ok(ShutdownMessage::ShutdownAll) => break,
                        Ok(_) => continue, // Ignore other shutdown messages
                        Err(_) => break,
                    }
                }
            };

            task_manager.spawn(async move {
                if let Err(e) = monitoring_server.run(shutdown_signal).await {
                    error!("Monitoring server error: {}", e);
                }
            });
        }

        let channel_manager_clone = channel_manager.clone();
        let mut bitcoin_core_sv2_join_handle: Option<JoinHandle<()>> = None;

        match self.config.template_provider_type().clone() {
            TemplateProviderType::Sv2Tp {
                address,
                public_key,
            } => {
                let sv2_tp = Sv2Tp::new(
                    address.clone(),
                    public_key,
                    channel_manager_to_tp_receiver,
                    tp_to_channel_manager_sender,
                    notify_shutdown.clone(),
                    task_manager.clone(),
                    status_sender.clone(),
                )
                .await?;

                sv2_tp
                    .start(
                        address,
                        notify_shutdown.clone(),
                        status_sender.clone(),
                        task_manager.clone(),
                    )
                    .await?;

                info!("Sv2 Template Provider setup done");
            }
            TemplateProviderType::BitcoinCoreIpc {
                network,
                data_dir,
                fee_threshold,
                min_interval,
            } => {
                let unix_socket_path =
                    stratum_apps::tp_type::resolve_ipc_socket_path(&network, data_dir)
                        .ok_or_else(|| PoolErrorKind::Configuration(
                            "Could not determine Bitcoin data directory. Please set data_dir in config.".to_string()
                        ))?;

                info!(
                    "Using Bitcoin Core IPC socket at: {}",
                    unix_socket_path.display()
                );

                // incoming and outgoing TDP channels from the perspective of BitcoinCoreSv2
                let incoming_tdp_receiver = channel_manager_to_tp_receiver.clone();
                let outgoing_tdp_sender = tp_to_channel_manager_sender.clone();

                let bitcoin_core_config = BitcoinCoreSv2Config {
                    unix_socket_path,
                    fee_threshold,
                    min_interval,
                    incoming_tdp_receiver,
                    outgoing_tdp_sender,
                    cancellation_token: CancellationToken::new(),
                };

                bitcoin_core_sv2_join_handle = Some(
                    connect_to_bitcoin_core(
                        bitcoin_core_config,
                        notify_shutdown.clone(),
                        task_manager.clone(),
                        status_sender.clone(),
                    )
                    .await,
                );
            }
        }

        channel_manager
            .start(
                notify_shutdown.clone(),
                status_sender.clone(),
                task_manager.clone(),
                coinbase_outputs,
            )
            .await?;

        channel_manager_clone
            .start_downstream_server(
                *self.config.authority_public_key(),
                *self.config.authority_secret_key(),
                self.config.cert_validity_sec(),
                *self.config.listen_address(),
                task_manager.clone(),
                notify_shutdown.clone(),
                status_sender,
                downstream_to_channel_manager_sender,
                channel_manager_to_downstream_sender,
            )
            .await?;

        info!("Spawning status listener task...");
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl+C received — initiating graceful shutdown...");
                    let _ = notify_shutdown.send(ShutdownMessage::ShutdownAll);
                    break;
                }
                message = status_receiver.recv() => {
                    if let Ok(status) = message {
                        match status.state {
                            State::DownstreamShutdown{downstream_id,..} => {
                                warn!("Downstream {downstream_id:?} disconnected — Channel manager.");
                                let _ = notify_shutdown.send(ShutdownMessage::DownstreamShutdown(downstream_id));
                            }
                            State::TemplateReceiverShutdown(_) => {
                                warn!("Template Receiver shutdown requested — initiating full shutdown.");
                                let _ = notify_shutdown.send(ShutdownMessage::ShutdownAll);
                                break;
                            }
                            State::ChannelManagerShutdown(_) => {
                                warn!("Channel Manager shutdown requested — initiating full shutdown.");
                                let _ = notify_shutdown.send(ShutdownMessage::ShutdownAll);
                                break;
                            }
                        }
                    }
                }
            }
        }

        if let Some(bitcoin_core_sv2_join_handle) = bitcoin_core_sv2_join_handle {
            info!("Waiting for BitcoinCoreSv2 dedicated thread to shutdown...");
            match bitcoin_core_sv2_join_handle.join() {
                Ok(_) => info!("BitcoinCoreSv2 dedicated thread shutdown complete."),
                Err(e) => error!("BitcoinCoreSv2 dedicated thread error: {e:?}"),
            }
        }

        warn!("Graceful shutdown");
        task_manager.abort_all().await;
        info!("Joining remaining tasks...");
        task_manager.join_all().await;
        info!("Pool shutdown complete.");
        Ok(())
    }
}

impl Drop for PoolSv2 {
    fn drop(&mut self) {
        info!("PoolSv2 dropped");
        let _ = self.notify_shutdown.send(ShutdownMessage::ShutdownAll);
    }
}
