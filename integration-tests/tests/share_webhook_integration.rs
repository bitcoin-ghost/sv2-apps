//! Integration test for native SRI Pool share webhook functionality.
//!
//! Tests the complete mining flow:
//! 1. Template Provider provides block templates
//! 2. Pool receives templates and serves miners
//! 3. Miners submit shares
//! 4. Pool validates shares and sends webhook notifications
//! 5. Mock webhook server receives batched share data
//! 6. Block found and propagated

use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Json, Router,
};
use integration_tests_sv2::{
    interceptor::MessageDirection,
    start_minerd, start_pool_with_webhook, start_sniffer, start_sv2_translator,
    start_template_provider, start_tracing, sv2_tp_config,
    template_provider::DifficultyLevel, ShareWebhookConfig,
};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use stratum_apps::stratum_core::mining_sv2::*;
use tokio::sync::RwLock;

/// Share data structure matching the pool's webhook payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareData {
    pub timestamp_ms: u64,
    pub share_hash: String,
    pub share_work: f64,
    pub channel_id: u32,
    pub sequence_number: u32,
    pub job_id: u32,
    pub downstream_id: usize,
    pub is_block: bool,
    /// User identity string (format: <payout_address>.<worker_name>)
    #[serde(default)]
    pub user_identity: String,
}

/// Batch of shares from webhook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareBatch {
    pub pool_id: u16,
    pub batch_seq: u64,
    pub shares: Vec<ShareData>,
}

/// State for the mock webhook server
#[derive(Debug)]
struct WebhookState {
    /// Total shares received
    total_shares: AtomicU64,
    /// Total batches received
    total_batches: AtomicU64,
    /// Block-finding shares received
    blocks_found: AtomicU64,
    /// All received shares for verification
    shares: RwLock<Vec<ShareData>>,
}

impl WebhookState {
    fn new() -> Self {
        Self {
            total_shares: AtomicU64::new(0),
            total_batches: AtomicU64::new(0),
            blocks_found: AtomicU64::new(0),
            shares: RwLock::new(Vec::new()),
        }
    }
}

/// Handler for the /api/internal/shares endpoint
async fn share_batch_handler(
    State(state): State<Arc<WebhookState>>,
    Json(batch): Json<ShareBatch>,
) -> (StatusCode, Json<serde_json::Value>) {
    let share_count = batch.shares.len();
    let blocks_in_batch = batch.shares.iter().filter(|s| s.is_block).count();

    tracing::info!(
        pool_id = batch.pool_id,
        batch_seq = batch.batch_seq,
        share_count,
        blocks_in_batch,
        "Webhook received share batch"
    );

    // Update counters
    state.total_shares.fetch_add(share_count as u64, Ordering::Relaxed);
    state.total_batches.fetch_add(1, Ordering::Relaxed);
    state.blocks_found.fetch_add(blocks_in_batch as u64, Ordering::Relaxed);

    // Store shares for verification
    {
        let mut shares = state.shares.write().await;
        shares.extend(batch.shares);
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ok",
            "recorded": share_count
        })),
    )
}

/// Start a mock webhook server and return its address
async fn start_webhook_server() -> (Arc<WebhookState>, SocketAddr) {
    let state = Arc::new(WebhookState::new());
    let state_clone = state.clone();

    let app = Router::new()
        .route("/api/internal/shares", post(share_batch_handler))
        .with_state(state_clone);

    // Find an available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind webhook server");
    let addr = listener.local_addr().expect("Failed to get local address");

    tracing::info!("Starting mock webhook server on {}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("Webhook server failed");
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (state, addr)
}

/// Test that shares are properly sent to the webhook endpoint
#[tokio::test]
async fn test_share_webhook_receives_valid_shares() {
    
    start_tracing();

    // 1. Start the mock webhook server
    let (webhook_state, webhook_addr) = start_webhook_server().await;
    let webhook_url = format!("http://{}/api/internal/shares", webhook_addr);
    tracing::info!("Webhook URL: {}", webhook_url);

    // 2. Create webhook configuration with small batch size for faster testing
    let webhook_config = ShareWebhookConfig {
        url: webhook_url,
        batch_size: 5, // Small batch size for testing
        batch_timeout_ms: 2000, // 2 second timeout
        max_retries: 3,
    };

    // 3. Start template provider (regtest = every share is a block)
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    tracing::info!("Template Provider started on {}", tp_addr);

    // 4. Start pool with webhook enabled
    let (_pool, pool_addr) = start_pool_with_webhook(
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        Some(webhook_config),
    )
    .await;
    tracing::info!("Pool started on {} with webhook enabled", pool_addr);

    // 5. Start sniffer to monitor messages
    let (sniffer, sniffer_addr) = start_sniffer("webhook-test", pool_addr, false, vec![], None);

    // 6. Start translator and miner
    let (_translator, tproxy_addr) =
        start_sv2_translator(&[sniffer_addr], false, vec![], vec![], None).await;
    tracing::info!("Translator started on {}", tproxy_addr);

    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;
    tracing::info!("Miner started");

    // 7. Wait for shares to be submitted and acknowledged
    tracing::info!("Waiting for shares to be submitted...");
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;
    tracing::info!("First share acknowledgment received");

    // 8. Wait for more shares to accumulate
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 9. Verify webhook received shares
    let total_shares = webhook_state.total_shares.load(Ordering::Relaxed);
    let total_batches = webhook_state.total_batches.load(Ordering::Relaxed);
    let blocks_found = webhook_state.blocks_found.load(Ordering::Relaxed);

    tracing::info!(
        "Webhook stats: {} shares in {} batches, {} blocks found",
        total_shares,
        total_batches,
        blocks_found
    );

    // Verify we received at least some shares
    assert!(
        total_shares > 0,
        "Expected webhook to receive shares, got 0"
    );

    // Verify batches were received
    assert!(
        total_batches > 0,
        "Expected webhook to receive batches, got 0"
    );

    // In regtest (DifficultyLevel::Low), every share should be a block
    assert!(
        blocks_found > 0,
        "Expected blocks to be found in regtest mode, got 0"
    );

    // Verify share data structure
    let shares = webhook_state.shares.read().await;
    if let Some(first_share) = shares.first() {
        tracing::info!("First share: {:?}", first_share);
        assert!(first_share.timestamp_ms > 0, "Share should have timestamp");
        assert!(!first_share.share_hash.is_empty(), "Share should have hash");
        assert!(first_share.share_work > 0.0, "Share should have work value");
    }

    tracing::info!("Share webhook integration test PASSED!");
}

/// Test that webhook batching works correctly (timeout triggers partial batch)
#[tokio::test]
async fn test_share_webhook_batch_timeout() {
    
    start_tracing();

    // Start webhook server
    let (webhook_state, webhook_addr) = start_webhook_server().await;
    let webhook_url = format!("http://{}/api/internal/shares", webhook_addr);

    // Large batch size so we rely on timeout
    let webhook_config = ShareWebhookConfig {
        url: webhook_url,
        batch_size: 1000, // Large - won't fill in test
        batch_timeout_ms: 1000, // 1 second timeout
        max_retries: 3,
    };

    // Start infrastructure
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_pool, pool_addr) = start_pool_with_webhook(
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        Some(webhook_config),
    )
    .await;
    let (sniffer, sniffer_addr) = start_sniffer("timeout-test", pool_addr, false, vec![], None);
    let (_translator, tproxy_addr) =
        start_sv2_translator(&[sniffer_addr], false, vec![], vec![], None).await;
    let (_minerd_process, _) = start_minerd(tproxy_addr, None, None, false).await;

    // Wait for first share
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;

    // Wait for timeout to trigger batch send (batch_timeout_ms = 1000)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Should have received at least one batch via timeout
    let total_batches = webhook_state.total_batches.load(Ordering::Relaxed);
    assert!(
        total_batches > 0,
        "Expected timeout to trigger batch send, got {} batches",
        total_batches
    );

    tracing::info!("Batch timeout test PASSED! {} batches received", total_batches);
}

/// Test that mining continues even when webhook is unavailable
#[tokio::test]
async fn test_mining_continues_without_webhook() {
    
    start_tracing();

    // Configure webhook pointing to non-existent server
    let webhook_config = ShareWebhookConfig {
        url: "http://127.0.0.1:59999/api/internal/shares".to_string(), // Intentionally invalid
        batch_size: 5,
        batch_timeout_ms: 1000,
        max_retries: 1, // Low retries for faster test
    };

    // Start infrastructure
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_pool, pool_addr) = start_pool_with_webhook(
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        Some(webhook_config),
    )
    .await;
    let (sniffer, sniffer_addr) = start_sniffer("resilience-test", pool_addr, false, vec![], None);
    let (_translator, tproxy_addr) =
        start_sv2_translator(&[sniffer_addr], false, vec![], vec![], None).await;
    let (_minerd_process, _) = start_minerd(tproxy_addr, None, None, false).await;

    // Mining should work even with broken webhook
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;

    tracing::info!("Mining resilience test PASSED - mining continued despite webhook failure");
}

/// Full end-to-end test: mining, shares, block found, coinbase
#[tokio::test]
async fn test_full_mining_flow_with_webhook() {
    
    start_tracing();

    // Start webhook server
    let (webhook_state, webhook_addr) = start_webhook_server().await;
    let webhook_url = format!("http://{}/api/internal/shares", webhook_addr);

    let webhook_config = ShareWebhookConfig {
        url: webhook_url,
        batch_size: 1, // Send immediately for faster verification
        batch_timeout_ms: 500,
        max_retries: 3,
    };

    // Start template provider (regtest - every valid share is a block!)
    let (_tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    tracing::info!("Template Provider started (regtest mode)");

    // Start pool with webhook
    let (_pool, pool_addr) = start_pool_with_webhook(
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        Some(webhook_config),
    )
    .await;
    tracing::info!("Pool started with webhook enabled");

    // Start sniffer to verify block propagation
    let (sniffer, sniffer_addr) = start_sniffer("block-test", pool_addr, false, vec![], None);

    // Start translator and miner
    let (_translator, tproxy_addr) =
        start_sv2_translator(&[sniffer_addr], false, vec![], vec![], None).await;
    let (_minerd_process, _) = start_minerd(tproxy_addr, None, None, false).await;
    tracing::info!("Miner started - beginning mining");

    // Wait for share acknowledgments (in regtest, these are blocks!)
    tracing::info!("Waiting for first share/block...");
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;
    tracing::info!("First share acknowledged!");

    // Continue mining for more data
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify webhook received shares and blocks
    let blocks_found = webhook_state.blocks_found.load(Ordering::Relaxed);
    let total_shares = webhook_state.total_shares.load(Ordering::Relaxed);
    let total_batches = webhook_state.total_batches.load(Ordering::Relaxed);

    tracing::info!(
        "Mining complete: {} shares via webhook in {} batches ({} marked as blocks)",
        total_shares,
        total_batches,
        blocks_found
    );

    // Verify shares were received
    assert!(
        total_shares > 0,
        "Expected shares via webhook, got 0"
    );

    // In regtest (DifficultyLevel::Low), shares ARE blocks
    assert!(
        blocks_found > 0,
        "Expected block notifications via webhook in regtest mode"
    );

    // Verify share data structure
    let shares = webhook_state.shares.read().await;
    let block_shares: Vec<_> = shares.iter().filter(|s| s.is_block).collect();

    tracing::info!("Block-finding shares received:");
    for (i, share) in block_shares.iter().take(3).enumerate() {
        tracing::info!(
            "  Block {}: hash={}, work={}, job_id={}",
            i + 1,
            &share.share_hash[..16],
            share.share_work,
            share.job_id
        );
    }

    assert!(
        !block_shares.is_empty(),
        "Expected shares marked as blocks"
    );

    tracing::info!(
        "\n========================================\n\
         FULL MINING FLOW TEST PASSED!\n\
         ========================================\n\
         - Total shares received: {}\n\
         - Batches received: {}\n\
         - Blocks found: {}\n\
         - Webhook working: YES\n\
         ========================================",
        total_shares,
        total_batches,
        blocks_found
    );
}
