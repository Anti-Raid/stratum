//! Test client for stratum

use tokio::signal;
use tokio::sync::watch;
use std::sync::Arc;
use crate::config::CONFIG;

pub async fn client() -> Result<(), crate::Error> {
    log::info!("Connecting to stratum...");
    let client = Arc::new(stratum_client::StratumClient::new(&CONFIG.grpc_address, CONFIG.grpc_access_key.clone()).await?);
    
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let config = client.get_config().await?;
    let num_workers = config.num_workers;
    log::info!("Got stratum config, num workers: {num_workers}");

    client.listen_to_stream(client.shard_ready_stream().await?, Some(shutdown_rx.clone()), |evt| {
        log::info!("Shards ready: {:?} ({}/{})", evt.ready_shards, evt.ready_shards.len(), evt.total_shards);
        evt.ready_shards.len() as u32 == evt.total_shards
    }).await?;

    let tasks = (0..num_workers)
        .map(|wid| tokio::spawn(client_stub_worker(client.clone(), wid, shutdown_rx.clone())))
        .collect::<Vec<_>>();

    signal::ctrl_c().await?;
    log::info!("Shutting down test client");
    _ = shutdown_tx.send(true);

    for task in tasks {
        tokio::select! {
            _ = task => {},
            _ = signal::ctrl_c() => {}
        };
    }

    Ok(())
}

async fn client_stub_worker(client: Arc<stratum_client::StratumClient>, wid: u32, shutdown: watch::Receiver<bool>) {
    loop {
        match client_stub_worker_impl(client.clone(), wid, shutdown.clone()).await {
            Ok(_) => break,
            Err(e) => {
                log::error!("Error in stream {e:?}, retrying in 5 seconds");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}

async fn client_stub_worker_impl(client: Arc<stratum_client::StratumClient>, wid: u32, shutdown: watch::Receiver<bool>) -> Result<(), crate::Error> {
    let stream = client.event_stream(wid).await?;
    log::info!("Started event stream");
    client.listen_to_stream(stream, Some(shutdown), |evt| {
        let value = serde_json::from_str::<serde_json::Value>(&evt.payload);
        log::info!("Got event: {} json_ok({})", evt.event_name, value.is_ok());
        false
    }).await?;
    Ok(())
}