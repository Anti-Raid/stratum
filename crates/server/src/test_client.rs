//! Test client for stratum

use futures_util::StreamExt;
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

    client.shard_ready_stream_wait(|evt| {
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

async fn client_stub_worker(client: Arc<stratum_client::StratumClient>, wid: u32, mut shutdown: watch::Receiver<bool>) {
    let mut stream = client.event_stream(wid).await.expect("Failed to fetch event stream");
    log::info!("Started event stream");
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                log::debug!("Closing client");
            },
            evt = stream.next() => {
                let Some(evt) = evt else {
                    continue;
                };
                match evt {
                    Ok(evt) => {
                        let value = serde_json::from_str::<serde_json::Value>(&evt.payload);
                        log::info!("Got event: {} json_ok({})", evt.event_name, value.is_ok());
                    },
                    Err(e) => {
                        log::error!("Error: {e}");
                        // Make a new stream, dropping the existing one
                        drop(stream);
                        stream = client.event_stream(wid).await.expect("Failed to fetch event stream");
                    }
                }
            }
        }
    }
}