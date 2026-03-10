//! Test client for stratum

use futures_util::StreamExt;
use tokio::signal;
use tokio::sync::watch;

use crate::server::pb;
use crate::config::CONFIG;

fn oauth() -> pb::OtherAuthorized {
    pb::OtherAuthorized {
        grpc_access_key: CONFIG.grpc_access_key.clone()    
    }
}

fn worker(wid: u32) -> pb::Worker {
    pb::Worker {
        worker_id: wid,
        grpc_access_key: CONFIG.grpc_access_key.clone()    
    }
}


pub async fn client() -> Result<(), crate::Error> {
    log::info!("Connecting to stratum...");
    let uri = tonic::transport::Endpoint::from_shared(format!("http://{}", CONFIG.grpc_address))?;
    let mut client = pb::stratum_client::StratumClient::connect(uri).await?;
    
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let config = client.get_config(oauth()).await?;
    let num_workers = config.into_inner().num_workers;
    log::info!("Got stratum config, num workers: {num_workers}");

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

async fn client_stub_worker(mut client: pb::stratum_client::StratumClient<tonic::transport::Channel>, wid: u32, mut shutdown: watch::Receiver<bool>) {
    let mut stream = client.event_stream(worker(wid)).await.expect("Failed to fetch event stream").into_inner();
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
                        log::info!("Got event: {}", evt.event_name);
                    },
                    Err(e) => {
                        log::error!("Error: {e}");
                        // Make a new stream, dropping the existing one
                        drop(stream);
                        stream = client.event_stream(worker(wid)).await.expect("Failed to fetch event stream").into_inner();
                    }
                }
            }
        }
    }
}