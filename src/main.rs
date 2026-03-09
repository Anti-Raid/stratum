mod config;
mod eventparse;

use std::{pin::Pin, sync::{Arc, RwLock}};
use serde::Deserialize;
use tokio::{signal, sync::watch, sync::mpsc};
use twilight_model::id::{Id, marker::GuildMarker};
use std::io::Write;
use twilight_gateway::{
    CloseFrame, Config, Event, EventTypeFlags, Intents, Message, Shard
};
use twilight_http::Client;
use futures_util::{Stream, StreamExt};
use tonic::Status;

pub type Error = Box<dyn std::error::Error + Send + Sync>; // This is constant and should be copy pasted

/// Internal transport layer
mod pb {
    tonic::include_proto!("stratum");
}

impl pb::Id {
    pub fn from_tenant_id(tenant_id: TenantId) -> Self {
        match tenant_id {
            TenantId::Guild(guild_id) => Self {
                tenant_id: guild_id.get(),
                tenant_type: pb::TenantType::Guild as i32,
            },
        }
    }
}

// Validates workers
impl pb::Worker {
    pub fn validate(&self) -> Result<(), crate::Error> {
        // For now we just check that the worker ID is within bounds, but we can add more validation later if needed
        if self.worker_id as usize >= config::CONFIG.num_workers {
            return Err(format!("Worker ID {} is out of bounds for number of workers {}", self.worker_id, config::CONFIG.num_workers).into());
        }

        if self.grpc_access_key != config::CONFIG.grpc_access_key {
            return Err("Invalid gRPC access key".into());
        }

        Ok(())
    }
}

/// Core ID struct
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum TenantId {
    Guild(Id<GuildMarker>),
}

impl TenantId {
    /// Returns a the worker ID given tenant ID
    pub fn worker_id(self, num_workers: usize) -> usize {
        match self {
            // This is safe as AntiRaid workers does not currently support 32 bit platforms
            TenantId::Guild(guild_id) => (guild_id.get() >> 22) as usize % num_workers,
        }
    }
}


/// Worker struct that holds all the connections for a given shard and handles sending events to them
pub struct Worker {
    conn_txs: RwLock<Vec<mpsc::UnboundedSender<Result<pb::DiscordEvent, Status>>>>,
}

impl Worker {
    pub fn new() -> Self {
        Self {
            conn_txs: RwLock::new(Vec::new()),
        }
    }

    pub fn add_connection(&self, tx: mpsc::UnboundedSender<Result<pb::DiscordEvent, Status>>) {
        let mut conn_txs = self.conn_txs.write().unwrap();
        conn_txs.push(tx);
    }

    pub fn send_event(&self, event: pb::DiscordEvent) {
        let conn_txs = self.conn_txs.read().unwrap();
        match conn_txs.len() {
            0 => return, // no connections, drop the event
            1 => {
                let _ = conn_txs[0].send(Ok(event)); // only one connection, send directly without needing to clone
            }
            _ => {
                for tx in conn_txs.iter() {
                    let _ = tx.send(Ok(event.clone()));
                }
            }
        }
    }
}

/// A set of workers, sharded by tenant ID
pub struct WorkerSet {
    workers: Vec<Arc<Worker>>,
}

impl WorkerSet {
    /// Creates a new WorkerSet with the given number of workers
    pub fn new(num_workers: usize) -> Self {
        let mut workers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            workers.push(Arc::new(Worker::new()));
        }
        Self { workers }
    }

    /// Returns the worker for the given tenant ID
    pub fn get_worker(&self, id: TenantId) -> Arc<Worker> {
        let worker_id = id.worker_id(self.workers.len());
        Arc::clone(&self.workers[worker_id])
    }
}

#[derive(Clone)]
pub struct CommonState {
    pub cache: Arc<twilight_cache_inmemory::InMemoryCache>,
    pub workers: Arc<WorkerSet>,
}

impl CommonState {
    pub fn new() -> Self {
        let cache = twilight_cache_inmemory::DefaultInMemoryCache::builder()
        .message_cache_size(100)
        .build();
        let worker_set = WorkerSet::new(config::CONFIG.num_workers);
        Self {
            cache: Arc::new(cache),
            workers: Arc::new(worker_set),
        }
    }
}

#[derive(Clone)]
pub struct StratumServer {
    common_state: CommonState,
}

impl StratumServer {
    pub fn new(common_state: CommonState) -> Self {
        Self { common_state }
    }

    /// Starts the server, blocking the current thread
    pub async fn start(&self) -> Result<(), Error> {
        let addr = config::CONFIG.grpc_address.parse()?;
        let svc = pb::stratum_server::StratumServer::new(self.clone());
        log::info!("Starting gRPC server on {}", addr);
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl pb::stratum_server::Stratum for StratumServer {
    type EventStreamStream = Pin<Box<dyn Stream<Item = Result<pb::DiscordEvent, Status>> + Send>>;

    async fn event_stream(&self, request: tonic::Request<pb::Worker>) -> Result<tonic::Response<Self::EventStreamStream>, Status> {
        let worker = request.into_inner();
        worker.validate().map_err(|e| Status::unauthenticated(format!("Worker validation failed: {}", e)))?;

        let (tx, rx) = mpsc::unbounded_channel();
        self.common_state.workers.get_worker(TenantId::Guild(Id::new(0))).add_connection(tx);
        let rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(rx) as Self::EventStreamStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut env_builder = env_logger::builder();

    env_builder
        .format(move |buf, record| {
            writeln!(
                buf,
                "({}) {} - {}",
                record.target(),
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    env_builder.init();

    // Deref config to trigger loading it before starting up the proxy service
    let _ = &*config::CONFIG;

    // Select rustls backend
    rustls::crypto::aws_lc_rs::default_provider().install_default().unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let client = Arc::new(Client::new(config::CONFIG.token.clone()));
    let config = Config::new(config::CONFIG.token.clone(), Intents::from_bits(config::CONFIG.intents).expect("Invalid intents in config"));
    let common_state = CommonState::new();

    let tasks = twilight_gateway::create_recommended(&client, config, |_, builder| builder.build())
        .await?
        .map(|shard| tokio::spawn(dispatcher(shard, shutdown_rx.clone(), common_state.clone())))
        .collect::<Vec<_>>();

    signal::ctrl_c().await?;
    _ = shutdown_tx.send(true);

    for task in tasks {
        _ = task.await;
    }

    Ok(())
}

// Core dispatch loop
async fn dispatcher(mut shard: Shard, mut shutdown: watch::Receiver<bool>, common_state: CommonState) {
    log::info!("Starting shard with ID: {}", shard.id());
    loop {
        tokio::select! {
            _ = shutdown.changed() => shard.close(CloseFrame::NORMAL),
            Some(item) = shard.next() => {
                let msg = match item {
                    Ok(msg) => msg,
                    Err(source) => {
                        log::error!("Error receiving event for shard with ID: {}: {}", shard.id(), source);
                        continue;
                    }
                };

                let json_str = match msg {
                    Message::Close(frame) => {
                        log::warn!("Shard with ID: {} received close frame: {:?}", shard.id(), frame);
                        if *shutdown.borrow() {
                            log::info!("Shard with ID: {} is shutting down gracefully", shard.id());
                            break;
                        }
                        continue;
                    }
                    Message::Text(json_str) => json_str
                };

                let (event, event_name) = match eventparse::parse(&json_str, EventTypeFlags::all()) {
                    Ok((event, event_name)) => {
                        let Some(event_name) = event_name else {
                            log::warn!("Received event with unknown type for shard with ID: {}: {}", shard.id(), json_str);
                            continue;
                        };
                        (event, event_name)
                    },
                    Err(e) => {
                        log::error!("Error parsing event for shard with ID: {}: {}", shard.id(), e);
                        continue;
                    }
                };

                let parsed_event: Option<Event> = match event {
                    Some(event) => {
                        // Try to convert to concrete event type
                        match event.try_into() {
                            Ok(event) => {
                                common_state.cache.update(&event);
                                Some(event)
                            },
                            Err(e) => {
                                log::error!("Failed to convert gateway event for shard with ID: {} with error: {}", shard.id(), e);
                                None
                            }
                        }
                    },
                    None => None, // unknown event, use wildcard parsing
                };

                let Some(guild_id) = deduce_guild_id(&json_str, &parsed_event) else {
                    // ignore events we can't deduce a guild_id for, since we won't know which tenant to route them to
                    //
                    // TODO: Change this when we support user-installed apps in stratum
                    continue; 
                };

                let tenant_id = TenantId::Guild(guild_id);
                let worker = common_state.workers.get_worker(tenant_id);
                worker.send_event(pb::DiscordEvent {
                    event_name,
                    payload: json_str,
                    id: Some(pb::Id::from_tenant_id(tenant_id)),
                });
            }
        }
    }
}

/// Helper method to deduce the guild_id from an event, if possible
fn deduce_guild_id(raw_json: &str, parsed_event: &Option<Event>) -> Option<Id<GuildMarker>> {
    if let Some(event) = parsed_event {
        // Not handled by twilight's event.guild_id()
        match event {
            Event::EntitlementCreate(e) => return e.guild_id,
            Event::EntitlementDelete(e) => return e.guild_id,
            Event::EntitlementUpdate(e) => return e.guild_id,
            _ => {}
        };
        return event.guild_id(); // Directly use event.guild_id()
    }

    // Fallback to wildcard parsing to try extracting guild id from event
    #[derive(Deserialize)]
    struct GuildIdData {
        guild_id: Id<GuildMarker>,
    }

    #[derive(Deserialize)]
    struct GuildIdWrapper {
        d: GuildIdData,
    }
    
    if let Ok(wrapper) = serde_json::from_str::<GuildIdWrapper>(raw_json) {
        return Some(wrapper.d.guild_id);
    }

    // If we can't find a guild_id, return None and ignore the event, since we won't know which tenant to route it to
    None
}