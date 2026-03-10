use std::{pin::Pin, sync::{Arc, RwLock}, time::Duration};
use serde::Deserialize;
use tokio::{signal, sync::watch, sync::mpsc};
use twilight_cache_inmemory::model::CachedGuild;
use twilight_model::{channel::Channel, gateway::payload::incoming::{ChannelUpdate, GuildUpdate}, guild::PartialGuild, id::{Id, marker::GuildMarker}};
use twilight_gateway::{
    CloseFrame, Config, Event, EventTypeFlags, Intents, Message, Shard, ShardState
};
use twilight_http::Client;
use futures_util::{Stream, StreamExt};
use tonic::Status;
use crate::config::CONFIG;

/// Internal transport layer
mod pb {
    tonic::include_proto!("stratum");
}

fn encode_any<T: serde::Serialize>(msg: &T) -> Result<Vec<u8>, crate::Error> {
    let bytes = rmp_serde::encode::to_vec(msg)
        .map_err(|e| format!("Failed to serialize Mesophyll any: {}", e))?;
    Ok(bytes)
}

fn decode_any<T: for<'de> serde::Deserialize<'de>>(msg: &[u8]) -> Result<T, crate::Error> {
    let decoded: T = rmp_serde::from_slice(msg)
        .map_err(|e| format!("Failed to deserialize Mesophyll any: {}", e))?;
    Ok(decoded)
}

impl pb::AnyValue {
    pub fn from_real<T: serde::Serialize>(value: &T) -> Result<Self, Status> {
        Self::from_real_exec(value).map_err(|e| Status::internal(e.to_string()))
    }

    pub fn from_real_exec<T: serde::Serialize>(value: &T) -> Result<Self, crate::Error> {
        let data = encode_any(value)
            .map_err(|e| format!("Failed to encode response value: {}", e))?;
        Ok(Self { data })
    }

    pub fn to_real<T: for<'de> serde::Deserialize<'de>>(&self) -> Result<T, Status> {
        self.to_real_exec().map_err(|e| Status::internal(e.to_string()))
    }

    pub fn to_real_exec<T: for<'de> serde::Deserialize<'de>>(&self) -> Result<T, crate::Error> {
        let val = decode_any(&self.data)
            .map_err(|e| format!("Failed to decode request value: {}", e))?;
        Ok(val)
    }
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
        if self.worker_id as usize >= CONFIG.num_workers {
            return Err(format!("Worker ID {} is out of bounds for number of workers {}", self.worker_id, CONFIG.num_workers).into());
        }

        if self.grpc_access_key != CONFIG.grpc_access_key {
            return Err("Invalid gRPC access key".into());
        }

        Ok(())
    }
}

// Validates other authorized requests
impl pb::OtherAuthorized {
    pub fn validate(&self) -> Result<(), crate::Error> {
        if self.grpc_access_key != CONFIG.grpc_access_key {
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
    pub fn get_worker_for_tenant(&self, id: TenantId) -> Arc<Worker> {
        let worker_id = id.worker_id(self.workers.len());
        self.get_worker_by_id(worker_id)
    }

    /// Returns the worker for the given tenant ID
    pub fn get_worker_by_id(&self, wid: usize) -> Arc<Worker> {
        Arc::clone(&self.workers[wid])
    }
}

/// Shard data struct, which holds the cache for a shard and other relevant data that might be needed globally by Stratum
pub struct ShardData {
    collected_data: RwLock<Option<CollectedShardData>>,
}

impl ShardData {
    pub fn new() -> Self {
        Self {
            collected_data: RwLock::new(None),
        }
    }

    /// Updates the collected shard data for the shard, which is used for statistics and other global data needs
    pub fn update(&self, collected_data: CollectedShardData) {
        let mut data = self.collected_data.write().unwrap();
        *data = Some(collected_data);
    }
}

/// A set of shard data, sharded by shard ID
pub struct ShardDataSet {
    shard_data: Vec<Arc<ShardData>>,
}

impl ShardDataSet {
    pub fn new(num_shards: usize) -> Self {
        let mut shard_data = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shard_data.push(Arc::new(ShardData::new()));
        }
        Self { shard_data }
    }

    pub fn get_shard_data(&self, shard_id: u32) -> Arc<ShardData> {
        Arc::clone(&self.shard_data[shard_id as usize])
    }
}

#[derive(Clone, Copy)]
pub struct CollectedShardData {
    pub shard_id: u32,
    pub latency: Option<std::time::Duration>,
    pub state: ShardState,
}

impl CollectedShardData {
    pub fn from_shard(shard: &Shard) -> Self {
        Self {
            shard_id: shard.id().number(),
            latency: shard.latency().average(),
            state: shard.state(),
        }
    }
}

#[derive(Clone)]
pub struct CommonState {
    pub workers: Arc<WorkerSet>,
    pub shards: Arc<ShardDataSet>,
    pub cache: Arc<twilight_cache_inmemory::InMemoryCache>,
}

impl CommonState {
    pub fn new(num_shards: usize) -> Self {
        let worker_set = WorkerSet::new(CONFIG.num_workers);
        let shard_data_set = ShardDataSet::new(num_shards);

        let cache = twilight_cache_inmemory::DefaultInMemoryCache::builder()
        .message_cache_size(100)
        .build();

        Self {
            workers: Arc::new(worker_set),
            shards: Arc::new(shard_data_set),
            cache: Arc::new(cache)
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
    pub async fn start(&self, shutdown: watch::Receiver<bool>) -> Result<(), crate::Error> {
        let addr = CONFIG.grpc_address.parse()?;
        let svc = pb::stratum_server::StratumServer::new(self.clone());
        log::info!("Starting gRPC server on {}", addr);
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async {
                // Wait for the shutdown signal
                let mut rx = shutdown.clone();
                while rx.changed().await.is_ok() {
                    if *rx.borrow() {
                        break;
                    }
                }
                log::info!("gRPC server received shutdown signal");
            })
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
        self.common_state.workers.get_worker_by_id(worker.worker_id as usize).add_connection(tx);
        let rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(tonic::Response::new(Box::pin(rx) as Self::EventStreamStream))
    }

    async fn get_status(&self, request: tonic::Request<pb::OtherAuthorized>) -> Result<tonic::Response<pb::Status>, Status> {
        let other = request.into_inner();
        other.validate().map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
        let mut shards = Vec::with_capacity(self.common_state.shards.shard_data.len());

        for shard_data in self.common_state.shards.shard_data.iter() {
            let collected_data = {
                let data = shard_data.collected_data.read().unwrap();
                *data
            };

            let Some(collected_data) = collected_data else {
                continue; // if we don't have collected data for the shard, skip it in the status response
            };

            shards.push(pb::ShardStatus {
                shard_id: collected_data.shard_id,
                latency: collected_data.latency.map(|d| {
                    // Convert latency to milliseconds as a float (copy pasted from currently unstable as_millis_f64)
                    const MILLIS_PER_SEC: u64 = 1_000;
                    const NANOS_PER_MILLI: u32 = 1_000_000;
                    (d.as_secs() as f64) * (MILLIS_PER_SEC as f64)
                    + (d.subsec_nanos() as f64) / (NANOS_PER_MILLI as f64)
                }).unwrap_or(-1.0) as f64,
                state: match collected_data.state {
                    ShardState::Resuming => pb::ShardState::Resuming as i32,
                    ShardState::Identifying => pb::ShardState::Identifying as i32,
                    ShardState::FatallyClosed => pb::ShardState::FatallyClosed as i32,
                    ShardState::Disconnected { reconnect_attempts: _ } => pb::ShardState::Disconnected as i32,
                    ShardState::Active => pb::ShardState::Active as i32,
                },
            });
        }

        Ok(tonic::Response::new(pb::Status {
            shards,
            guild_count: self.common_state.cache.stats().guilds().try_into().map_err(|_| Status::internal("Guild count exceeds u64 max"))?,
            user_count: self.common_state.cache.stats().users().try_into().map_err(|_| Status::internal("User count exceeds u64 max"))?,
        }))
    }

    async fn get_resource_from_cache(&self, request: tonic::Request<pb::GetResourceRequest>) -> Result<tonic::Response<pb::AnyValue>, Status> {
        let ccr = request.into_inner();
        let typ = ccr.r#type();
        let Some(other) = ccr.auth else {
            return Err(Status::unauthenticated(format!("No other found")));
        };
        other.validate().map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
        
        match typ {
            pb::ResourceType::RChannel => {
                let id = Id::new_checked(ccr.id)
                .ok_or_else(|| Status::invalid_argument("Missing channel info in request"))?;

                let chan = match self.common_state.cache.channel(id) {
                    Some(chan) => pb::AnyValue::from_real(chan.value()),
                    None => pb::AnyValue::from_real(&None::<Channel>)
                }?;

                Ok(tonic::Response::new(chan))
            }
            pb::ResourceType::RGuild => {
                let id = Id::new_checked(ccr.id)
                .ok_or_else(|| Status::invalid_argument("Missing channel info in request"))?;

                let g = match self.common_state.cache.guild(id) {
                    Some(g) => pb::AnyValue::from_real(g.value()),
                    None => pb::AnyValue::from_real(&None::<CachedGuild>)
                }?; 

                Ok(tonic::Response::new(g))
            }
        }
    }

    async fn cached_guild_channels(&self, request: tonic::Request<pb::CachedGuildChannelsRequest>) -> Result<tonic::Response<pb::GuildChannelIds>, Status> {
        let ccr = request.into_inner();
        let Some(other) = ccr.auth else {
            return Err(Status::unauthenticated(format!("No other found")));
        };
        other.validate().map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
        let guild_id = Id::new_checked(ccr.guild_id)
        .ok_or_else(|| Status::invalid_argument("Missing guild info in request"))?;

        match self.common_state.cache.guild_channels(guild_id) {
            Some(g) => {
                let chans = Vec::from_iter(g.value().iter().map(|x| x.get()));
                Ok(tonic::Response::new(pb::GuildChannelIds {
                    found: true,
                    channel_ids: chans,
                }))
            },
            None => {
                Ok(tonic::Response::new(pb::GuildChannelIds {
                    found: false,
                    channel_ids: vec![],
                }))
            }
        }
    }

    async fn push_resource_to_cache(&self, request: tonic::Request<pb::PushResourceRequest>) -> Result<tonic::Response<pb::Empty>, Status> {
        let ccr = request.into_inner();
        let typ = ccr.r#type();
        let Some(other) = ccr.auth else {
            return Err(Status::unauthenticated(format!("No other found")));
        };
        other.validate().map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;

        let Some(payload) = ccr.value else {
            return Err(Status::unauthenticated(format!("No payload found")));
        };

        match typ {
            pb::ResourceType::RChannel => {
                let chan = payload.to_real::<Channel>()?;
                self.common_state.cache.update(&ChannelUpdate(chan))
            }
            pb::ResourceType::RGuild => {
                let g = payload.to_real::<PartialGuild>()?;
                self.common_state.cache.update(&GuildUpdate(g))
            }
        }

        Ok(tonic::Response::new(pb::Empty {}))
    }
}

// Core dispatch loop
async fn dispatcher(mut shard: Shard, mut shutdown: watch::Receiver<bool>, common_state: CommonState) {
    log::info!("Starting shard with ID: {}", shard.id());
    let sd = common_state.shards.get_shard_data(shard.id().number());
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = shutdown.changed() => shard.close(CloseFrame::NORMAL),
            _ = ticker.tick() => {
                let collected_data = CollectedShardData::from_shard(&shard);
                sd.update(collected_data);
            }
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

                if let Err(e) = dispatch_single(json_str, &common_state) {
                    log::warn!("dispatch_single on shard {} failed: {e}", shard.id());
                }
            }
        }
    }
}

/// Helper method to actually perform the event dispatch + cache update
fn dispatch_single(event_json: String, common_state: &CommonState) -> Result<(), crate::Error> {
    let (event, event_name) = crate::eventparse::parse(&event_json, EventTypeFlags::all())?;
    let Some(event_name) = event_name else {
        return Err("Received event with unknown type".into());
    };

    let parsed_event: Option<Event> = match event {
        Some(event) => {
            let event = event.into();
            common_state.cache.update(&event);
            Some(event)
        },
        None => None, // unknown event, use wildcard parsing
    };

    if is_internal_event(&event_name) {
        // Don't send internal events to workers
        return Ok(());
    }

    let Some(guild_id) = deduce_guild_id(&event_json, &parsed_event) else {
        // ignore events we can't deduce a guild_id for, since we won't know which tenant to route them to
        //
        // TODO: Change this when we support user-installed apps in stratum
        return Ok(()); 
    };

    let tenant_id = TenantId::Guild(guild_id);
    let worker = common_state.workers.get_worker_for_tenant(tenant_id);
    worker.send_event(pb::DiscordEvent {
        event_name,
        payload: event_json,
        id: Some(pb::Id::from_tenant_id(tenant_id)),
    });

    Ok(())
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

/// Returns true if the event is an internal event that should not be sent to workers, false otherwise
fn is_internal_event(event_name: &str) -> bool {
    [
        "READY", 
        "RESUMED", 
        "GUILD_CREATE", 
        "GUILD_DELETE",
        "RATE_LIMITED", 
    ].contains(&event_name)
}

pub async fn server() -> Result<(), crate::Error> {
    // Select rustls backend
    rustls::crypto::aws_lc_rs::default_provider().install_default().unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let client = Arc::new(Client::new(CONFIG.token.clone()));
    let config = Config::new(CONFIG.token.clone(), Intents::from_bits(CONFIG.intents).expect("Invalid intents in config"));
    
    let shards = twilight_gateway::create_recommended(&client, config, |_, builder| builder.build())
        .await?;
    let common_state = CommonState::new(shards.len());
    let mut tasks = shards
        .map(|shard| tokio::spawn(dispatcher(shard, shutdown_rx.clone(), common_state.clone())))
        .collect::<Vec<_>>();

    // Push grpc server to the very end
    tasks.push(tokio::spawn(async move {
        let srv = StratumServer::new(common_state);
        loop {
            let shutdown = shutdown_rx.clone();
            if let Err(e) = srv.start(shutdown).await {
                log::error!("gRPC server shut down unexpectedly: {e:?}, restarting");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }
    }));

    signal::ctrl_c().await?;
    _ = shutdown_tx.send(true);

    for task in tasks {
        _ = task.await;
    }

    Ok(())
}