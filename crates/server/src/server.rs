use std::{collections::HashMap, pin::Pin, sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}}, task::{Context, Poll}, time::Duration};
use serde::Deserialize;
use serde_json::value::RawValue;
use stratum_common::{pb, GuildFetchOpts};
use tokio::{signal, sync::watch, sync::mpsc};
use twilight_cache_inmemory::model::CachedGuild;
use twilight_gateway_queue::InMemoryQueue;
use twilight_model::{application::interaction::InteractionContextType, channel::Channel, gateway::OpCode, guild::{Member, Role}, id::{Id, marker::{GuildMarker, UserMarker}}, user::CurrentUser};
use twilight_gateway::{
    CloseFrame, ConfigBuilder, Event, EventTypeFlags, Intents, Message, Shard, ShardId, ShardState
};
use twilight_http::Client;
use futures_util::{Stream, StreamExt};
use tonic::Status;
use crate::config::CONFIG;

/// Validates workers
pub fn validate_worker(worker: &pb::Worker) -> Result<(), crate::Error> {
    // For now we just check that the worker ID is within bounds, but we can add more validation later if needed
    if worker.worker_id as usize >= CONFIG.num_workers {
        return Err(format!("Worker ID {} is out of bounds for number of workers {}", worker.worker_id, CONFIG.num_workers).into());
    }

    if worker.grpc_access_key != CONFIG.grpc_access_key {
        return Err("Invalid gRPC access key".into());
    }

    Ok(())
}

/// validates other authorized requests
pub fn validate_oauth(oauth: &pb::OtherAuthorized) -> Result<(), crate::Error> {
    if oauth.grpc_access_key != CONFIG.grpc_access_key {
        return Err("Invalid gRPC access key".into());
    }   
    Ok(())
}

/// Core ID struct
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum TenantId {
    Guild(Id<GuildMarker>), // Marker is for type-safe IDs, so we don't accidentally use a channel ID where a guild ID is expected, etc.    
    User(Id<UserMarker>),
}

impl TenantId {
    /// Determines which worker gets the tenant
    /// 
    /// To ensure that all events for a given tenant go to the same worker, we shard by tenant ID. 
    /// based on Discords sharding formula (id >> 22) % num_shards.
    ///
    /// Returns the worker ID that the event should be routed to for the given tenant (ID)
    pub fn worker_id(self, num_workers: usize) -> usize {
        match self {
            // This is safe as AntiRaid workers do not currently support 32 bit platforms
            TenantId::Guild(guild_id) => (guild_id.get() >> 22) as usize % num_workers,
            TenantId::User(user_id) => (user_id.get() >> 22) as usize % num_workers,
        }
    }
}

#[inline(always)]
/// Creates a twilight Id<T> given a `u64` id
fn get_id<T>(id: u64) -> Result<Id<T>, Status> {
    Id::new_checked(id).ok_or_else(|| Status::invalid_argument("Invalid Snowflake ID"))
}

/// Special holder struct that when dropped, removes the connection from the workers connection list
pub struct ConnectionGuard {
    conn_id: u64,
    worker: Worker,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        {
            let mut conn_txs = self.worker.conn_txs.write().unwrap();
            if conn_txs.remove(&self.conn_id).is_none() {
                return;
            }
        }

        log::info!("Cleaned up connection {} for worker {}", self.conn_id, self.worker.id);
    }
}

/// Stream that holds a connection guard to drop the connection from the worker map once closed
pub struct GuardedStream<S> {
    inner: S,
    _guard: ConnectionGuard,
}

// need Unpin and Pin for safe polling of async streams
impl<S: Stream + Unpin> Stream for GuardedStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Worker struct that holds all the connections for a given shard and handles sending events to them
#[derive(Clone)]
pub struct Worker {
    conn_txs: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<Result<pb::DiscordEvent, Status>>>>>,
    curr_id: Arc<AtomicU64>,
    id: usize
}

impl Worker {
    pub fn new(id: usize) -> Self {
        Self {
            conn_txs: Arc::new(RwLock::new(HashMap::new())),
            curr_id: AtomicU64::new(0).into(),
            id
        }
    }

    pub fn add_connection(&self, tx: mpsc::UnboundedSender<Result<pb::DiscordEvent, Status>>) -> ConnectionGuard {
        let next_id = self.curr_id.fetch_add(1, Ordering::SeqCst);
        {
            let mut conn_txs = self.conn_txs.write().unwrap();
            conn_txs.insert(next_id, tx.clone());
        }

        ConnectionGuard { conn_id: next_id, worker: self.clone() }
    }
    
    pub fn send_event(&self, event: pb::DiscordEvent) {
        let conn_txs = self.conn_txs.read().unwrap();
        match conn_txs.len() {
            0 => return, // no connections, drop the event
            1 => {
                let _ = conn_txs.iter().map(|(_, c)| c).next().unwrap().send(Ok(event)); // only one connection, send directly without needing to clone
            }
            _ => { // multiple connections, need to clone the event for each connection
                for (_, tx) in conn_txs.iter() {
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
        for i in 0..num_workers {
            workers.push(Arc::new(Worker::new(i)));
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

    pub fn close_all_connections(&self) {
        for worker in &self.workers {
            worker.conn_txs.write().unwrap().clear();
        }
    }
}

/// Shard data struct, which holds the cache for a shard and other relevant data that might be needed globally by Stratum
pub struct ShardData {
    collected_data: RwLock<Option<CollectedShardData>>,
    ready: Ready,
}

impl ShardData {
    pub fn new(shard_id: usize) -> Self {
        Self {
            collected_data: RwLock::new(None),
            ready: Ready::new(shard_id)
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
        for shard_id in 0..num_shards {
            shard_data.push(Arc::new(ShardData::new(shard_id)));
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
#[allow(dead_code)]
struct Ready {
    is_ready_tx: watch::Sender<bool>,
    is_ready_rx: watch::Receiver<bool>,
    shard_id: usize,
}

#[allow(dead_code)]
impl Ready {
    fn new(shard_id: usize) -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            is_ready_tx: tx,
            is_ready_rx: rx,
            shard_id
        }
    }

    fn mark_ready(&self) {
        log::info!("Shard {} is now ready!", self.shard_id);
        self.is_ready_tx.send_replace(true);
    }

    fn mark_not_ready(&self) {
        self.is_ready_tx.send_replace(false);
    }

    async fn wait_until_ready(&self) -> Result<(), crate::Error> {
        if self.is_ready(){ 
            return Ok(()) 
        }

        let mut rx = self.is_ready_tx.subscribe();
        rx.wait_for(|x| *x).await?;
        Ok(())
    }

    fn is_ready(&self) -> bool {
        *self.is_ready_rx.borrow()
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
            .max_frame_size(Some(1024 * 1024 * 16 - 10)) // 16MB
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
    type ShardReadyStreamStream = Pin<Box<dyn Stream<Item = Result<pb::ShardReadyUpdate, Status>> + Send>>;

    async fn event_stream(&self, request: tonic::Request<pb::Worker>) -> Result<tonic::Response<Self::EventStreamStream>, Status> {
        let worker = request.into_inner();
        validate_worker(&worker).map_err(|e| Status::unauthenticated(format!("Worker validation failed: {}", e)))?;

        let (tx, rx) = mpsc::unbounded_channel();
        let cs = self.common_state.workers.get_worker_by_id(worker.worker_id as usize).add_connection(tx);
        let rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let rx = GuardedStream {
            inner: rx,
            _guard: cs
        };
        Ok(tonic::Response::new(Box::pin(rx) as Self::EventStreamStream))
    }

    async fn get_status(&self, request: tonic::Request<pb::OtherAuthorized>) -> Result<tonic::Response<pb::Status>, Status> {
        let other = request.into_inner();
        validate_oauth(&other).map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
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
        validate_oauth(&other).map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
  
        match typ {
            pb::ResourceType::RChannel => {
                let id = get_id(ccr.id)?;

                let chan = match self.common_state.cache.channel(id) {
                    Some(chan) => pb::AnyValue::from_real(chan.value()),
                    None => pb::AnyValue::from_real(&None::<Channel>)
                }?;

                Ok(tonic::Response::new(chan))
            }
            pb::ResourceType::RGuild => {
                let id = get_id(ccr.id)?;
                let flags = GuildFetchOpts::from_bits(ccr.flags)
                .ok_or_else(|| Status::invalid_argument("Missing flags in request"))?;

                // Fetch the guild (using a sep thread if needed)
                let g_opt = if flags.is_expensive() {
                    let cache = self.common_state.cache.clone();
                    tokio::task::spawn_blocking(move || crate::cacher_guild::get_guild(&cache, id, flags)).await
                    .map_err(|e| Status::internal(e.to_string()))?
                } else {
                    crate::cacher_guild::get_guild(&self.common_state.cache, id, flags)
                };

                let g = match g_opt {
                    Some(g) => pb::AnyValue::from_real(&g),
                    None => pb::AnyValue::from_real(&None::<CachedGuild>)
                }?; 

                Ok(tonic::Response::new(g))
            }
            pb::ResourceType::RGuildRole => {
                let id = get_id(ccr.id)?;
 
                let gr = match self.common_state.cache.role(id) {
                    Some(gr) => pb::AnyValue::from_real(gr.value().resource()),
                    None => pb::AnyValue::from_real(&None::<Role>)
                }?;

                Ok(tonic::Response::new(gr))
            }
            pb::ResourceType::RGuildRoles => {
                let id = get_id(ccr.id)?;
                let gr = crate::cacher_guild::get_roles_resource(&self.common_state.cache, id)?;

                Ok(tonic::Response::new(gr))
            }
            pb::ResourceType::RGuildChannels => {
                let id = get_id(ccr.id)?;
                let gc = crate::cacher_guild::get_channels_resource(&self.common_state.cache, id)?;

                Ok(tonic::Response::new(gc))
            }
            pb::ResourceType::RGuildMember => {
                let guild_id = get_id(ccr.id)?;
                let user_id = get_id(ccr.id_b)?;

                let gm = match crate::cacher_guild::member(&self.common_state.cache, guild_id, user_id) {
                    Some(gm) => pb::AnyValue::from_real(&gm),
                    None => pb::AnyValue::from_real(&None::<Member>)
                }?;

                Ok(tonic::Response::new(gm))
            }
            pb::ResourceType::RGuildIds => {
                if !CONFIG.allow_guild_ids_get.unwrap_or(false) {
                    return Err(Status::failed_precondition("getting guild ids has been disabled"));
                }
                let ids = self.common_state.cache.iter().guilds().map(|x| x.id()).collect::<Vec<_>>();
                let resp = pb::AnyValue::from_real(&ids)?;
                Ok(tonic::Response::new(resp))
            }
            pb::ResourceType::RCurrentUser => {
                let cu = match self.common_state.cache.current_user() {
                    Some(cu) => pb::AnyValue::from_real(&cu),
                    None => pb::AnyValue::from_real(&None::<CurrentUser>)
                }?; 

                Ok(tonic::Response::new(cu))
            }
        }
    }

    async fn is_resource_in_cache(&self, request: tonic::Request<pb::IsResourceInCacheRequest>) -> Result<tonic::Response<pb::IsResourceInCacheResponse>, Status> {
        let ccr = request.into_inner();
        let typ = ccr.r#type();
        let Some(other) = ccr.auth else {
            return Err(Status::unauthenticated(format!("No other found")));
        };
        validate_oauth(&other).map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
        
        let is_cached = match typ {
            pb::ResourceType::RChannel => self.common_state.cache.channel(get_id(ccr.id)?).is_some(),
            pb::ResourceType::RGuild => self.common_state.cache.guild(get_id(ccr.id)?).is_some(),
            pb::ResourceType::RGuildRole => self.common_state.cache.role(get_id(ccr.id)?).is_some(),
            pb::ResourceType::RGuildRoles => self.common_state.cache.guild_roles(get_id(ccr.id)?).is_some(),
            pb::ResourceType::RGuildChannels => self.common_state.cache.guild_channels(get_id(ccr.id)?).is_some(),
            pb::ResourceType::RGuildMember => self.common_state.cache.member(get_id(ccr.id)?, get_id(ccr.id_b)?).is_some(),
            pb::ResourceType::RCurrentUser => self.common_state.cache.current_user().is_some(),
            pb::ResourceType::RGuildIds => return Err(Status::failed_precondition("operation not supported"))
        };

        Ok(tonic::Response::new(pb::IsResourceInCacheResponse { cached: is_cached }))
    }

    async fn bulk_is_resource_in_cache(&self, request: tonic::Request<pb::BulkIsResourceInCacheRequest>) -> Result<tonic::Response<pb::BulkIsResourceInCacheResponse>, Status> {
        let ccr = request.into_inner();
        let typ = ccr.r#type();
        let Some(other) = ccr.auth else {
            return Err(Status::unauthenticated(format!("No other found")));
        };
        validate_oauth(&other).map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;

        let mut cached = Vec::with_capacity(ccr.id.len());
        if typ == pb::ResourceType::RGuildMember {
            // Requires both id and id_b
            if ccr.id.len() != ccr.id_b.len() {
                return Err(Status::invalid_argument("id.len() != id_b.len() for GuildMember"));
            }
            for (id_a, id_b) in ccr.id.into_iter().zip(ccr.id_b.into_iter()) {
                cached.push(self.common_state.cache.member(get_id(id_a)?, get_id(id_b)?).is_some());
            }
        } else {
            // Single id only
            for id in ccr.id {
                let is_cached = match typ {
                    pb::ResourceType::RChannel => self.common_state.cache.channel(get_id(id)?).is_some(),
                    pb::ResourceType::RGuild => self.common_state.cache.guild(get_id(id)?).is_some(),
                    pb::ResourceType::RGuildRole => self.common_state.cache.role(get_id(id)?).is_some(),
                    pb::ResourceType::RGuildRoles => self.common_state.cache.guild_roles(get_id(id)?).is_some(),
                    pb::ResourceType::RGuildChannels => self.common_state.cache.guild_channels(get_id(id)?).is_some(),
                    pb::ResourceType::RGuildMember => return Err(Status::invalid_argument("unreachable")),
                    pb::ResourceType::RCurrentUser => self.common_state.cache.current_user().is_some(),
                    pb::ResourceType::RGuildIds => return Err(Status::failed_precondition("operation not supported")) // must use rguild instead
                };
                cached.push(is_cached);
            }
        }

        Ok(tonic::Response::new(pb::BulkIsResourceInCacheResponse { cached }))
    }

    async fn get_config(&self, request: tonic::Request<pb::OtherAuthorized>) -> Result<tonic::Response<pb::StratumConfig>, Status> {
        let other = request.into_inner();
        validate_oauth(&other).map_err(|e| Status::unauthenticated(format!("Validation failed: {}", e)))?;
        Ok(tonic::Response::new(pb::StratumConfig { 
            num_workers: CONFIG.num_workers.try_into().map_err(|e| Status::internal(format!("Config fetch failed: {}", e)))?
        }))
    }

    async fn shard_ready_stream(&self, request: tonic::Request<pb::OtherAuthorized>) -> Result<tonic::Response<Self::ShardReadyStreamStream>, Status> {
        let other = request.into_inner();
        validate_oauth(&other).map_err(|e| Status::unauthenticated(format!("Worker validation failed: {}", e)))?;

        let (tx, rx) = mpsc::unbounded_channel();
        let rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        let common_data = self.common_state.clone();
        tokio::task::spawn(async move {
            let mut shard_rxs = common_data.shards.shard_data.iter()
                .map(|s| s.ready.is_ready_rx.clone())
                .collect::<Vec<_>>();
            
            loop {
                // Determine ready shards and push update
                let ready_shards: Vec<u32> = shard_rxs.iter()
                    .enumerate()
                    .filter(|(_, rx)| *rx.borrow())
                    .map(|(i, _)| i as u32)
                    .collect();

                let update = pb::ShardReadyUpdate {
                    ready_shards,
                    total_shards: shard_rxs.len() as u32,
                };

                if tx.send(Ok(update)).is_err() {
                    break;
                }
                
                // Now wait for next shard update
                let has_changed = shard_rxs.iter_mut()
                .map(|rx| {
                    rx.mark_unchanged();
                    Box::pin(rx.changed())
                })
                .collect::<Vec<_>>();

                tokio::select! {
                    _ = tx.closed() => break,
                    _ = futures_util::future::select_all(has_changed) => continue,
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(rx) as Self::ShardReadyStreamStream))
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
                        sd.ready.mark_not_ready();
                        if *shutdown.borrow() {
                            log::info!("Shard with ID: {} is shutting down gracefully", shard.id());
                            break;
                        }
                        continue;
                    }
                    Message::Text(json_str) => json_str
                };

                if let Err(e) = dispatch_single(shard.id().number(), json_str, &common_state, &sd) {
                    log::warn!("dispatch_single on shard {} failed: {e}", shard.id());
                }
            }
        }
    }
}

/// Helper method to actually perform the event dispatch + cache update
fn dispatch_single(shard_id: u32, event_json: String, common_state: &CommonState, sd: &ShardData) -> Result<(), crate::Error> {
    let (event, event_name, opcode) = crate::eventparse::parse(&event_json, EventTypeFlags::all())?;
    
    if opcode != OpCode::Dispatch {
        if opcode == OpCode::Reconnect {
            log::info!("Shard {shard_id} is restarting");
        }

        log::debug!("Ignoring msg with opcode: {opcode:?}");
        return Ok(()); // Ignore non-dispatch messages
    }
    
    let Some(event_name) = event_name else {
        return Err(format!("Received event with unknown type: {event_json}").into());
    };

    if event_name == "READY" || event_name == "RESUMED" {
        sd.ready.mark_ready();
    }

    //log::info!("dispatch_single: {event_json}");

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
    
    let (event, guild_id, msg_author, target_user) = deduce_parts(&event_json, &parsed_event);
    let Some(event) = event else {
        log::info!("Ignoring msg with no known 'd': {event_json}");
        return Ok(());
    };
    
    let tenant_id = if let Some(guild_id) = guild_id {
        TenantId::Guild(guild_id) 
    } else if let Some(user_id) = target_user {
        TenantId::User(user_id)
    } else {
        log::info!("Ignoring msg with no guild_id or target_user: {event_json}");
        return Ok(());
    };

    let worker = common_state.workers.get_worker_for_tenant(tenant_id);
    worker.send_event(pb::DiscordEvent {
        event_name,
        payload: event,
        guild_id: guild_id.map(|x| x.get()).unwrap_or(0),
        msg_author: msg_author.map(|x| x.get()).unwrap_or(0),
        target_user: target_user.map(|x| x.get()).unwrap_or(0)
    });

    Ok(())
}

/// Helper method to deduce the d key, guild_id and msg_author from an event, if possible
fn deduce_parts(raw_json: &str, parsed_event: &Option<Event>) -> (Option<String>, Option<Id<GuildMarker>>, Option<Id<UserMarker>>, Option<Id<UserMarker>>) {
    #[derive(Deserialize)]
    struct Evt<'a> {
        #[serde(borrow)]
        d: &'a RawValue,
    }

    let Ok(input) = serde_json::from_str::<Evt>(raw_json) else {
        return (None, None, None, None);
    };

    if let Some(event) = parsed_event {
        // Find guild_id, handling cases not handled by twilight's event.guild_id()
        let guild_id = match event {
            Event::EntitlementCreate(e) => e.guild_id,
            Event::EntitlementDelete(e) => e.guild_id,
            Event::EntitlementUpdate(e) => e.guild_id,
            _ => event.guild_id()
        };

        let msg_author = match event {
            Event::MessageCreate(m) => Some(m.author.id),
            Event::MessageUpdate(m) => Some(m.author.id),
            _ => None,
        };

        let target_user = match event {
            // for interactions we want user id of command invoker
            // have to check interaction type for private_channel or guild
            Event::InteractionCreate(e) => match e.context {
                Some(InteractionContextType::BotDm | InteractionContextType::PrivateChannel) => e.author_id(),
                _ => None,
            },
            _ => None,
        };

        (Some(input.d.to_string()), guild_id, msg_author, target_user)
    } else {
        // Fallback to wildcard parsing to try extracting guild id from event
        #[derive(Deserialize)]
        struct IdData {
            guild_id: Option<Id<GuildMarker>>,
        }
        
        if let Ok(wrapper) = serde_json::from_str::<IdData>(input.d.get()) {
            return (Some(input.d.to_string()), wrapper.guild_id, None, None);
        }

        // If we can't find the ids, return None and ignore the event, since we won't know which tenant to route it to
        (None, None, None, None)
    }
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

    let get_gw = client.gateway().authed().await?.model().await?;
    let queue = InMemoryQueue::new(
        get_gw.session_start_limit.max_concurrency,
        get_gw.session_start_limit.remaining,
        Duration::from_millis(get_gw.session_start_limit.reset_after),
        get_gw.session_start_limit.total,
    );

    let config = ConfigBuilder::new(CONFIG.token.clone(), Intents::from_bits(CONFIG.intents).expect("Invalid intents in config"))
    .queue(queue)
    .build();

    let shards = (0..get_gw.shards).map(|shard| {
        let shard_id = ShardId::new(shard, get_gw.shards);
        Shard::with_config(shard_id, config.clone())
    });
    let common_state = CommonState::new(shards.len());

    let mut tasks = shards
        .map(|shard| tokio::spawn(dispatcher(shard, shutdown_rx.clone(), common_state.clone())))
        .collect::<Vec<_>>();

    // Push server count printer task
    let common_state_ref = common_state.clone();
    let shutdown_state = common_state.clone();
    let mut shutdown_rx_ref = shutdown_rx.clone();
    tasks.push(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = shutdown_rx_ref.changed() => break,
                _ = ticker.tick() => {
                    let mut ready = 0;
                    let total = common_state_ref.shards.shard_data.len();
                    for shard in common_state_ref.shards.shard_data.iter() {
                        if shard.ready.is_ready() {
                            ready+=1;
                        }
                    }

                    log::info!("Currently in {} servers with {ready}/{total} shards up", common_state_ref.cache.stats().guilds());
                }
            }
        }
    }));

    // Push grpc server to the very end
    tasks.push(tokio::spawn(async move {
        let srv = StratumServer::new(common_state);
        loop {
            let shutdown = shutdown_rx.clone();
            if let Err(e) = srv.start(shutdown).await {
                if *shutdown_rx.borrow() {
                    break;
                }
                log::error!("gRPC server shut down unexpectedly: {e:?}, restarting");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            if *shutdown_rx.borrow() {
                break;
            }
        }
    }));

    signal::ctrl_c().await?;
    _ = shutdown_tx.send(true);
    shutdown_state.workers.close_all_connections();

    for task in tasks {
        tokio::select! {
            _ = task => {},
            _ = signal::ctrl_c() => {}
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        };
    }

    Ok(())
}