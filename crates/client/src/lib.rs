use stratum_common::{Error, GuildFetchOpts};
pub use stratum_common::pb;
use futures_util::{Stream, StreamExt};
use tokio::sync::watch;

/// A GetResourceRequest that is type-safe
#[derive(Clone, Copy)]
pub enum GetResourceRequest {
    /// single channel (guild or dm)
    Channel { channel_id: u64 }, 
    /// all guild data
    Guild { guild_id: u64, flags: GuildFetchOpts }, 
    /// single guild role
    GuildRole { role_id: u64 }, 
    /// all guild roles
    GuildRoles { guild_id: u64 }, 
    /// all guild channels
    GuildChannels { guild_id: u64 }, 
    /// single guild members
    GuildMember { guild_id: u64, user_id: u64 }, 
    /// current user
    CurrentUser, 
}

/// A IsResourceInCacheRequest that is type-safe
#[derive(Clone, Copy)]
pub enum IsResourceInCacheRequest {
    /// single channel (guild or dm)
    Channel { channel_id: u64 }, 
    /// all guild data
    Guild { guild_id: u64 }, 
    /// single guild role
    GuildRole { role_id: u64 }, 
    /// all guild roles
    GuildRoles { guild_id: u64 }, 
    /// all guild channels
    GuildChannels { guild_id: u64 }, 
    /// single guild members
    GuildMember { guild_id: u64, user_id: u64 }, 
    /// current user
    CurrentUser, 
}

/// Stratum mid/high-level client
pub struct StratumClient {
    client: pb::stratum_client::StratumClient<tonic::transport::Channel>,
    grpc_access_key: String,
}

impl StratumClient {
    pub async fn new(grpc_address: &str, grpc_access_key: String) -> Result<Self, Error> {
        let uri = tonic::transport::Endpoint::from_shared(format!("http://{grpc_address}"))?;
        let client = pb::stratum_client::StratumClient::connect(uri).await?;
        Ok(Self { client, grpc_access_key })
    }

    /// GetConfig returns the configuration of the running stratum server
    pub async fn get_config(&self) -> Result<pb::StratumConfig, Error> {
        let mut client = self.client.clone();
        let resp = client.get_config(self.oauth()).await?;
        Ok(resp.into_inner())
    }

    /// ShardReadyStream provides shard ready updates as shards become ready/non-ready
    /// 
    /// This returns a stream of events. The next event can then be retrieved with <stream.next()> 
    pub async fn shard_ready_stream(&self) -> Result<tonic::Streaming<pb::ShardReadyUpdate>, Error> {
        let mut client = self.client.clone();
        let resp = client.shard_ready_stream(self.oauth()).await?;
        Ok(resp.into_inner())
    }

    /// EventStream is a streaming RPC that allows the master to send Discord events to the worker
    /// 
    /// This returns a stream of events. The next event can then be retrieved with <stream.next()> 
    pub async fn event_stream(&self, wid: u32) -> Result<tonic::Streaming<pb::DiscordEvent>, Error> {
        let mut client = self.client.clone();
        let resp = client.event_stream(self.worker(wid)).await?;
        Ok(resp.into_inner())
    }

    /// GetStatus returns the current status of shards in Stratum
    pub async fn get_status(&self) -> Result<pb::Status, Error> {
        let mut client = self.client.clone();
        let resp = client.get_status(self.oauth()).await?;
        Ok(resp.into_inner())
    }

    /// GetResourceFromCache returns the cached resource data or null
    pub async fn get_resource_from_cache(&self, req: GetResourceRequest) -> Result<Option<serde_json::Value>, Error> {
        let grr = match req {
            GetResourceRequest::Channel { channel_id } => pb::GetResourceRequest { r#type: pb::ResourceType::RChannel as i32, flags: 0, id: channel_id, id_b: 0, auth: Some(self.oauth()) },
            GetResourceRequest::Guild { guild_id, flags } => pb::GetResourceRequest { r#type: pb::ResourceType::RGuild as i32, flags: flags.bits(), id: guild_id, id_b: 0, auth: Some(self.oauth()) },
            GetResourceRequest::GuildRole { role_id } => pb::GetResourceRequest { r#type: pb::ResourceType::RGuildRole as i32, flags: 0, id: role_id, id_b: 0, auth: Some(self.oauth()) },
            GetResourceRequest::GuildRoles { guild_id } => pb::GetResourceRequest { r#type: pb::ResourceType::RGuildRoles as i32, flags: 0, id: guild_id, id_b: 0, auth: Some(self.oauth()) },
            GetResourceRequest::GuildChannels { guild_id } => pb::GetResourceRequest { r#type: pb::ResourceType::RGuildChannels as i32, flags: 0, id: guild_id, id_b: 0, auth: Some(self.oauth()) },
            GetResourceRequest::GuildMember { guild_id, user_id } => pb::GetResourceRequest { r#type: pb::ResourceType::RGuildMember as i32, flags: 0, id: guild_id, id_b: user_id, auth: Some(self.oauth()) },
            GetResourceRequest::CurrentUser => pb::GetResourceRequest { r#type: pb::ResourceType::RCurrentUser as i32, flags: 0, id: 0, id_b: 0, auth: Some(self.oauth()) },
        };

        let mut client = self.client.clone();
        let resp = client.get_resource_from_cache(grr).await?;
        resp.into_inner().to_real_exec()
    }

    /// Helper method on top of `get_resource_from_cache` that also deserializes into a `T`
    pub async fn get_parsed_resource_from_cache<T: for<'de> serde::Deserialize<'de>>(&self, req: GetResourceRequest) -> Result<Option<T>, Error> {
        let Some(v) = self.get_resource_from_cache(req).await? else {
            return Ok(None)
        };
        Ok(serde_json::from_value(v)?)
    }

    /// IsResourceInCache returns if a resource is in cache or not
    /// 
    /// Resource specific notes:
    /// - For R_CURRENT_USER, id must be 0
    pub async fn is_resource_in_cache(&self, req: IsResourceInCacheRequest) -> Result<pb::IsResourceInCacheResponse, Error> {
        let grr = match req {
            IsResourceInCacheRequest::Channel { channel_id } => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RChannel as i32, id: channel_id, id_b: 0, auth: Some(self.oauth()) },
            IsResourceInCacheRequest::Guild { guild_id } => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RGuild as i32, id: guild_id, id_b: 0, auth: Some(self.oauth()) },
            IsResourceInCacheRequest::GuildRole { role_id } => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RGuildRole as i32, id: role_id, id_b: 0, auth: Some(self.oauth()) },
            IsResourceInCacheRequest::GuildRoles { guild_id } => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RGuildRoles as i32, id: guild_id, id_b: 0, auth: Some(self.oauth()) },
            IsResourceInCacheRequest::GuildChannels { guild_id } => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RGuildChannels as i32, id: guild_id, id_b: 0, auth: Some(self.oauth()) },
            IsResourceInCacheRequest::GuildMember { guild_id, user_id } => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RGuildMember as i32, id: guild_id, id_b: user_id, auth: Some(self.oauth()) },
            IsResourceInCacheRequest::CurrentUser => pb::IsResourceInCacheRequest { r#type: pb::ResourceType::RCurrentUser as i32, id: 0, id_b: 0, auth: Some(self.oauth()) },
        };

        let mut client = self.client.clone();
        let resp = client.is_resource_in_cache(grr).await?;
        Ok(resp.into_inner())
    }

    /// Returns a OtherAuthorized ident for API's requiring this level of identification
    fn oauth(&self) -> pb::OtherAuthorized {
        pb::OtherAuthorized {
            grpc_access_key: self.grpc_access_key.clone()    
        }
    }

    /// Returns a Worker ident for API's requiring this level of identification
    fn worker(&self, wid: u32) -> pb::Worker {
        pb::Worker {
            worker_id: wid,
            grpc_access_key: self.grpc_access_key.clone()    
        }
    }

    /// Helper method that listens for a stream of events from the desired stream calling `f` with events and returning `Ok(())` once `f` returns `true`
    pub async fn listen_to_stream<S, T, F>(
        &self,
        mut stream: S,
        mut shutdown: Option<watch::Receiver<bool>>,
        on_event: F,
    ) -> Result<(), Error> 
    where
        S: Stream<Item = Result<T, tonic::Status>> + Unpin,
        F: Fn(T) -> bool,
    {
        loop {
            if let Some(ref mut sd) = shutdown {
                tokio::select! {
                    _ = sd.changed() => return Ok(()),
                    next = stream.next() => {
                        if !Self::handle_next(next, &on_event)? { break; }
                    }
                }
            } else {
                let next = stream.next().await;
                if !Self::handle_next(next, &on_event)? { break; }
            }
        }
        Ok(())
    }

    /// Internal helper to process the stream result
    fn handle_next<T, F>(
        next: Option<Result<T, tonic::Status>>, 
        on_event: &F
    ) -> Result<bool, Error> 
    where F: Fn(T) -> bool 
    {
        match next {
            Some(Ok(evt)) => Ok(!(on_event)(evt)), // Continue if f returns false
            Some(Err(e)) => Err(e.into()),
            None => Ok(false), // Stop loop
        }
    }
}