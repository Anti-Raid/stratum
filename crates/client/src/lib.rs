use stratum_common::Error;
pub use stratum_common::pb;
use futures_util::{Stream, StreamExt};
use tokio::sync::watch;

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