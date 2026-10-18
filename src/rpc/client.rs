use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use thiserror::Error;

use crate::rpc::protocol::{
    ClientCommand, ClientRequestMsg, ClientResponseMsg, ClientResult, RaftMessage, RaftPayload,
};
use crate::rpc::router::ShardRouter;
use crate::rpc::transport::TcpCodec;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Failed to connect to cluster node {0} at {1}: {2}")]
    Connect(u64, SocketAddr, std::io::Error),
    #[error("RPC transport error: {0}")]
    Transport(#[from] crate::rpc::transport::TransportError),
    #[error("Cluster command failed: {0}")]
    Execution(String),
    #[error("No known node for shard group {0}")]
    UnknownGroup(u64),
    #[error("Request timed out or leader unavailable")]
    Timeout,
}

/// Production distributed client for executing transactional key-value operations across Raft-Nexus.
pub struct NexusClient {
    pub node_addresses: HashMap<u64, SocketAddr>,
    pub router: Arc<ShardRouter>,
    pub client_id: String,
    seq_counter: std::sync::atomic::AtomicU64,
}

impl NexusClient {
    pub fn new(node_addresses: HashMap<u64, SocketAddr>, router: Arc<ShardRouter>) -> Self {
        Self {
            node_addresses,
            router,
            client_id: format!("client-{}", rand::random::<u32>()),
            seq_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Executes a command by routing to the shard group leader.
    pub async fn execute(&self, key: &[u8], cmd: ClientCommand) -> Result<ClientResult, ClientError> {
        let shard = self
            .router
            .route_key(key)
            .ok_or_else(|| ClientError::UnknownGroup(0))?;

        let target_node = shard.leader_hint.unwrap_or_else(|| {
            *shard.replicas.first().unwrap_or(&1)
        });

        let addr = self
            .node_addresses
            .get(&target_node)
            .ok_or_else(|| ClientError::UnknownGroup(shard.group_id))?;

        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| ClientError::Connect(target_node, *addr, e))?;

        let msg = RaftMessage {
            group_id: shard.group_id,
            from: 0,
            to: target_node,
            payload: RaftPayload::ClientRequest(ClientRequestMsg {
                client_id: self.client_id.clone(),
                request_seq: self.next_seq(),
                command: cmd,
            }),
        };

        TcpCodec::write_message(&mut stream, &msg).await?;
        let reply_msg = TcpCodec::read_message(&mut stream).await?;

        if let RaftPayload::ClientResponse(resp) = reply_msg.payload {
            if let Some(new_leader) = resp.leader_hint {
                self.router.update_leader(shard.group_id, new_leader);
            }
            Ok(resp.result)
        } else {
            Err(ClientError::Execution("Invalid response format received".into()))
        }
    }

    /// Stores a key-value pair.
    pub async fn put(&self, key: &[u8], val: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.execute(key, ClientCommand::Put {
            key: key.to_vec(),
            val: val.to_vec(),
        }).await? {
            ClientResult::Success(prev) => Ok(prev),
            ClientResult::Error(e) => Err(ClientError::Execution(e)),
            _ => Err(ClientError::Execution("Unexpected result type".into())),
        }
    }

    /// Retrieves the value associated with a key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.execute(key, ClientCommand::Get {
            key: key.to_vec(),
        }).await? {
            ClientResult::Success(val) => Ok(val),
            ClientResult::Error(e) => Err(ClientError::Execution(e)),
            _ => Err(ClientError::Execution("Unexpected result type".into())),
        }
    }

    /// Deletes a key from the cluster.
    pub async fn delete(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.execute(key, ClientCommand::Delete {
            key: key.to_vec(),
        }).await? {
            ClientResult::Success(prev) => Ok(prev),
            ClientResult::Error(e) => Err(ClientError::Execution(e)),
            _ => Err(ClientError::Execution("Unexpected result type".into())),
        }
    }

    /// Scans a range of keys beginning with `start` up to `limit` entries.
    pub async fn scan(
        &self,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClientError> {
        match self.execute(start, ClientCommand::Scan {
            start: start.to_vec(),
            limit,
        }).await? {
            ClientResult::ScanResults(results) => Ok(results),
            ClientResult::Error(e) => Err(ClientError::Execution(e)),
            _ => Err(ClientError::Execution("Unexpected result type".into())),
        }
    }
}
