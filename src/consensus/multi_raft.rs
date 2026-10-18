use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use thiserror::Error;

use crate::consensus::raft::{RaftEngine, RaftError};
use crate::rpc::protocol::{
    ClientCommand, ClientResult, RaftMessage, RaftPayload,
};
use crate::storage::engine::StorageEngine;

#[derive(Debug, Error)]
pub enum MultiRaftError {
    #[error("Raft group {0} not found on this node")]
    GroupNotFound(u64),
    #[error("Raft consensus error: {0}")]
    Consensus(#[from] RaftError),
    #[error("Routing error: {0}")]
    Routing(String),
}

/// Key range defining a shard managed by a Multi-Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub group_id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl KeyRange {
    pub fn contains(&self, key: &[u8]) -> bool {
        let after_start = self.start_key.is_empty() || key >= self.start_key.as_slice();
        let before_end = self.end_key.is_empty() || key < self.end_key.as_slice();
        after_start && before_end
    }
}

/// Multi-Raft coordinator hosting multiple consensus groups on a single physical node.
pub struct MultiRaftNode {
    pub node_id: u64,
    pub groups: HashMap<u64, RaftEngine>,
    pub ranges: Vec<KeyRange>,
    pub base_data_dir: Option<std::path::PathBuf>,
}

impl MultiRaftNode {
    pub fn new(node_id: u64, base_data_dir: Option<std::path::PathBuf>) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            ranges: Vec::new(),
            base_data_dir,
        }
    }

    /// Registers and starts a Raft consensus group.
    pub fn add_group(&mut self, group_id: u64, peers: Vec<u64>) {
        let storage = if let Some(ref base) = self.base_data_dir {
            StorageEngine::open(base, group_id).ok()
        } else {
            None
        };

        let engine = RaftEngine::new(group_id, self.node_id, peers, storage);
        self.groups.insert(group_id, engine);
    }

    /// Registers a key range partition mapped to a group.
    pub fn add_range(&mut self, group_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) {
        self.ranges.push(KeyRange {
            group_id,
            start_key,
            end_key,
        });
    }

    /// Finds the group responsible for a given key.
    pub fn locate_group(&self, key: &[u8]) -> Option<u64> {
        for range in &self.ranges {
            if range.contains(key) {
                return Some(range.group_id);
            }
        }
        // Default to first group if no range specified
        self.groups.keys().next().copied()
    }

    /// Advances timers across all hosted Raft groups.
    pub fn tick(&mut self, now: Instant) {
        for engine in self.groups.values_mut() {
            engine.tick(now);
        }
    }

    /// Routes an incoming network Raft message to its target group.
    pub fn handle_message(&mut self, msg: RaftMessage) -> Result<(), MultiRaftError> {
        let engine = self
            .groups
            .get_mut(&msg.group_id)
            .ok_or(MultiRaftError::GroupNotFound(msg.group_id))?;

        match msg.payload {
            RaftPayload::RequestVote(args) => {
                let reply = engine.handle_request_vote(args);
                engine.outbound_messages.push(RaftMessage {
                    group_id: msg.group_id,
                    from: self.node_id,
                    to: msg.from,
                    payload: RaftPayload::RequestVoteReply(reply),
                });
            }
            RaftPayload::RequestVoteReply(reply) => {
                engine.handle_request_vote_reply(msg.from, reply);
            }
            RaftPayload::AppendEntries(args) => {
                let reply = engine.handle_append_entries(args);
                engine.outbound_messages.push(RaftMessage {
                    group_id: msg.group_id,
                    from: self.node_id,
                    to: msg.from,
                    payload: RaftPayload::AppendEntriesReply(reply),
                });
            }
            RaftPayload::AppendEntriesReply(reply) => {
                engine.handle_append_entries_reply(msg.from, reply);
            }
            RaftPayload::InstallSnapshot(_) => {}
            RaftPayload::InstallSnapshotReply(_) => {}
            RaftPayload::ClientRequest(_) => {}
            RaftPayload::ClientResponse(_) => {}
        }

        Ok(())
    }

    /// Collects and drains all pending outbound messages across all groups.
    pub fn drain_outbound_messages(&mut self) -> Vec<RaftMessage> {
        let mut messages = Vec::new();
        for engine in self.groups.values_mut() {
            messages.append(&mut engine.outbound_messages);
        }
        messages
    }

    /// Proposes a client command to the group owning the key.
    pub fn execute(&mut self, key: &[u8], cmd: ClientCommand) -> Result<u64, MultiRaftError> {
        let group_id = self
            .locate_group(key)
            .ok_or_else(|| MultiRaftError::Routing("No matching shard range for key".into()))?;

        let engine = self
            .groups
            .get_mut(&group_id)
            .ok_or(MultiRaftError::GroupNotFound(group_id))?;

        let index = engine.propose(cmd, None)?;
        Ok(index)
    }
}
