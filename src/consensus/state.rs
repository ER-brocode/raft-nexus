use std::collections::HashMap;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use super::role::RaftRole;

/// Persistent consensus state on stable storage (WAL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    /// Latest term server has seen (initialized to 0 on first boot).
    pub current_term: u64,
    /// CandidateId that received vote in current term (or None if none).
    pub voted_for: Option<u64>,
}

impl Default for HardState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

/// Volatile cluster runtime state (not persisted to disk).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
    /// ID of the current leader if known.
    pub leader_id: Option<u64>,
    /// Node consensus role.
    pub role: RaftRole,
}

/// Replication progress tracked by the leader for each peer node.
#[derive(Debug, Clone)]
pub struct PeerProgress {
    /// For each server, index of the next log entry to send to that server.
    pub next_index: u64,
    /// For each server, index of highest log entry known to be replicated on server.
    pub match_index: u64,
    /// Whether the peer is currently reachable and responding to heartbeats.
    pub active: bool,
    /// Timestamp of last successful RPC response.
    pub last_ack: Instant,
}

impl PeerProgress {
    pub fn new(next_index: u64) -> Self {
        Self {
            next_index,
            match_index: 0,
            active: true,
            last_ack: Instant::now(),
        }
    }
}

/// Consolidated state representing all components of a Raft node.
#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub hard_state: HardState,
    pub soft_state: SoftState,
    /// Index of highest log entry known to be committed.
    pub commit_index: u64,
    /// Index of highest log entry applied to state machine.
    pub last_applied: u64,
    /// Peer tracking indices (used when Leader).
    pub progress: HashMap<u64, PeerProgress>,
}

impl ConsensusState {
    pub fn new(node_id: u64, peers: &[u64]) -> Self {
        let mut progress = HashMap::new();
        for &peer in peers {
            if peer != node_id {
                progress.insert(peer, PeerProgress::new(1));
            }
        }

        Self {
            hard_state: HardState::default(),
            soft_state: SoftState {
                leader_id: None,
                role: RaftRole::Follower,
            },
            commit_index: 0,
            last_applied: 0,
            progress,
        }
    }
}
