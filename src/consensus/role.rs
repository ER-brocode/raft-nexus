use serde::{Deserialize, Serialize};
use std::fmt;

/// Represents the consensus role of a node in a specific Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    /// Follower: accepts heartbeats and log replication from the leader.
    Follower,
    /// Candidate: actively soliciting votes during an election.
    Candidate,
    /// Leader: handles client requests, maintains heartbeats, replicates log entries.
    Leader,
}

impl fmt::Display for RaftRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RaftRole::Follower => write!(f, "Follower"),
            RaftRole::Candidate => write!(f, "Candidate"),
            RaftRole::Leader => write!(f, "Leader"),
        }
    }
}
