use serde::{Deserialize, Serialize};
use crate::consensus::log::LogEntry;

/// Client mutation operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    Put { key: Vec<u8>, val: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Client command passed to the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientCommand {
    Put { key: Vec<u8>, val: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
    Scan { start: Vec<u8>, limit: usize },
    TwoPhasePrepare {
        tx_id: u64,
        mutations: Vec<Mutation>,
    },
    TwoPhaseCommit {
        tx_id: u64,
    },
    TwoPhaseAbort {
        tx_id: u64,
    },
}

/// Result returned from applying a client command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientResult {
    Success(Option<Vec<u8>>),
    ScanResults(Vec<(Vec<u8>, Vec<u8>)>),
    Error(String),
}

/// Client request message routed through the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequestMsg {
    pub client_id: String,
    pub request_seq: u64,
    pub command: ClientCommand,
}

/// Client response message returned to client or proxy gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientResponseMsg {
    pub request_seq: u64,
    pub leader_hint: Option<u64>,
    pub result: ClientResult,
}

/// Arguments for the RequestVote RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Reply for the RequestVote RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

/// Arguments for the AppendEntries RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// Reply for the AppendEntries RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

/// Arguments for InstallSnapshot RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

/// Reply for InstallSnapshot RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub bytes_stored: u64,
}

/// Envelope payload uniting all internal and external cluster messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftPayload {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshotArgs),
    InstallSnapshotReply(InstallSnapshotReply),
    ClientRequest(ClientRequestMsg),
    ClientResponse(ClientResponseMsg),
}

/// Universal envelope routed between cluster nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMessage {
    /// Target Multi-Raft group identifier.
    pub group_id: u64,
    /// Originating node identifier.
    pub from: u64,
    /// Destination node identifier.
    pub to: u64,
    /// Inner RPC message payload.
    pub payload: RaftPayload,
}
