use std::collections::HashMap;
use std::time::{Duration, Instant};
use log::{debug, info, warn};
use thiserror::Error;

use crate::consensus::election::{ElectionTimer, VoteTracker};
use crate::consensus::log::{EntryPayload, LogEntry, RaftLog};
use crate::consensus::replication::ReplicationManager;
use crate::consensus::role::RaftRole;
use crate::consensus::state::{ConsensusState, HardState, PeerProgress, SoftState};
use crate::rpc::protocol::{
    AppendEntriesArgs, AppendEntriesReply, ClientCommand, ClientResult,
    InstallSnapshotArgs, InstallSnapshotReply, RaftMessage, RaftPayload,
    RequestVoteArgs, RequestVoteReply,
};
use crate::storage::engine::StorageEngine;

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("Node {0} is not the cluster leader. Leader hint: {1:?}")]
    NotLeader(u64, Option<u64>),
    #[error("Storage engine error: {0}")]
    Storage(String),
    #[error("Cluster configuration error: {0}")]
    Config(String),
}

/// Fully autonomous single-group Raft Consensus Engine.
pub struct RaftEngine {
    pub group_id: u64,
    pub node_id: u64,
    pub peers: Vec<u64>,
    pub state: ConsensusState,
    pub log: RaftLog,
    pub storage: Option<StorageEngine>,

    // Timers & State Machine
    election_timer: ElectionTimer,
    election_deadline: Instant,
    heartbeat_interval: Duration,
    last_heartbeat: Instant,

    // Active election vote collector
    vote_tracker: Option<VoteTracker>,

    // Outbound message queue for network dispatch
    pub outbound_messages: Vec<RaftMessage>,

    // Committed client results buffer
    pub applied_results: HashMap<u64, ClientResult>,
}

impl RaftEngine {
    pub fn new(
        group_id: u64,
        node_id: u64,
        peers: Vec<u64>,
        mut storage: Option<StorageEngine>,
    ) -> Self {
        let election_timer = ElectionTimer::default();
        let timeout = election_timer.randomize_timeout();
        let election_deadline = Instant::now() + timeout;

        let mut raft_log = RaftLog::new(0, 0);
        let mut hard_state = HardState::default();

        if let Some(ref mut store) = storage {
            let wal_path = std::path::PathBuf::from(format!("./group_{}/raft.wal", group_id));
            if let Ok((recovered_hs, recovered_log)) = store.recover(&wal_path) {
                if let Some(hs) = recovered_hs {
                    hard_state = hs;
                }
                raft_log = recovered_log;
            }
        }

        let mut state = ConsensusState::new(node_id, &peers);
        state.hard_state = hard_state;

        Self {
            group_id,
            node_id,
            peers,
            state,
            log: raft_log,
            storage,
            election_timer,
            election_deadline,
            heartbeat_interval: Duration::from_millis(50),
            last_heartbeat: Instant::now(),
            vote_tracker: None,
            outbound_messages: Vec::new(),
            applied_results: HashMap::new(),
        }
    }

    /// Resets the randomized election timer.
    pub fn reset_election_timer(&mut self) {
        let timeout = self.election_timer.randomize_timeout();
        self.election_deadline = Instant::now() + timeout;
    }

    /// Ticks internal timers, driving elections or heartbeats.
    pub fn tick(&mut self, now: Instant) {
        match self.state.soft_state.role {
            RaftRole::Follower | RaftRole::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
                }
            }
            RaftRole::Leader => {
                if now.duration_since(self.last_heartbeat) >= self.heartbeat_interval {
                    self.send_heartbeats();
                    self.last_heartbeat = now;
                }
            }
        }
    }

    /// Transitions to Candidate and initiates a leader election round.
    pub fn start_election(&mut self) {
        self.state.soft_state.role = RaftRole::Candidate;
        self.state.hard_state.current_term += 1;
        self.state.hard_state.voted_for = Some(self.node_id);
        self.state.soft_state.leader_id = None;
        self.reset_election_timer();

        if let Some(ref mut store) = self.storage {
            let _ = store.save_hard_state(&self.state.hard_state);
        }

        info!(
            "[Group {}] Node {} starting election for Term {}",
            self.group_id, self.node_id, self.state.hard_state.current_term
        );

        let total_nodes = self.peers.len() + 1; // peers + self
        let mut tracker = VoteTracker::new(self.state.hard_state.current_term, self.node_id, total_nodes);

        // If single-node cluster, achieve quorum immediately
        if tracker.has_quorum() {
            self.become_leader();
            return;
        }

        self.vote_tracker = Some(tracker);

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        let vote_args = RequestVoteArgs {
            term: self.state.hard_state.current_term,
            candidate_id: self.node_id,
            last_log_index,
            last_log_term,
        };

        for &peer in &self.peers {
            self.outbound_messages.push(RaftMessage {
                group_id: self.group_id,
                from: self.node_id,
                to: peer,
                payload: RaftPayload::RequestVote(vote_args.clone()),
            });
        }
    }

    /// Transitions to Leader role, establishing heartbeats and unblocking replication.
    pub fn become_leader(&mut self) {
        self.state.soft_state.role = RaftRole::Leader;
        self.state.soft_state.leader_id = Some(self.node_id);
        self.vote_tracker = None;

        info!(
            "[Group {}] Node {} became Leader for Term {}",
            self.group_id, self.node_id, self.state.hard_state.current_term
        );

        let next_idx = self.log.last_index() + 1;
        for progress in self.state.progress.values_mut() {
            progress.next_index = next_idx;
            progress.match_index = 0;
            progress.active = true;
        }

        // Leader commits a No-Op entry to establish term boundary and read-index linearizability
        let noop_index = next_idx;
        let noop_entry = LogEntry {
            index: noop_index,
            term: self.state.hard_state.current_term,
            payload: EntryPayload::NoOp,
            request_id: None,
        };

        self.log.append_entry(noop_entry.clone());
        if let Some(ref mut store) = self.storage {
            let _ = store.append_entries(&[noop_entry]);
        }

        // If single-node cluster, commit immediately
        if self.peers.is_empty() {
            self.state.commit_index = noop_index;
            self.apply_committed();
        }

        self.send_heartbeats();
        self.last_heartbeat = Instant::now();
    }

    /// Transitions to Follower role.
    pub fn become_follower(&mut self, term: u64, leader_id: Option<u64>) {
        self.state.soft_state.role = RaftRole::Follower;
        self.state.hard_state.current_term = term;
        self.state.hard_state.voted_for = None;
        self.state.soft_state.leader_id = leader_id;
        self.vote_tracker = None;
        self.reset_election_timer();

        if let Some(ref mut store) = self.storage {
            let _ = store.save_hard_state(&self.state.hard_state);
        }
    }

    /// Sends heartbeats or pending log entries to all peers.
    pub fn send_heartbeats(&mut self) {
        for &peer in &self.peers {
            self.replicate_to_peer(peer);
        }
    }

    /// Replicates log entries to a specific peer.
    pub fn replicate_to_peer(&mut self, peer: u64) {
        let (prev_log_index, entries) = {
            let progress = match self.state.progress.get(&peer) {
                Some(p) => p,
                None => return,
            };

            let prev_index = progress.next_index.saturating_sub(1);
            let entries = self.log.entries_from(progress.next_index, 200);
            (prev_index, entries)
        };

        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

        let args = AppendEntriesArgs {
            term: self.state.hard_state.current_term,
            leader_id: self.node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.state.commit_index,
        };

        self.outbound_messages.push(RaftMessage {
            group_id: self.group_id,
            from: self.node_id,
            to: peer,
            payload: RaftPayload::AppendEntries(args),
        });
    }

    /// Handles incoming RequestVote RPC.
    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.state.hard_state.current_term {
            self.become_follower(args.term, None);
        }

        let mut vote_granted = false;

        if args.term == self.state.hard_state.current_term {
            let can_vote = self.state.hard_state.voted_for.is_none()
                || self.state.hard_state.voted_for == Some(args.candidate_id);

            let my_last_index = self.log.last_index();
            let my_last_term = self.log.last_term();

            let log_ok = args.last_log_term > my_last_term
                || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

            if can_vote && log_ok {
                vote_granted = true;
                self.state.hard_state.voted_for = Some(args.candidate_id);
                self.reset_election_timer();

                if let Some(ref mut store) = self.storage {
                    let _ = store.save_hard_state(&self.state.hard_state);
                }
            }
        }

        RequestVoteReply {
            term: self.state.hard_state.current_term,
            vote_granted,
        }
    }

    /// Handles incoming RequestVoteReply RPC.
    pub fn handle_request_vote_reply(&mut self, from: u64, reply: RequestVoteReply) {
        if reply.term > self.state.hard_state.current_term {
            self.become_follower(reply.term, None);
            return;
        }

        if self.state.soft_state.role != RaftRole::Candidate
            || reply.term != self.state.hard_state.current_term
        {
            return;
        }

        if let Some(ref mut tracker) = self.vote_tracker {
            tracker.register_vote(from, reply.vote_granted);
            if tracker.has_quorum() {
                self.become_leader();
            } else if tracker.is_rejected() {
                self.become_follower(self.state.hard_state.current_term, None);
            }
        }
    }

    /// Handles incoming AppendEntries RPC (heartbeats and log replication).
    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.state.hard_state.current_term {
            return AppendEntriesReply {
                term: self.state.hard_state.current_term,
                success: false,
                match_index: self.log.last_index(),
            };
        }

        if args.term > self.state.hard_state.current_term
            || self.state.soft_state.role == RaftRole::Candidate
        {
            self.become_follower(args.term, Some(args.leader_id));
        } else {
            self.state.soft_state.leader_id = Some(args.leader_id);
            self.reset_election_timer();
        }

        // Verify log consistency at prev_log_index
        if args.prev_log_index > 0 {
            if args.prev_log_index > self.log.last_index() {
                return AppendEntriesReply {
                    term: self.state.hard_state.current_term,
                    success: false,
                    match_index: self.log.last_index(),
                };
            }
            if let Some(term) = self.log.term_at(args.prev_log_index) {
                if term != args.prev_log_term {
                    // Conflict detected: truncate conflicting entry and all that follow
                    self.log.truncate_after(args.prev_log_index.saturating_sub(1));
                    if let Some(ref mut store) = self.storage {
                        let _ = store.truncate_log(args.prev_log_index.saturating_sub(1));
                    }
                    return AppendEntriesReply {
                        term: self.state.hard_state.current_term,
                        success: false,
                        match_index: self.log.last_index(),
                    };
                }
            }
        }

        // Append new entries not already present
        let mut entries_to_persist = Vec::new();
        for entry in args.entries {
            if entry.index > self.log.last_index() {
                entries_to_persist.push(entry.clone());
                self.log.append_entry(entry);
            } else if let Some(existing_term) = self.log.term_at(entry.index) {
                if existing_term != entry.term {
                    self.log.truncate_after(entry.index - 1);
                    if let Some(ref mut store) = self.storage {
                        let _ = store.truncate_log(entry.index - 1);
                    }
                    entries_to_persist.push(entry.clone());
                    self.log.append_entry(entry);
                }
            }
        }

        if !entries_to_persist.is_empty() {
            if let Some(ref mut store) = self.storage {
                let _ = store.append_entries(&entries_to_persist);
            }
        }

        // Advance follower commit_index
        if args.leader_commit > self.state.commit_index {
            self.state.commit_index = args.leader_commit.min(self.log.last_index());
            self.apply_committed();
        }

        AppendEntriesReply {
            term: self.state.hard_state.current_term,
            success: true,
            match_index: self.log.last_index(),
        }
    }

    /// Handles AppendEntriesReply from peers.
    pub fn handle_append_entries_reply(&mut self, from: u64, reply: AppendEntriesReply) {
        if reply.term > self.state.hard_state.current_term {
            self.become_follower(reply.term, None);
            return;
        }

        if self.state.soft_state.role != RaftRole::Leader
            || reply.term != self.state.hard_state.current_term
        {
            return;
        }

        if let Some(progress) = self.state.progress.get_mut(&from) {
            progress.last_ack = Instant::now();
            if reply.success {
                progress.match_index = reply.match_index;
                progress.next_index = reply.match_index + 1;
            } else {
                // Decrement next_index to locate agreement point
                if progress.next_index > 1 {
                    progress.next_index = progress.next_index.saturating_sub(1);
                    if reply.match_index < progress.next_index {
                        progress.next_index = reply.match_index + 1;
                    }
                }
            }
        }

        // Recheck majority commit index
        let new_commit = ReplicationManager::calculate_commit_index(
            self.state.hard_state.current_term,
            self.state.commit_index,
            self.log.last_index(),
            &self.state.progress,
            &self.log,
        );

        if new_commit > self.state.commit_index {
            self.state.commit_index = new_commit;
            self.apply_committed();
            // Notify followers of updated commit index
            self.send_heartbeats();
        }
    }

    /// Submits a client command to the leader for consensus replication.
    pub fn propose(&mut self, command: ClientCommand, request_id: Option<String>) -> Result<u64, RaftError> {
        if self.state.soft_state.role != RaftRole::Leader {
            return Err(RaftError::NotLeader(
                self.node_id,
                self.state.soft_state.leader_id,
            ));
        }

        let cmd_payload = serde_json::to_vec(&command).map_err(|e| RaftError::Storage(e.to_string()))?;
        let new_index = self.log.last_index() + 1;
        let entry = LogEntry {
            index: new_index,
            term: self.state.hard_state.current_term,
            payload: EntryPayload::Command(cmd_payload),
            request_id,
        };

        self.log.append_entry(entry.clone());
        if let Some(ref mut store) = self.storage {
            let _ = store.append_entries(&[entry]);
        }

        if self.peers.is_empty() {
            self.state.commit_index = new_index;
            self.apply_committed();
        } else {
            self.send_heartbeats();
        }

        Ok(new_index)
    }

    /// Applies committed log entries to the in-memory state machine.
    pub fn apply_committed(&mut self) {
        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;
            let applied_idx = self.state.last_applied;

            if let Some(entry) = self.log.get_entry(applied_idx) {
                match &entry.payload {
                    EntryPayload::Command(bytes) => {
                        if let Ok(cmd) = serde_json::from_slice::<ClientCommand>(bytes) {
                            let result = if let Some(ref store) = self.storage {
                                store.apply_command(&cmd)
                            } else {
                                ClientResult::Success(None)
                            };
                            self.applied_results.insert(applied_idx, result);
                        }
                    }
                    EntryPayload::NoOp => {
                        debug!("[Group {}] Applied No-Op at index {}", self.group_id, applied_idx);
                    }
                    EntryPayload::ConfigChange { .. } => {}
                }
            }
        }
    }

    /// Compacts log up to `index` by taking a snapshot.
    pub fn snapshot(&mut self, index: u64) -> Result<(), RaftError> {
        if index <= self.log.snapshot_last_index || index > self.state.last_applied {
            return Ok(());
        }

        let term = self.log.term_at(index).unwrap_or(0);
        let active_entries = self.log.entries_from(index + 1, usize::MAX);

        if let Some(ref mut store) = self.storage {
            store
                .create_snapshot(
                    index,
                    term,
                    &self.peers,
                    &self.state.hard_state,
                    &active_entries,
                )
                .map_err(|e| RaftError::Storage(e.to_string()))?;
        }

        self.log.compact_up_to(index, term);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_node_election_and_proposal() {
        let mut engine = RaftEngine::new(1, 1, vec![], None);
        engine.start_election();
        assert_eq!(engine.state.soft_state.role, RaftRole::Leader);

        let index = engine
            .propose(
                ClientCommand::Put {
                    key: b"k1".to_vec(),
                    val: b"v1".to_vec(),
                },
                None,
            )
            .unwrap();

        assert_eq!(index, 2); // 1 is No-Op, 2 is Put
        assert_eq!(engine.state.commit_index, 2);
        assert_eq!(engine.state.last_applied, 2);
    }
}
