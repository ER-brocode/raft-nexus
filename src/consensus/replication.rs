use std::collections::HashMap;
use crate::consensus::log::RaftLog;
use crate::consensus::state::PeerProgress;

/// Helper calculations for log replication and commit index advancement.
pub struct ReplicationManager;

impl ReplicationManager {
    /// Determines whether the leader can advance its commit_index.
    /// In Raft, if there exists an N such that N > commit_index, a majority of
    /// match_index[i] >= N, and log[N].term == current_term: set commit_index = N.
    pub fn calculate_commit_index(
        current_term: u64,
        commit_index: u64,
        local_last_index: u64,
        progress: &HashMap<u64, PeerProgress>,
        log: &RaftLog,
    ) -> u64 {
        // Collect all match indices including the leader's own last log index
        let mut match_indices: Vec<u64> = progress.values().map(|p| p.match_index).collect();
        match_indices.push(local_last_index);

        // Sort in ascending order
        match_indices.sort_unstable();

        // The median element represents the index replicated on at least a majority of nodes
        let total_nodes = match_indices.len();
        let median_idx = total_nodes.saturating_sub(1) / 2;
        let median_match = match_indices[median_idx];

        if median_match > commit_index {
            // Check Raft invariant: only log entries from the leader's current term are committed by counting replicas
            if let Some(entry_term) = log.term_at(median_match) {
                if entry_term == current_term {
                    return median_match;
                }
            }
        }

        commit_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::log::{EntryPayload, LogEntry};

    #[test]
    fn test_calculate_commit_index() {
        let mut log = RaftLog::new(0, 0);
        for i in 1..=5 {
            log.append_entry(LogEntry {
                index: i,
                term: 2,
                payload: EntryPayload::NoOp,
                request_id: None,
            });
        }

        let mut progress = HashMap::new();
        // Peer 2 has match_index 5
        let mut p2 = PeerProgress::new(6);
        p2.match_index = 5;
        progress.insert(2, p2);

        // Peer 3 has match_index 3
        let mut p3 = PeerProgress::new(4);
        p3.match_index = 3;
        progress.insert(3, p3);

        // Local leader has last_index = 5
        // Match indices: [3, 5, 5]. Median (sorted index (3-1)/2 = 1) is 5.
        // Majority is 2 nodes (peers 1 and 2), both >= 5.
        let new_commit = ReplicationManager::calculate_commit_index(2, 0, 5, &progress, &log);
        assert_eq!(new_commit, 5);
    }
}
