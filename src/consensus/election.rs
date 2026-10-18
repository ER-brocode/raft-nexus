use std::collections::HashSet;
use std::time::Duration;
use rand::Rng;

/// Manages randomized election timeouts to minimize split-vote collisions.
#[derive(Debug, Clone)]
pub struct ElectionTimer {
    min_timeout_ms: u64,
    max_timeout_ms: u64,
}

impl ElectionTimer {
    pub fn new(min_timeout_ms: u64, max_timeout_ms: u64) -> Self {
        assert!(min_timeout_ms < max_timeout_ms, "min_timeout must be less than max_timeout");
        Self {
            min_timeout_ms,
            max_timeout_ms,
        }
    }

    /// Generates a randomized timeout duration within [min, max).
    pub fn randomize_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let ms = rng.gen_range(self.min_timeout_ms..self.max_timeout_ms);
        Duration::from_millis(ms)
    }
}

impl Default for ElectionTimer {
    fn default() -> Self {
        Self::new(150, 300)
    }
}

/// Tracks votes collected during a candidate election round.
#[derive(Debug, Clone)]
pub struct VoteTracker {
    pub term: u64,
    pub votes_granted: HashSet<u64>,
    pub votes_rejected: HashSet<u64>,
    pub total_nodes: usize,
}

impl VoteTracker {
    pub fn new(term: u64, candidate_id: u64, total_nodes: usize) -> Self {
        let mut votes_granted = HashSet::new();
        votes_granted.insert(candidate_id); // Candidate automatically votes for itself
        Self {
            term,
            votes_granted,
            votes_rejected: HashSet::new(),
            total_nodes,
        }
    }

    /// Registers a vote response.
    pub fn register_vote(&mut self, voter_id: u64, granted: bool) {
        if granted {
            self.votes_granted.insert(voter_id);
            self.votes_rejected.remove(&voter_id);
        } else {
            self.votes_rejected.insert(voter_id);
            self.votes_granted.remove(&voter_id);
        }
    }

    /// Checks if a strict majority quorum has been reached.
    pub fn has_quorum(&self) -> bool {
        let quorum_needed = (self.total_nodes / 2) + 1;
        self.votes_granted.len() >= quorum_needed
    }

    /// Checks if it is impossible to win this election (majority rejected).
    pub fn is_rejected(&self) -> bool {
        let max_possible = self.total_nodes - self.votes_rejected.len();
        let quorum_needed = (self.total_nodes / 2) + 1;
        max_possible < quorum_needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_election_quorum() {
        let mut tracker = VoteTracker::new(1, 1, 3);
        assert!(!tracker.has_quorum()); // 1/3

        tracker.register_vote(2, true);
        assert!(tracker.has_quorum()); // 2/3 is quorum
    }

    #[test]
    fn test_election_rejection() {
        let mut tracker = VoteTracker::new(1, 1, 3);
        tracker.register_vote(2, false);
        tracker.register_vote(3, false);
        assert!(tracker.is_rejected());
    }
}
