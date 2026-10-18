use serde::{Deserialize, Serialize};

/// Type of entry stored in the Raft consensus log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// No-Op entry committed at the start of a leader's term.
    NoOp,
    /// State machine command (e.g. KV Put, Delete, Batch).
    Command(Vec<u8>),
    /// Cluster membership configuration change.
    ConfigChange {
        node_id: u64,
        is_add: bool,
    },
}

/// A single Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Monotonically increasing index in the log.
    pub index: u64,
    /// Term when entry was received by the leader.
    pub term: u64,
    /// Command payload for the state machine.
    pub payload: EntryPayload,
    /// Unique identifier from client for idempotent execution.
    pub request_id: Option<String>,
}

/// In-memory view of the Raft log, backed by compacted snapshots.
#[derive(Debug, Clone)]
pub struct RaftLog {
    /// Entries present in the active log.
    pub entries: Vec<LogEntry>,
    /// Index of the last entry included in the most recent snapshot.
    pub snapshot_last_index: u64,
    /// Term of the last entry included in the most recent snapshot.
    pub snapshot_last_term: u64,
}

impl RaftLog {
    /// Creates a new RaftLog with given snapshot base coordinates.
    pub fn new(snapshot_last_index: u64, snapshot_last_term: u64) -> Self {
        Self {
            entries: Vec::new(),
            snapshot_last_index,
            snapshot_last_term,
        }
    }

    /// Returns the index of the highest log entry.
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_last_index)
    }

    /// Returns the term of the highest log entry.
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_last_term)
    }

    /// Translates an absolute 1-based log index into a local vector slice index.
    fn to_local_index(&self, index: u64) -> Option<usize> {
        if index <= self.snapshot_last_index {
            None
        } else {
            let offset = (index - self.snapshot_last_index - 1) as usize;
            if offset < self.entries.len() {
                Some(offset)
            } else {
                None
            }
        }
    }

    /// Returns the term of an entry at the given index.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_last_index {
            return Some(self.snapshot_last_term);
        }
        if index < self.snapshot_last_index {
            return None;
        }
        self.to_local_index(index).map(|idx| self.entries[idx].term)
    }

    /// Returns a reference to the log entry at the specified index.
    pub fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        let local_idx = self.to_local_index(index)?;
        self.entries.get(local_idx)
    }

    /// Appends a new entry to the log.
    pub fn append_entry(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Appends multiple entries to the log.
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) {
        self.entries.extend(entries);
    }

    /// Returns a slice of entries starting at `from_index` up to `max_count`.
    pub fn entries_from(&self, from_index: u64, max_count: usize) -> Vec<LogEntry> {
        if from_index <= self.snapshot_last_index {
            return Vec::new();
        }
        let start_offset = (from_index - self.snapshot_last_index - 1) as usize;
        if start_offset >= self.entries.len() {
            return Vec::new();
        }
        let end_offset = (start_offset + max_count).min(self.entries.len());
        self.entries[start_offset..end_offset].to_vec()
    }

    /// Truncates the log after `index`, deleting conflicting uncommitted entries.
    pub fn truncate_after(&mut self, index: u64) {
        if index < self.snapshot_last_index {
            self.entries.clear();
        } else {
            let keep_len = (index - self.snapshot_last_index) as usize;
            if keep_len < self.entries.len() {
                self.entries.truncate(keep_len);
            }
        }
    }

    /// Discards all entries up to and including `index` as part of snapshot compaction.
    pub fn compact_up_to(&mut self, index: u64, term: u64) {
        if index <= self.snapshot_last_index {
            return;
        }
        let drop_count = (index - self.snapshot_last_index) as usize;
        if drop_count >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(0..drop_count);
        }
        self.snapshot_last_index = index;
        self.snapshot_last_term = term;
    }

    /// Checks if a given index is covered by the current log or snapshot.
    pub fn contains_index(&self, index: u64) -> bool {
        index >= self.snapshot_last_index && index <= self.last_index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_append_and_truncate() {
        let mut log = RaftLog::new(0, 0);
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);

        log.append_entry(LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::NoOp,
            request_id: None,
        });
        log.append_entry(LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command(b"k1=v1".to_vec()),
            request_id: Some("req-1".into()),
        });

        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(1));

        let slice = log.entries_from(1, 10);
        assert_eq!(slice.len(), 2);

        log.truncate_after(1);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.get_entry(2), None);
    }

    #[test]
    fn test_log_compaction() {
        let mut log = RaftLog::new(0, 0);
        for i in 1..=10 {
            log.append_entry(LogEntry {
                index: i,
                term: 2,
                payload: EntryPayload::NoOp,
                request_id: None,
            });
        }
        assert_eq!(log.last_index(), 10);

        log.compact_up_to(5, 2);
        assert_eq!(log.snapshot_last_index, 5);
        assert_eq!(log.snapshot_last_term, 2);
        assert_eq!(log.last_index(), 10);
        assert_eq!(log.entries.len(), 5);
        assert_eq!(log.term_at(5), Some(2));
        assert_eq!(log.term_at(4), None); // compacted away
    }
}
