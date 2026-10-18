use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;
use serde::{Deserialize, Serialize};

use crate::rpc::protocol::{ClientCommand, ClientResult, Mutation};

/// In-memory state machine supporting standard KV and 2PC transactions.
#[derive(Debug, Default)]
pub struct MemTable {
    /// Committed key-value store.
    store: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    /// Pending prepared 2PC transactions (tx_id -> mutations).
    pending_tx: RwLock<HashMap<u64, Vec<Mutation>>>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotData {
    kv_pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(BTreeMap::new()),
            pending_tx: RwLock::new(HashMap::new()),
        }
    }

    /// Reads a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let lock = self.store.read().unwrap();
        lock.get(key).cloned()
    }

    /// Sets a key-value pair.
    pub fn put(&self, key: Vec<u8>, val: Vec<u8>) -> Option<Vec<u8>> {
        let mut lock = self.store.write().unwrap();
        lock.insert(key, val)
    }

    /// Deletes a key.
    pub fn delete(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut lock = self.store.write().unwrap();
        lock.remove(key)
    }

    /// Scans a range of keys starting from `start` up to `limit` items.
    pub fn scan(&self, start: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let lock = self.store.read().unwrap();
        lock.range(start.to_vec()..)
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Prepares a Two-Phase Commit transaction.
    pub fn prepare_tx(&self, tx_id: u64, mutations: Vec<Mutation>) {
        let mut lock = self.pending_tx.write().unwrap();
        lock.insert(tx_id, mutations);
    }

    /// Commits a prepared Two-Phase Commit transaction.
    pub fn commit_tx(&self, tx_id: u64) -> bool {
        let mutations = {
            let mut pending = self.pending_tx.write().unwrap();
            pending.remove(&tx_id)
        };

        if let Some(mutations) = mutations {
            let mut store = self.store.write().unwrap();
            for mutation in mutations {
                match mutation {
                    Mutation::Put { key, val } => {
                        store.insert(key, val);
                    }
                    Mutation::Delete { key } => {
                        store.remove(&key);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Aborts a prepared Two-Phase Commit transaction.
    pub fn abort_tx(&self, tx_id: u64) -> bool {
        let mut pending = self.pending_tx.write().unwrap();
        pending.remove(&tx_id).is_some()
    }

    /// Applies a state machine command directly.
    pub fn apply_command(&self, command: &ClientCommand) -> ClientResult {
        match command {
            ClientCommand::Put { key, val } => {
                let prev = self.put(key.clone(), val.clone());
                ClientResult::Success(prev)
            }
            ClientCommand::Get { key } => {
                let val = self.get(key);
                ClientResult::Success(val)
            }
            ClientCommand::Delete { key } => {
                let prev = self.delete(key);
                ClientResult::Success(prev)
            }
            ClientCommand::Scan { start, limit } => {
                let results = self.scan(start, *limit);
                ClientResult::ScanResults(results)
            }
            ClientCommand::TwoPhasePrepare { tx_id, mutations } => {
                self.prepare_tx(*tx_id, mutations.clone());
                ClientResult::Success(None)
            }
            ClientCommand::TwoPhaseCommit { tx_id } => {
                let ok = self.commit_tx(*tx_id);
                if ok {
                    ClientResult::Success(None)
                } else {
                    ClientResult::Error(format!("Transaction {} not found or already completed", tx_id))
                }
            }
            ClientCommand::TwoPhaseAbort { tx_id } => {
                let ok = self.abort_tx(*tx_id);
                if ok {
                    ClientResult::Success(None)
                } else {
                    ClientResult::Error(format!("Transaction {} not found or already completed", tx_id))
                }
            }
        }
    }

    /// Exports full state machine state into serialized bytes for snapshotting.
    pub fn export_snapshot(&self) -> Vec<u8> {
        let lock = self.store.read().unwrap();
        let kv_pairs: Vec<(Vec<u8>, Vec<u8>)> = lock.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let snapshot = SnapshotData { kv_pairs };
        serde_json::to_vec(&snapshot).unwrap_or_default()
    }

    /// Imports state from a serialized snapshot.
    pub fn import_snapshot(&self, data: &[u8]) -> Result<(), String> {
        let snapshot: SnapshotData = serde_json::from_slice(data).map_err(|e| e.to_string())?;
        let mut lock = self.store.write().unwrap();
        lock.clear();
        for (k, v) in snapshot.kv_pairs {
            lock.insert(k, v);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_basic() {
        let mem = MemTable::new();
        assert_eq!(mem.get(b"foo"), None);

        mem.put(b"foo".to_vec(), b"bar".to_vec());
        assert_eq!(mem.get(b"foo"), Some(b"bar".to_vec()));

        let prev = mem.delete(b"foo");
        assert_eq!(prev, Some(b"bar".to_vec()));
        assert_eq!(mem.get(b"foo"), None);
    }

    #[test]
    fn test_memtable_2pc() {
        let mem = MemTable::new();
        let mutations = vec![
            Mutation::Put {
                key: b"k1".to_vec(),
                val: b"v1".to_vec(),
            },
            Mutation::Put {
                key: b"k2".to_vec(),
                val: b"v2".to_vec(),
            },
        ];

        mem.prepare_tx(100, mutations);
        // Not yet committed
        assert_eq!(mem.get(b"k1"), None);

        assert!(mem.commit_tx(100));
        assert_eq!(mem.get(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(mem.get(b"k2"), Some(b"v2".to_vec()));
    }
}
