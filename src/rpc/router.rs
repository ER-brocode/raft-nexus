use std::sync::RwLock;
use serde::{Deserialize, Serialize};

/// Metadata describing a key range shard and its consensus replica set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardDescriptor {
    pub group_id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub replicas: Vec<u64>,
    pub leader_hint: Option<u64>,
}

impl ShardDescriptor {
    pub fn contains(&self, key: &[u8]) -> bool {
        let after_start = self.start_key.is_empty() || key >= self.start_key.as_slice();
        let before_end = self.end_key.is_empty() || key < self.end_key.as_slice();
        after_start && before_end
    }
}

/// Shard Range Router mapping keyspaces to Multi-Raft groups.
pub struct ShardRouter {
    shards: RwLock<Vec<ShardDescriptor>>,
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardRouter {
    pub fn new() -> Self {
        Self {
            shards: RwLock::new(Vec::new()),
        }
    }

    /// Adds a new shard descriptor to the routing table.
    pub fn add_shard(&self, shard: ShardDescriptor) {
        let mut list = self.shards.write().unwrap();
        list.push(shard);
        list.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    }

    /// Locates the shard responsible for a given key.
    pub fn route_key(&self, key: &[u8]) -> Option<ShardDescriptor> {
        let list = self.shards.read().unwrap();
        for shard in list.iter() {
            if shard.contains(key) {
                return Some(shard.clone());
            }
        }
        list.first().cloned()
    }

    /// Updates the cached leader hint for a group.
    pub fn update_leader(&self, group_id: u64, leader_id: u64) {
        let mut list = self.shards.write().unwrap();
        for shard in list.iter_mut() {
            if shard.group_id == group_id {
                shard.leader_hint = Some(leader_id);
            }
        }
    }

    /// Splits a shard into two at `split_key`.
    pub fn split_shard(&self, group_id: u64, split_key: Vec<u8>, new_group_id: u64) -> bool {
        let mut list = self.shards.write().unwrap();
        let pos = list.iter().position(|s| s.group_id == group_id);
        if let Some(idx) = pos {
            let mut original = list[idx].clone();
            let new_shard = ShardDescriptor {
                group_id: new_group_id,
                start_key: split_key.clone(),
                end_key: original.end_key.clone(),
                replicas: original.replicas.clone(),
                leader_hint: None,
            };
            original.end_key = split_key;
            list[idx] = original;
            list.push(new_shard);
            list.sort_by(|a, b| a.start_key.cmp(&b.start_key));
            true
        } else {
            false
        }
    }

    /// Returns a snapshot of all active shards in the cluster.
    pub fn all_shards(&self) -> Vec<ShardDescriptor> {
        let list = self.shards.read().unwrap();
        list.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_routing_and_split() {
        let router = ShardRouter::new();
        router.add_shard(ShardDescriptor {
            group_id: 1,
            start_key: vec![],
            end_key: b"m".to_vec(),
            replicas: vec![1, 2, 3],
            leader_hint: Some(1),
        });
        router.add_shard(ShardDescriptor {
            group_id: 2,
            start_key: b"m".to_vec(),
            end_key: vec![],
            replicas: vec![1, 2, 3],
            leader_hint: Some(2),
        });

        let s1 = router.route_key(b"apple").unwrap();
        assert_eq!(s1.group_id, 1);

        let s2 = router.route_key(b"zebra").unwrap();
        assert_eq!(s2.group_id, 2);

        // Split group 2 at "t"
        assert!(router.split_shard(2, b"t".to_vec(), 3));
        let s_middle = router.route_key(b"nest").unwrap();
        assert_eq!(s_middle.group_id, 2);

        let s_end = router.route_key(b"zoo").unwrap();
        assert_eq!(s_end.group_id, 3);
    }
}
