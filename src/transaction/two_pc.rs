use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

use crate::consensus::multi_raft::MultiRaftNode;
use crate::rpc::protocol::{ClientCommand, ClientResult, Mutation};
use crate::rpc::router::ShardRouter;

#[derive(Debug, Error)]
pub enum TxError {
    #[error("Transaction {0} aborted during prepare phase: {1}")]
    PrepareFailed(u64, String),
    #[error("Transaction {0} aborted during commit phase: {1}")]
    CommitFailed(u64, String),
    #[error("Shard routing error: {0}")]
    Routing(String),
}

/// Coordinates distributed Two-Phase Commit transactions across Multi-Raft shards.
pub struct TwoPhaseCoordinator {
    tx_counter: AtomicU64,
    router: Arc<ShardRouter>,
}

impl TwoPhaseCoordinator {
    pub fn new(router: Arc<ShardRouter>) -> Self {
        Self {
            tx_counter: AtomicU64::new(1),
            router,
        }
    }

    /// Allocates a new unique global transaction identifier.
    pub fn new_tx_id(&self) -> u64 {
        self.tx_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Executes a distributed 2PC transaction against a local or clustered multi-raft node.
    pub fn execute_transaction(
        &self,
        node: &mut MultiRaftNode,
        mutations: Vec<Mutation>,
    ) -> Result<u64, TxError> {
        let tx_id = self.new_tx_id();

        // 1. Group mutations by target shard group
        let mut grouped_mutations: HashMap<u64, Vec<Mutation>> = HashMap::new();
        for mutation in mutations {
            let key = match &mutation {
                Mutation::Put { key, .. } => key,
                Mutation::Delete { key } => key,
            };

            let shard = self
                .router
                .route_key(key)
                .ok_or_else(|| TxError::Routing("Unable to locate shard for key".into()))?;

            grouped_mutations
                .entry(shard.group_id)
                .or_default()
                .push(mutation);
        }

        let mut prepared_groups = Vec::new();

        // 2. Phase 1: Prepare
        for (&group_id, group_muts) in &grouped_mutations {
            let prepare_cmd = ClientCommand::TwoPhasePrepare {
                tx_id,
                mutations: group_muts.clone(),
            };

            if let Some(engine) = node.groups.get_mut(&group_id) {
                match engine.propose(prepare_cmd, None) {
                    Ok(_) => {
                        prepared_groups.push(group_id);
                    }
                    Err(e) => {
                        // Abort all previously prepared groups
                        self.abort_transaction(node, tx_id, &prepared_groups);
                        return Err(TxError::PrepareFailed(tx_id, e.to_string()));
                    }
                }
            } else {
                self.abort_transaction(node, tx_id, &prepared_groups);
                return Err(TxError::PrepareFailed(
                    tx_id,
                    format!("Group {} not found on node", group_id),
                ));
            }
        }

        // 3. Phase 2: Commit
        for &group_id in &prepared_groups {
            let commit_cmd = ClientCommand::TwoPhaseCommit { tx_id };
            if let Some(engine) = node.groups.get_mut(&group_id) {
                let _ = engine.propose(commit_cmd, None);
            }
        }

        Ok(tx_id)
    }

    /// Aborts an in-flight transaction across all prepared shards.
    fn abort_transaction(&self, node: &mut MultiRaftNode, tx_id: u64, groups: &[u64]) {
        for &group_id in groups {
            let abort_cmd = ClientCommand::TwoPhaseAbort { tx_id };
            if let Some(engine) = node.groups.get_mut(&group_id) {
                let _ = engine.propose(abort_cmd, None);
            }
        }
    }
}
