use std::sync::Arc;
use tempfile::tempdir;

use raft_nexus::consensus::multi_raft::MultiRaftNode;
use raft_nexus::rpc::protocol::{ClientCommand, Mutation};
use raft_nexus::rpc::router::{ShardDescriptor, ShardRouter};
use raft_nexus::storage::engine::StorageEngine;
use raft_nexus::transaction::two_pc::TwoPhaseCoordinator;

#[test]
fn test_multi_raft_sharding_and_storage_engine() {
    let dir = tempdir().unwrap();
    let mut node = MultiRaftNode::new(1, Some(dir.path().to_path_buf()));

    // Shard 1: [a .. m) -> Group 1
    node.add_group(1, vec![]);
    node.add_range(1, b"a".to_vec(), b"m".to_vec());

    // Shard 2: [m .. z) -> Group 2
    node.add_group(2, vec![]);
    node.add_range(2, b"m".to_vec(), b"z".to_vec());

    // Make both groups leaders locally
    node.groups.get_mut(&1).unwrap().become_leader();
    node.groups.get_mut(&2).unwrap().become_leader();

    // 1. Propose key in Group 1
    let idx1 = node
        .execute(
            b"apple",
            ClientCommand::Put {
                key: b"apple".to_vec(),
                val: b"fruit".to_vec(),
            },
        )
        .expect("write to shard 1");

    // 2. Propose key in Group 2
    let idx2 = node
        .execute(
            b"zebra",
            ClientCommand::Put {
                key: b"zebra".to_vec(),
                val: b"animal".to_vec(),
            },
        )
        .expect("write to shard 2");

    assert!(idx1 > 0);
    assert!(idx2 > 0);

    // Verify state machine values
    let g1 = node.groups.get(&1).unwrap();
    let val_apple = g1.storage.as_ref().unwrap().memtable.get(b"apple");
    assert_eq!(val_apple, Some(b"fruit".to_vec()));

    let g2 = node.groups.get(&2).unwrap();
    let val_zebra = g2.storage.as_ref().unwrap().memtable.get(b"zebra");
    assert_eq!(val_zebra, Some(b"animal".to_vec()));
}

#[test]
fn test_cross_shard_two_phase_commit() {
    let dir = tempdir().unwrap();
    let mut node = MultiRaftNode::new(1, Some(dir.path().to_path_buf()));

    let router = Arc::new(ShardRouter::new());
    router.add_shard(ShardDescriptor {
        group_id: 1,
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        replicas: vec![1],
        leader_hint: Some(1),
    });
    router.add_shard(ShardDescriptor {
        group_id: 2,
        start_key: b"m".to_vec(),
        end_key: b"z".to_vec(),
        replicas: vec![1],
        leader_hint: Some(1),
    });

    node.add_group(1, vec![]);
    node.add_range(1, b"a".to_vec(), b"m".to_vec());
    node.add_group(2, vec![]);
    node.add_range(2, b"m".to_vec(), b"z".to_vec());

    node.groups.get_mut(&1).unwrap().become_leader();
    node.groups.get_mut(&2).unwrap().become_leader();

    let coordinator = TwoPhaseCoordinator::new(router);

    let mutations = vec![
        Mutation::Put {
            key: b"banana".to_vec(), // Shard 1
            val: b"yellow".to_vec(),
        },
        Mutation::Put {
            key: b"orange".to_vec(), // Shard 2
            val: b"citrus".to_vec(),
        },
    ];

    let tx_id = coordinator
        .execute_transaction(&mut node, mutations)
        .expect("2PC transaction succeeds");

    assert!(tx_id > 0);

    // Apply committed entries in both groups
    node.groups.get_mut(&1).unwrap().apply_committed();
    node.groups.get_mut(&2).unwrap().apply_committed();

    let g1 = node.groups.get(&1).unwrap();
    assert_eq!(
        g1.storage.as_ref().unwrap().memtable.get(b"banana"),
        Some(b"yellow".to_vec())
    );

    let g2 = node.groups.get(&2).unwrap();
    assert_eq!(
        g2.storage.as_ref().unwrap().memtable.get(b"orange"),
        Some(b"citrus".to_vec())
    );
}

#[test]
fn test_snapshot_compaction_and_recovery() {
    let dir = tempdir().unwrap();
    let group_id = 1;

    let mut engine = StorageEngine::open(dir.path(), group_id).unwrap();

    // Store keys in memtable
    engine.apply_command(&ClientCommand::Put {
        key: b"hero".to_vec(),
        val: b"batman".to_vec(),
    });

    let hs = raft_nexus::consensus::state::HardState {
        current_term: 5,
        voted_for: Some(1),
    };
    engine.save_hard_state(&hs).unwrap();

    let snapshot = engine
        .create_snapshot(10, 5, &[1, 2, 3], &hs, &[])
        .expect("snapshot creation succeeds");

    assert_eq!(snapshot.metadata.last_included_index, 10);
    assert_eq!(snapshot.metadata.last_included_term, 5);

    // Reopen and recover
    let wal_path = dir.path().join("group_1/raft.wal");
    let mut new_engine = StorageEngine::open(dir.path(), group_id).unwrap();
    let (recovered_hs, recovered_log) = new_engine.recover(&wal_path).unwrap();

    assert_eq!(recovered_hs, Some(hs));
    assert_eq!(recovered_log.snapshot_last_index, 10);
    assert_eq!(recovered_log.snapshot_last_term, 5);
    assert_eq!(
        new_engine.memtable.get(b"hero"),
        Some(b"batman".to_vec())
    );
}
