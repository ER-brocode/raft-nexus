//! raft-nexus: Multi-Raft Distributed Consensus & Transactional Key-Value Fabric.
//!
//! Designed for high-throughput, low-latency distributed storage with range-based
//! Multi-Raft partitioning, linearizable operations, Write-Ahead Logging (WAL) with
//! zero-copy snapshots, and Hybrid Logical Clock (HLC) distributed transactions.

pub mod consensus;
pub mod rpc;
pub mod storage;
pub mod transaction;

pub use consensus::multi_raft::MultiRaftNode;
pub use consensus::raft::RaftEngine;
pub use rpc::protocol::*;
pub use storage::engine::StorageEngine;
