use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::consensus::log::{LogEntry, RaftLog};
use crate::consensus::state::HardState;
use crate::rpc::protocol::{ClientCommand, ClientResult};
use crate::storage::memtable::MemTable;
use crate::storage::snapshot::{Snapshot, SnapshotManager, SnapshotMetadata};
use crate::storage::wal::WriteAheadLog;

/// Storage engine integrating Write-Ahead Log, in-memory state machine, and snapshotting.
pub struct StorageEngine {
    pub group_id: u64,
    wal: WriteAheadLog,
    pub memtable: Arc<MemTable>,
    snapshot_manager: SnapshotManager,
}

impl StorageEngine {
    /// Opens or initializes a storage engine for a specific Raft consensus group.
    pub fn open<P: AsRef<Path>>(base_dir: P, group_id: u64) -> io::Result<Self> {
        let group_dir = base_dir.as_ref().join(format!("group_{}", group_id));
        std::fs::create_dir_all(&group_dir)?;

        let wal_path = group_dir.join("raft.wal");
        let wal = WriteAheadLog::open(wal_path)?;

        let snap_dir = group_dir.join("snapshots");
        let snapshot_manager = SnapshotManager::new(snap_dir);

        let memtable = Arc::new(MemTable::new());

        Ok(Self {
            group_id,
            wal,
            memtable,
            snapshot_manager,
        })
    }

    /// Recovers persistent state (HardState, Snapshots, LogEntries) on startup.
    pub fn recover(&mut self, wal_path: &Path) -> io::Result<(Option<HardState>, RaftLog)> {
        // Load latest snapshot if present
        let snapshot_opt = self.snapshot_manager.load_latest()?;
        let (snap_last_index, snap_last_term) = if let Some(ref snap) = snapshot_opt {
            let _ = self.memtable.import_snapshot(&snap.data);
            (
                snap.metadata.last_included_index,
                snap.metadata.last_included_term,
            )
        } else {
            (0, 0)
        };

        let mut log = RaftLog::new(snap_last_index, snap_last_term);

        // Replay WAL entries
        let (hard_state, wal_entries) = WriteAheadLog::replay(wal_path)?;
        for entry in wal_entries {
            if entry.index > snap_last_index {
                log.append_entry(entry);
            }
        }

        Ok((hard_state, log))
    }

    /// Persists updated hard state to WAL.
    pub fn save_hard_state(&mut self, state: &HardState) -> io::Result<()> {
        self.wal.write_hard_state(state)?;
        self.wal.sync()
    }

    /// Appends new log entries to the WAL.
    pub fn append_entries(&mut self, entries: &[LogEntry]) -> io::Result<()> {
        for entry in entries {
            self.wal.write_log_entry(entry)?;
        }
        self.wal.sync()
    }

    /// Persists a log truncation event to WAL.
    pub fn truncate_log(&mut self, index: u64) -> io::Result<()> {
        self.wal.write_truncate(index)?;
        self.wal.sync()
    }

    /// Applies a committed command directly to the state machine.
    pub fn apply_command(&self, cmd: &ClientCommand) -> ClientResult {
        self.memtable.apply_command(cmd)
    }

    /// Creates a point-in-time snapshot, persists it, and compacts the WAL.
    pub fn create_snapshot(
        &mut self,
        last_index: u64,
        last_term: u64,
        peers: &[u64],
        current_hard_state: &HardState,
        active_entries: &[LogEntry],
    ) -> io::Result<Snapshot> {
        let snapshot_data = self.memtable.export_snapshot();
        let metadata = SnapshotMetadata {
            last_included_index: last_index,
            last_included_term: last_term,
            peers: peers.to_vec(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        let snapshot = Snapshot {
            metadata,
            data: snapshot_data,
        };

        // Save snapshot file
        self.snapshot_manager.save(&snapshot)?;

        // Compact WAL to remove compacted entries
        self.wal.compact(current_hard_state, active_entries)?;

        Ok(snapshot)
    }

    /// Restores full state machine state from an incoming snapshot.
    pub fn restore_snapshot(&mut self, snapshot: &Snapshot) -> io::Result<()> {
        self.memtable
            .import_snapshot(&snapshot.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.snapshot_manager.save(snapshot)?;
        Ok(())
    }
}
