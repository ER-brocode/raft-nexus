use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

/// Snapshot metadata identifying the point in the consensus log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub peers: Vec<u64>,
    pub timestamp_ms: u64,
}

/// A complete point-in-time state machine snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub data: Vec<u8>,
}

/// Snapshot persistence and chunking manager.
pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let _ = fs::create_dir_all(&dir);
        Self { dir }
    }

    /// Saves a snapshot to disk atomically via a temporary file.
    pub fn save(&self, snapshot: &Snapshot) -> io::Result<PathBuf> {
        let filename = format!(
            "snap_{}_{}.snap",
            snapshot.metadata.last_included_index, snapshot.metadata.last_included_term
        );
        let final_path = self.dir.join(&filename);
        let temp_path = self.dir.join(format!("{}.tmp", filename));

        let encoded = serde_json::to_vec(snapshot).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut file = File::create(&temp_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);

        fs::rename(temp_path, &final_path)?;
        Ok(final_path)
    }

    /// Finds and loads the latest snapshot from disk.
    pub fn load_latest(&self) -> io::Result<Option<Snapshot>> {
        if !self.dir.exists() {
            return Ok(None);
        }

        let mut latest_snap: Option<(u64, PathBuf)> = None;

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if file_name.starts_with("snap_") && file_name.ends_with(".snap") {
                    let parts: Vec<&str> = file_name
                        .trim_start_matches("snap_")
                        .trim_end_matches(".snap")
                        .split('_')
                        .collect();
                    if parts.len() == 2 {
                        if let Ok(idx) = parts[0].parse::<u64>() {
                            match &latest_snap {
                                Some((max_idx, _)) if idx > *max_idx => {
                                    latest_snap = Some((idx, path));
                                }
                                None => {
                                    latest_snap = Some((idx, path));
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        if let Some((_, path)) = latest_snap {
            let mut file = File::open(path)?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            let snapshot: Snapshot = serde_json::from_slice(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }
}

/// Helper for splitting a snapshot into chunks for network transmission.
pub struct SnapshotChunker<'a> {
    data: &'a [u8],
    chunk_size: usize,
    offset: usize,
}

impl<'a> SnapshotChunker<'a> {
    pub fn new(data: &'a [u8], chunk_size: usize) -> Self {
        Self {
            data,
            chunk_size: chunk_size.max(1),
            offset: 0,
        }
    }

    /// Returns the next chunk: (offset, chunk_bytes, is_done).
    pub fn next_chunk(&mut self) -> Option<(u64, Vec<u8>, bool)> {
        if self.offset >= self.data.len() && !self.data.is_empty() {
            return None;
        }

        let current_offset = self.offset;
        let end = (self.offset + self.chunk_size).min(self.data.len());
        let chunk = self.data[self.offset..end].to_vec();
        self.offset = end;
        let done = self.offset >= self.data.len();

        Some((current_offset as u64, chunk, done))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_snapshot_save_load() {
        let dir = tempdir().unwrap();
        let manager = SnapshotManager::new(dir.path());

        let snapshot = Snapshot {
            metadata: SnapshotMetadata {
                last_included_index: 42,
                last_included_term: 3,
                peers: vec![1, 2, 3],
                timestamp_ms: 1000,
            },
            data: b"state-data".to_vec(),
        };

        manager.save(&snapshot).unwrap();

        let loaded = manager.load_latest().unwrap().expect("snapshot exists");
        assert_eq!(loaded.metadata.last_included_index, 42);
        assert_eq!(loaded.metadata.last_included_term, 3);
        assert_eq!(loaded.data, b"state-data");
    }

    #[test]
    fn test_snapshot_chunking() {
        let data = b"Hello, World! Multi-Raft Snapshot Chunking Test";
        let mut chunker = SnapshotChunker::new(data, 10);

        let mut received = Vec::new();
        while let Some((offset, chunk, _done)) = chunker.next_chunk() {
            assert_eq!(offset as usize, received.len());
            received.extend_from_slice(&chunk);
        }
        assert_eq!(received, data);
    }
}
