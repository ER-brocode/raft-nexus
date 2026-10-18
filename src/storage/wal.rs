use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::consensus::log::LogEntry;
use crate::consensus::state::HardState;

const WAL_MAGIC: &[u8; 8] = b"NXWALv01";
const TYPE_HARD_STATE: u8 = 1;
const TYPE_LOG_ENTRY: u8 = 2;
const TYPE_TRUNCATE: u8 = 3;

/// Events recorded in the Write-Ahead Log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    HardState(HardState),
    LogEntry(LogEntry),
    TruncateLog(u64),
}

/// Durable append-only write-ahead log with CRC32 integrity verification.
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WriteAheadLog {
    /// Opens or creates a WAL file at the specified path.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let is_new = !path.exists() || std::fs::metadata(&path)?.len() == 0;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        if is_new {
            file.write_all(WAL_MAGIC)?;
            file.sync_all()?;
        }

        // Seek to end for writing
        file.seek(SeekFrom::End(0))?;
        let writer = BufWriter::new(file);

        Ok(Self { path, writer })
    }

    /// Replays all valid records from the WAL file.
    pub fn replay<P: AsRef<Path>>(path: P) -> io::Result<(Option<HardState>, Vec<LogEntry>)> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok((None, Vec::new()));
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 8];
        if reader.read_exact(&mut magic).is_err() || &magic != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid WAL magic header",
            ));
        }

        let mut hard_state = None;
        let mut entries = Vec::new();

        loop {
            // Read length (u32)
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut len_buf).is_err() {
                break; // Clean EOF
            }
            let len = u32::from_be_bytes(len_buf) as usize;

            // Read CRC (u32)
            let mut crc_buf = [0u8; 4];
            if reader.read_exact(&mut crc_buf).is_err() {
                break; // Corrupt EOF
            }
            let expected_crc = u32::from_be_bytes(crc_buf);

            // Read Record Type (u8)
            let mut type_buf = [0u8; 1];
            if reader.read_exact(&mut type_buf).is_err() {
                break;
            }
            let record_type = type_buf[0];

            // Read Payload
            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            // Verify Checksum
            let mut hasher = Hasher::new();
            hasher.update(&[record_type]);
            hasher.update(&payload);
            let calculated_crc = hasher.finalize();

            if calculated_crc != expected_crc {
                eprintln!("CRC mismatch in WAL at payload length {}, stopping replay", len);
                break;
            }

            match record_type {
                TYPE_HARD_STATE => {
                    if let Ok(hs) = serde_json::from_slice::<HardState>(&payload) {
                        hard_state = Some(hs);
                    }
                }
                TYPE_LOG_ENTRY => {
                    if let Ok(entry) = serde_json::from_slice::<LogEntry>(&payload) {
                        entries.push(entry);
                    }
                }
                TYPE_TRUNCATE => {
                    if let Ok(truncate_index) = serde_json::from_slice::<u64>(&payload) {
                        entries.retain(|e| e.index <= truncate_index);
                    }
                }
                _ => {}
            }
        }

        Ok((hard_state, entries))
    }

    /// Appends a raw record with CRC32 framing.
    fn append_record(&mut self, record_type: u8, data: &[u8]) -> io::Result<()> {
        let mut hasher = Hasher::new();
        hasher.update(&[record_type]);
        hasher.update(data);
        let crc = hasher.finalize();

        let len = (data.len() as u32).to_be_bytes();
        let crc_bytes = crc.to_be_bytes();

        self.writer.write_all(&len)?;
        self.writer.write_all(&crc_bytes)?;
        self.writer.write_all(&[record_type])?;
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Persists HardState update.
    pub fn write_hard_state(&mut self, state: &HardState) -> io::Result<()> {
        let payload = serde_json::to_vec(state).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.append_record(TYPE_HARD_STATE, &payload)
    }

    /// Persists a single LogEntry.
    pub fn write_log_entry(&mut self, entry: &LogEntry) -> io::Result<()> {
        let payload = serde_json::to_vec(entry).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.append_record(TYPE_LOG_ENTRY, &payload)
    }

    /// Records a log truncation event.
    pub fn write_truncate(&mut self, index: u64) -> io::Result<()> {
        let payload = serde_json::to_vec(&index).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.append_record(TYPE_TRUNCATE, &payload)
    }

    /// Forces all buffered writes to stable storage disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Rewrites the WAL file to compact it during snapshotting.
    pub fn compact(&mut self, state: &HardState, active_entries: &[LogEntry]) -> io::Result<()> {
        self.writer.flush()?;
        let temp_path = self.path.with_extension("compact.tmp");

        {
            let mut temp_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;

            temp_file.write_all(WAL_MAGIC)?;
            let mut temp_wal = WriteAheadLog {
                path: temp_path.clone(),
                writer: BufWriter::new(temp_file),
            };

            temp_wal.write_hard_state(state)?;
            for entry in active_entries {
                temp_wal.write_log_entry(entry)?;
            }
            temp_wal.sync()?;
        }

        std::fs::rename(&temp_path, &self.path)?;
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.writer.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::consensus::log::EntryPayload;

    #[test]
    fn test_wal_roundtrip() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let hs = HardState {
            current_term: 3,
            voted_for: Some(2),
        };

        let entry1 = LogEntry {
            index: 1,
            term: 3,
            payload: EntryPayload::Command(b"k1=v1".to_vec()),
            request_id: Some("req-1".into()),
        };

        let entry2 = LogEntry {
            index: 2,
            term: 3,
            payload: EntryPayload::Command(b"k2=v2".to_vec()),
            request_id: Some("req-2".into()),
        };

        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            wal.write_hard_state(&hs).unwrap();
            wal.write_log_entry(&entry1).unwrap();
            wal.write_log_entry(&entry2).unwrap();
            wal.sync().unwrap();
        }

        let (recovered_hs, recovered_entries) = WriteAheadLog::replay(&wal_path).unwrap();
        assert_eq!(recovered_hs, Some(hs));
        assert_eq!(recovered_entries.len(), 2);
        assert_eq!(recovered_entries[0], entry1);
        assert_eq!(recovered_entries[1], entry2);
    }
}
