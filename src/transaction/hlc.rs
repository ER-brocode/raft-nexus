use std::cmp::Ordering;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

/// Hybrid Logical Clock timestamp providing causal ordering and strict serializability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HlcTimestamp {
    pub physical_time_ms: u64,
    pub logical_counter: u32,
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical_time_ms.cmp(&other.physical_time_ms) {
            Ordering::Equal => self.logical_counter.cmp(&other.logical_counter),
            other_order => other_order,
        }
    }
}

/// Thread-safe Hybrid Logical Clock (HLC).
pub struct HybridLogicalClock {
    latest_physical: AtomicU64,
    latest_logical: AtomicU32,
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        let now = Self::system_physical_time();
        Self {
            latest_physical: AtomicU64::new(now),
            latest_logical: AtomicU32::new(0),
        }
    }

    fn system_physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Generates a monotonically increasing HLC timestamp for a local event.
    pub fn now(&self) -> HlcTimestamp {
        let phys_now = Self::system_physical_time();
        let mut old_phys = self.latest_physical.load(AtomicOrdering::Relaxed);

        loop {
            let (new_phys, new_logical) = if phys_now > old_phys {
                (phys_now, 0)
            } else {
                let logical = self.latest_logical.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                (old_phys, logical)
            };

            match self.latest_physical.compare_exchange_weak(
                old_phys,
                new_phys,
                AtomicOrdering::SeqCst,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    if new_phys > old_phys {
                        self.latest_logical.store(0, AtomicOrdering::SeqCst);
                    }
                    return HlcTimestamp {
                        physical_time_ms: new_phys,
                        logical_counter: new_logical,
                    };
                }
                Err(actual) => {
                    old_phys = actual;
                }
            }
        }
    }

    /// Updates internal HLC upon receiving a message with a remote timestamp.
    pub fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
        let phys_now = Self::system_physical_time();
        let mut old_phys = self.latest_physical.load(AtomicOrdering::Relaxed);

        loop {
            let max_phys = phys_now.max(old_phys).max(remote.physical_time_ms);
            let old_logical = self.latest_logical.load(AtomicOrdering::Relaxed);

            let new_logical = if max_phys == old_phys && max_phys == remote.physical_time_ms {
                old_logical.max(remote.logical_counter) + 1
            } else if max_phys == old_phys {
                old_logical + 1
            } else if max_phys == remote.physical_time_ms {
                remote.logical_counter + 1
            } else {
                0
            };

            match self.latest_physical.compare_exchange_weak(
                old_phys,
                max_phys,
                AtomicOrdering::SeqCst,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => {
                    self.latest_logical.store(new_logical, AtomicOrdering::SeqCst);
                    return HlcTimestamp {
                        physical_time_ms: max_phys,
                        logical_counter: new_logical,
                    };
                }
                Err(actual) => {
                    old_phys = actual;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_monotonicity() {
        let hlc = HybridLogicalClock::new();
        let t1 = hlc.now();
        let t2 = hlc.now();
        assert!(t2 >= t1);

        let remote = HlcTimestamp {
            physical_time_ms: t2.physical_time_ms + 1000,
            logical_counter: 5,
        };

        let t3 = hlc.update(remote);
        assert!(t3 > remote);
    }
}
