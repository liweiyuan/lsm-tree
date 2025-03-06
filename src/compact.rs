mod leveld;
mod simple_leveld;
mod tiered;

pub use leveld::LeveledCompactionOptions;
pub use simple_leveld::SimpleLeveledCompactionOptions;
pub use tiered::TieredCompactionOptions;

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}
