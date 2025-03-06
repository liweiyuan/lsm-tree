use std::{collections::HashMap, sync::Arc};

use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions},
    mem_table::MemTable,
};

/// Represents the state of the storage engine.
///
#[derive(Clone)]
pub struct LsmStorageState {
    ///The current memtable
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest
    pub imm_memtable: Vec<Arc<MemTable>>,
    /// L0 SSTs, from lastest to earliest
    pub l0_sstable: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    // SST objects.
    //pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approxmiate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to l0 when it reaches the limit
    pub num_memttable_limit: usize,
    // Compaction option
    pub compaction_options: CompactionOptions,
    // Enable wal
    pub enable_wal: bool,
    // Searialized
    pub serialized: bool,
}

impl LsmStorageState {
    /// Create  Self
    pub fn create(option: &LsmStorageOptions) -> Self {
        let levels = match &option.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => Vec::new(),
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtable: Vec::new(),
            l0_sstable: Vec::new(),
            levels,
            // todo!()
            //sstables: HashMap::new(),
        }
    }
}

/// 测试
#[cfg(test)]
mod tests {
    use super::*;

    /// Test lsmStorageState create
    ///
    #[test]
    fn test_lsm_storage_state_create() {
        let option = LsmStorageOptions {
            block_size: 1024,
            target_sst_size: 1024 * 1024,
            num_memttable_limit: 10,
            compaction_options: CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                size_ratio_percent: 10,
                level0_file_num_compaction_trigger: 10,
                max_levels: 10,
            }),
            enable_wal: true,
            serialized: true,
        };
        let state = LsmStorageState::create(&option);
        assert_eq!(state.memtable.id(), 0);
        assert_eq!(state.imm_memtable.len(), 0);
        assert_eq!(state.l0_sstable.len(), 0);
        assert_eq!(state.levels.len(), 10);
        // assert_eq!(state.sstables.len(), 0);
    }
}
