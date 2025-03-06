use std::{collections::HashMap, sync::Arc};

use crate::mem_table::MemTable;

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
}

impl LsmStorageState {
    /// Create  Self
    pub fn create(option: &LsmStorageOptions) -> Self {
        Self {
            memtable: Arc::new(MemTable::create(option.block_size)),
            imm_memtable: Vec::new(),
            l0_sstable: Vec::new(),
            levels: Vec::new(),
            //sstables: HashMap::new(),
        }
    }
}
