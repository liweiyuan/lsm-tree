use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
    thread::JoinHandle,
};

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

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
    // SST objects. todo need arc sstable, just use [u8] for now
    pub sstables: HashMap<usize, Arc<[u8]>>,
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
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    // todo need a `BlockCache`, just use () for now
    pub(crate) block_cache: Arc<()>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    // todo need a `CompactionController`, just use () for now
    pub(crate) compaction_controller: (),
    // todo need a `Manifest`, just use () for now
    pub(crate) manifest: Option<()>,
    #[allow(dead_code)]
    // todo need a `LsmMvccInner`, just use () for now
    pub(crate) mvcc: Option<()>,
    #[allow(dead_code)]
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}
impl LsmStorageInner {
    fn open(path: impl AsRef<Path>, option: LsmStorageOptions) -> Result<Self> {
        //todo
        todo!()
    }

    /// Spawn the compaction thread.
    fn spawn_compation_thread(
        &self,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        //todo
        todo!()
    }

    /// Spawn the flush thread.
    fn spawn_flush_thread(
        &self,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        //todo
        todo!()
    }
}

/// A thin wrapper for `LsmStorageInner` and the user interace for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the l0 flush thread to stop working.
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread.
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working.
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread.
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        // Notify the compaction thread to stop working.
        self.compaction_notifier.send(()).ok();
        // Notify the flush thread to stop working.
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // todo!()
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    ///
    pub fn open(path: impl AsRef<Path>, option: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, option)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compation_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flus_thread = inner.spawn_flush_thread(rx)?;
        let lsm = Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flus_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        };
        Ok(Arc::new(lsm))
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
        assert_eq!(state.sstables.len(), 0);
    }

    /// Test MiniLsm open
    ///
    ///#[test]
    fn test_minilsm_open() {
        let path = PathBuf::from("test");
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
        let lsm = MiniLsm::open(path, option).unwrap();
        //assert_eq!(lsm.inner.path, path);
        assert_eq!(lsm.inner.block_cache, Arc::new(()));
        assert_eq!(
            lsm.inner
                .next_sst_id
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        //assert_eq!(lsm.inner.options, option);
        assert_eq!(lsm.inner.compaction_controller, ());
        assert_eq!(lsm.inner.manifest, None);
        assert_eq!(lsm.inner.mvcc, None);
        /* assert_eq!(
            lsm.inner.compaction_filters,
            Arc::new(Mutex::new(Vec::new()))
        ); */
        //删除目录
        //std::fs::remove_dir_all(path).unwrap();
    }
}
