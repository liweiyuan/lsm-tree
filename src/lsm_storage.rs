use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
    thread::JoinHandle,
};

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

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

    /// Get a key from the storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    /// Put a key-value pair into the storage.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }
    /// Delete a key from the storage.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }
}

impl LsmStorageInner {
    fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn open(path: impl AsRef<Path>, option: LsmStorageOptions) -> Result<Self> {
        //todo
        todo!()
    }

    /// Get the value for the given key.
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; //drop global lock here
        // search on the current memtable
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }

        /// search on immutable memtablse.
        for memtable in snapshot.imm_memtable.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        /// search on sstables. todo
        Ok(None)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage.
    ///
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Delete a key from the storage.
    ///
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memetable could hava already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
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

    /// Force freeze the current memtable to an immutable memtable
    fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        //todo

        Ok(())
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // Swap the current memtable with a new one
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the old memtable to the immutable memtable list
        snapshot.imm_memtable.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);
        drop(guard);
        // todo 同步wal
        //old_memtable.sys_wal()?;
        Ok(())
    }
}

/// 测试
#[cfg(test)]
mod tests {
    use super::*;

    /// Test format!
    ///
    #[test]
    fn test_format() {
        let s = format!("{:05}.wal", &1);
        assert_eq!(s, "00001.wal");
    }

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
    //#[test]
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
