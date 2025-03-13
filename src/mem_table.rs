/// MemTable
use std::{
    path::Path,
    sync::{Arc, atomic::AtomicUsize},
};

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::wal::Wal;

/// A baic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

impl MemTable {
    //Create a new mem-table
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with wal.
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mem_table = Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(path.as_ref())?),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        };
        Ok(mem_table)
    }

    /// Recover a mem-table from wal.
    pub fn recover_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = Arc::new(SkipMap::new());

        let mem_table = Self {
            wal: Some(Wal::recover(path.as_ref(), &map)?),
            map,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        };

        Ok(mem_table)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|v| v.value().clone())
    }

    /// Put a key-value pair.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let estimated_size = key.len() + value.len();
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }
        Ok(())
    }

    /// Get the approximate size of the mem-table.
    ///
    /// # Returns usize
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get the id of the mem-table.
    ///
    /// # Returns usize
    ///
    pub fn id(&self) -> usize {
        self.id
    }
}

/// MemTable test
#[cfg(test)]
mod tests {
    use anyhow::Ok;

    use super::*;

    #[test]
    fn test_mem_table_create() {
        let mem_table = MemTable::create(0);
        assert_eq!(mem_table.id, 0);
        assert_eq!(
            mem_table
                .approximate_size
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    /// Test mem_table by wal create
    #[test]
    fn test_mem_table_create_with_wal() -> Result<()> {
        let path = "test_wal_create.wal";
        let mem_table = MemTable::create_with_wal(0, path)?;
        assert_eq!(mem_table.id, 0);
        assert_eq!(
            mem_table
                .approximate_size
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Test mem_table by wal recover
    ///
    #[test]
    fn test_mem_table_recover_with_wal() -> Result<()> {
        let path = "test_wal_recover.wal";
        let mut mem_table = MemTable::create_with_wal(0, path)?;
        mem_table.put(b"key", b"value")?;
        std::fs::remove_file(path)?;
        assert_eq!(mem_table.id, 0);
        assert_eq!(
            mem_table
                .approximate_size
                .load(std::sync::atomic::Ordering::Relaxed),
            8
        );
        Ok(())
    }

    /// Test mem_table put
    #[test]
    fn test_mem_table_put() {
        let mut mem_table = MemTable::create(0);
        mem_table.put(b"key", b"value").unwrap();
        assert_eq!(mem_table.get(b"key"), Some(Bytes::from_static(b"value")));
    }

    /// Test mem_table get
    ///
    #[test]
    fn test_mem_table_get() {
        let mut mem_table = MemTable::create(0);
        mem_table.put(b"key", b"value").unwrap();
        assert_eq!(mem_table.get(b"key"), Some(Bytes::from_static(b"value")));
    }

    /// Test mem_table get None
    ///
    #[test]
    fn test_mem_table_get_none() {
        let mem_table = MemTable::create(0);
        assert_eq!(mem_table.get(b"key"), None);
    }
}
