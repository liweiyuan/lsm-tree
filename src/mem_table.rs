/// MemTable
use std::sync::{Arc, atomic::AtomicUsize};

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

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|v| v.value().clone())
    }

    /// Put a key-value pair.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let estimated_size = key.len() + value.len();
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.approximate_size
            .fetch_add(estimated_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(wal) = &mut self.wal {
            wal.put(key, value)?;
        }
        Ok(())
    }
}

/// MemTable test
#[cfg(test)]
mod tests {
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
