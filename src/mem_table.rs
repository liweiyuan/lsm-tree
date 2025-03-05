/// MemTable
use std::sync::{Arc, atomic::AtomicUsize};

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
}
