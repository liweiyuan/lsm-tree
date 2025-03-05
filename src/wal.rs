use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

/// wal

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}
impl Wal {
    /// Create Wal from file.
    ///
    /// # Arguments
    /// * `path` - File
    ///
    /// # Returns
    /// * `Result<Wal>`
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new()
                .read(true)
                .create_new(true)
                .write(true)
                .open(path)
                .context("Failed to open wal file")?,
        )));
        Ok(Self { file })
    }
    pub(crate) fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        /// todo!()
        Ok(())
    }

    /// Recover wal from file.
    pub(crate) fn recover(path: &Path, map: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        //todo!()

        let file = Arc::new(Mutex::new(BufWriter::new(
            OpenOptions::new()
                .read(true)
                .create_new(true)
                .write(true)
                .open(path)
                .context("Failed to open wal file")?,
        )));
        Ok(Self { file })
    }
}

/// Test mod
///
///
#[cfg(test)]
mod tests {
    use super::*;

    /// Test wal create
    ///
    #[test]
    fn test_wal_create() -> Result<()> {
        let path = "test_create.wal";
        let wal = Wal::create(path);
        assert!(wal.is_ok());
        // remove test.wal
        std::fs::remove_file(path)?;
        Ok(())
    }
}
