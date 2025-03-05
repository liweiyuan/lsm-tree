use std::{fs::File, io::BufWriter, sync::Arc};

use anyhow::Result;
use parking_lot::Mutex;

/// wal

pub struct Wal {
    _file: Arc<Mutex<BufWriter<File>>>,
}
impl Wal {
    pub(crate) fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        /// todo!()
        Ok(())
    }
}
