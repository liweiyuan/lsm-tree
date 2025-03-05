use std::{fs::File, io::BufWriter, sync::Arc};

use parking_lot::Mutex;

/// wal

pub struct Wal {
    _file: Arc<Mutex<BufWriter<File>>>,
}
