use crate::db::io::{read_record, write_record};
use crate::{DBKey, Error, LogEntry, Result};
use crossbeam_channel::{Sender, unbounded};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A struct that represents a durable, write-ahead log.
#[derive(Debug)]
pub struct Wal<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// The path used for the WAL file on disk.
    path: PathBuf,
    /// The writer used for appending to the WAL.
    writer: BufWriter<File>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Wal<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// Creates a new WAL at the specified path.
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Opens an existing WAL at the specified path.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().append(true).open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    /// Appends a batch of entries to the WAL and returns the total bytes written.
    pub fn append_batch(&mut self, entries: &[LogEntry<K, V>]) -> Result<usize> {
        let mut total = 0;
        for entry in entries {
            total += write_record(&mut self.writer, entry)? as usize;
        }
        Ok(total)
    }

    /// Clears the WAL file.
    pub fn clear(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Flushes the WAL buffer to disk and performs a hardware sync.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Performs a hardware sync of the WAL file.
    pub fn sync_all(&self) -> Result<()> {
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Flushes the WAL buffer to the operating system.
    pub fn flush_to_os(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Returns an iterator over the entries in the WAL.
    pub fn iter(&self) -> Result<WalIterator<K, V>> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
            _phantom: PhantomData,
        })
    }
}

/// A struct that represents an iterator over the entries of a WAL file.
pub struct WalIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// The reader used for the WAL file.
    reader: BufReader<File>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Iterator for WalIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = std::result::Result<LogEntry<K, V>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        read_record(&mut self.reader).transpose()
    }
}

enum WalTask<K, V> {
    Write {
        entries: Arc<Vec<LogEntry<K, V>>>,
        resp_tx: Sender<Result<()>>,
    },
    Rotate {
        resp_tx: Sender<Result<u64>>,
    },
    Delete {
        id: u64,
        resp_tx: Sender<Result<()>>,
    },
    Flush {
        resp_tx: Sender<Result<()>>,
    },
}

struct SyncTask {
    file: File,
    resps: Vec<Sender<Result<()>>>,
}

/// A struct that coordinates group commits and WAL rotation.
#[derive(Debug)]
pub struct WalManager<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The channel used to send tasks to the WAL worker.
    task_tx: Sender<WalTask<K, V>>,
}

impl<K, V> WalManager<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Creates a new WalManager.
    pub fn new(dir: PathBuf, mut current_id: u64) -> Result<Self> {
        let (task_tx, task_rx) = crossbeam_channel::bounded::<WalTask<K, V>>(1024);
        let (sync_tx, sync_rx) = unbounded::<SyncTask>();

        let wal_path = dir.join(format!("{:06}.wal", current_id));
        let mut wal = if wal_path.exists() {
            Wal::open(&wal_path)?
        } else {
            Wal::create(&wal_path)?
        };

        std::thread::spawn(move || {
            let mut last_sync_time = std::time::Instant::now();
            let mut bytes_since_last_sync = 0usize;
            while let Ok(first_task) = task_rx.recv() {
                match first_task {
                    WalTask::Write { entries, resp_tx } => {
                        let mut batch_resps = vec![resp_tx];
                        let mut next_rotate = None;

                        // Start batch by appending first request
                        let mut first_res = wal.append_batch(&entries);
                        let mut result: Result<()> =
                            first_res.as_ref().map(|_| ()).map_err(|e| e.clone());

                        if let Ok(bytes) = first_res {
                            bytes_since_last_sync += bytes;
                        }

                        // Group multiple writes if first succeeded
                        if result.is_ok() {
                            while let Ok(next_task) = task_rx.try_recv() {
                                match next_task {
                                    WalTask::Write {
                                        entries: next_entries,
                                        resp_tx: next_resp,
                                    } => {
                                        if let Ok(bytes) = wal.append_batch(&next_entries) {
                                            bytes_since_last_sync += bytes;
                                            batch_resps.push(next_resp);
                                        } else {
                                            result = wal.append_batch(&next_entries).map(|_| ());
                                            break;
                                        }
                                    }
                                    WalTask::Rotate { resp_tx: rot_tx } => {
                                        next_rotate = Some(rot_tx);
                                        break;
                                    }
                                    _ => break,
                                }
                                if batch_resps.len() >= 1024 {
                                    break;
                                }
                            }
                        }

                        // Adaptive Sync: always flush to OS, but sync_all based on interval or size
                        if result.is_ok() {
                            if let Err(e) = wal.flush_to_os() {
                                result = Err(e);
                            }
                            if result.is_ok()
                                && (last_sync_time.elapsed()
                                    >= std::time::Duration::from_millis(10)
                                    || bytes_since_last_sync >= 1_048_576)
                            {
                                // Delegate sync_all to SyncWorker to avoid blocking the worker thread
                                if let Ok(file) = wal.writer.get_ref().try_clone() {
                                    if let Err(_) = sync_tx.send(SyncTask {
                                        file,
                                        resps: batch_resps.clone(),
                                    }) {
                                        for r in &batch_resps {
                                            let _ = r.send(Err(Error::Corruption(
                                                "Sync worker channel closed".into(),
                                            )));
                                        }
                                    }
                                } else {
                                    for r in &batch_resps {
                                        let _ = r.send(Err(Error::Corruption(
                                            "Failed to clone WAL file".into(),
                                        )));
                                    }
                                }
                                last_sync_time = std::time::Instant::now();
                                bytes_since_last_sync = 0;
                            } else if result.is_ok() {
                                for r in &batch_resps {
                                    let _ = r.send(result.clone());
                                }
                            }
                        } else {
                            for r in &batch_resps {
                                let _ = r.send(result.clone());
                            }
                        }

                        if let Some(resp) = next_rotate {
                            if let Err(e) = result {
                                let _ = resp.send(Err(e));
                            } else {
                                let old_id = current_id;
                                current_id += 1;
                                let new_path = dir.join(format!("{:06}.wal", current_id));
                                match Wal::create(&new_path) {
                                    Ok(new_wal) => {
                                        wal = new_wal;
                                        let _ = resp.send(Ok(old_id));
                                    }
                                    Err(e) => {
                                        let _ = resp.send(Err(e));
                                    }
                                }
                            }
                        }
                    }
                    WalTask::Rotate { resp_tx } => {
                        let res = wal.flush().and_then(|_| {
                            let old_id = current_id;
                            current_id += 1;
                            let new_path = dir.join(format!("{:06}.wal", current_id));
                            Wal::create(&new_path).map(|new_wal| {
                                wal = new_wal;
                                old_id
                            })
                        });
                        let _ = resp_tx.send(res);
                    }
                    WalTask::Delete { id, resp_tx } => {
                        let path = dir.join(format!("{:06}.wal", id));
                        let _ = resp_tx.send(std::fs::remove_file(path).map_err(Into::into));
                    }
                    WalTask::Flush { resp_tx } => {
                        let res = wal.flush();
                        if res.is_ok() {
                            last_sync_time = std::time::Instant::now();
                            bytes_since_last_sync = 0;
                        }
                        let _ = resp_tx.send(res);
                    }
                }
            }
        });

        std::thread::spawn(move || {
            while let Ok(SyncTask { file, resps }) = sync_rx.recv() {
                let result = file.sync_all().map_err(Into::into);
                for r in resps {
                    let _ = r.send(result.clone());
                }
            }
        });

        Ok(Self { task_tx })
    }

    /// Submits entries to the WAL for durable storage.
    pub fn submit(&self, entries: Arc<Vec<LogEntry<K, V>>>) -> Result<()> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Write { entries, resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }

    /// Rotates the current WAL file to a new one.
    pub fn rotate(&self) -> Result<u64> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Rotate { resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }

    /// Deletes a specific WAL file by ID.
    pub fn delete(&self, id: u64) -> Result<()> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Delete { id, resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }

    /// Forcefully synchronizes the WAL to disk.
    pub fn flush(&self) -> Result<()> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Flush { resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }
}
