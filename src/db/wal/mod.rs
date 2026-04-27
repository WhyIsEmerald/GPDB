use crate::db::io::{read_record, write_record};
use crate::{DBKey, Error, LogEntry, Result};
use crossbeam_channel::{Receiver, Sender, unbounded};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// `Wal` provides a durable, write-ahead log.
#[derive(Debug)]
pub struct Wal<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    path: PathBuf,
    writer: BufWriter<File>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Wal<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
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

    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().write(true).append(true).open(path)?;

        Ok(Wal {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _phantom: PhantomData,
        })
    }

    pub fn append_batch(&mut self, entries: &[LogEntry<K, V>]) -> Result<()> {
        for entry in entries {
            write_record(&mut self.writer, entry)?;
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn iter(&self) -> Result<WalIterator<K, V>> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(WalIterator {
            reader: BufReader::new(file),
            _phantom: PhantomData,
        })
    }
}

pub struct WalIterator<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
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
}

/// `WalManager` coordinates Group Commits and WAL rotation.
#[derive(Debug)]
pub struct WalManager<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    task_tx: Sender<WalTask<K, V>>,
}

impl<K, V> WalManager<K, V>
where
    K: DBKey + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(dir: PathBuf, mut current_id: u64) -> Result<Self> {
        let (task_tx, task_rx): (Sender<WalTask<K, V>>, Receiver<WalTask<K, V>>) = unbounded();

        let wal_path = dir.join(format!("{:06}.wal", current_id));
        let mut wal = if wal_path.exists() {
            Wal::open(&wal_path)?
        } else {
            Wal::create(&wal_path)?
        };

        std::thread::spawn(move || {
            while let Ok(first_task) = task_rx.recv() {
                match first_task {
                    WalTask::Write { entries, resp_tx } => {
                        let mut batch_resps = vec![resp_tx];
                        let mut next_rotate = None;

                        // Start batch by appending first request
                        let mut result = wal.append_batch(&entries);

                        // Group multiple writes if first succeeded
                        if result.is_ok() {
                            while let Ok(next_task) = task_rx.try_recv() {
                                match next_task {
                                    WalTask::Write {
                                        entries: next_entries,
                                        resp_tx: next_resp,
                                    } => {
                                        result = wal.append_batch(&next_entries);
                                        batch_resps.push(next_resp);
                                        if result.is_err() {
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

                        // Flush only if all appends succeeded
                        if result.is_ok() {
                            result = wal.flush();
                        }

                        for r in batch_resps {
                            let _ = r.send(result.clone());
                        }

                        if let Some(resp) = next_rotate {
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
                    WalTask::Rotate { resp_tx } => {
                        let old_id = current_id;
                        current_id += 1;
                        let new_path = dir.join(format!("{:06}.wal", current_id));
                        match Wal::create(&new_path) {
                            Ok(new_wal) => {
                                wal = new_wal;
                                let _ = resp_tx.send(Ok(old_id));
                            }
                            Err(e) => {
                                let _ = resp_tx.send(Err(e));
                            }
                        }
                    }
                    WalTask::Delete { id, resp_tx } => {
                        let path = dir.join(format!("{:06}.wal", id));
                        let _ = resp_tx.send(std::fs::remove_file(path).map_err(Into::into));
                    }
                }
            }
        });

        Ok(Self { task_tx })
    }

    pub fn submit(&self, entries: Arc<Vec<LogEntry<K, V>>>) -> Result<()> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Write { entries, resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }

    pub fn rotate(&self) -> Result<u64> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Rotate { resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }

    pub fn delete(&self, id: u64) -> Result<()> {
        let (resp_tx, resp_rx) = unbounded();
        self.task_tx
            .send(WalTask::Delete { id, resp_tx })
            .map_err(|_| Error::Corruption("WAL worker crashed".into()))?;
        resp_rx
            .recv()
            .map_err(|_| Error::Corruption("WAL worker dropped response".into()))?
    }
}
