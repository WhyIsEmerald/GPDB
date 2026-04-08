use crate::{DBKey, ValueEntry};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use std::marker::PhantomData;

/// Target size for a data block (4KB)
pub const BLOCK_SIZE: usize = 4096;
/// Number of entries between restart points
pub const RESTART_INTERVAL: usize = 16;

/// A DataBlock using Delta Encoding (Prefix Compression).
/// The restart points allow binary search by jumping to full-key entries.
#[derive(Debug, Serialize, Deserialize)]
pub struct DataBlock<K, V> {
    pub data: Vec<u8>,
    pub restart_points: Vec<u32>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> DataBlock<K, V> {
    pub fn new(data: Vec<u8>, restart_points: Vec<u32>) -> Self {
        Self {
            data,
            restart_points,
            _phantom: PhantomData,
        }
    }

    pub fn iter(&self) -> DataBlockIterator<'_, K, V> {
        DataBlockIterator {
            block: self,
            cursor: Cursor::new(&self.data),
            last_key_bytes: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

/// BlockBuilder helps group items into DataBlocks using Prefix Compression.
pub struct DeltaBlockBuilder<K, V> {
    data: Vec<u8>,
    restart_points: Vec<u32>,
    last_key_bytes: Vec<u8>,
    count: usize,
    target_size: usize,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> DeltaBlockBuilder<K, V>
where
    K: DBKey,
    V: Serialize,
{
    pub fn new(target_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(target_size),
            restart_points: Vec::new(),
            last_key_bytes: Vec::new(),
            count: 0,
            target_size,
            _phantom: PhantomData,
        }
    }

    /// Adds an entry using prefix compression.
    pub fn add(&mut self, key: &K, value: &ValueEntry<V>) {
        let key_bytes = bincode::serialize(key).unwrap_or_default();
        let val_bytes = bincode::serialize(value).unwrap_or_default();

        let mut shared = 0;

        if self.count % RESTART_INTERVAL == 0 {
            self.restart_points.push(self.data.len() as u32);
        } else {
            // Calculate shared prefix length
            let min_len = std::cmp::min(self.last_key_bytes.len(), key_bytes.len());
            while shared < min_len && self.last_key_bytes[shared] == key_bytes[shared] {
                shared += 1;
            }
        }

        let unshared = key_bytes.len() - shared;

        // [shared: u32][unshared: u32][val_len: u32][suffix][value]
        self.data.extend_from_slice(&(shared as u32).to_le_bytes());
        self.data
            .extend_from_slice(&(unshared as u32).to_le_bytes());
        self.data
            .extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
        self.data.extend_from_slice(&key_bytes[shared..]);
        self.data.extend_from_slice(&val_bytes);

        self.last_key_bytes = key_bytes;
        self.count += 1;
    }

    pub fn is_full(&self) -> bool {
        self.data.len() + (self.restart_points.len() * 4) >= self.target_size
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn finish(&mut self) -> DataBlock<K, V> {
        let data = std::mem::take(&mut self.data);
        let restart_points = std::mem::take(&mut self.restart_points);
        self.last_key_bytes.clear();
        self.count = 0;
        DataBlock::new(data, restart_points)
    }
}

/// Iterator for reconstructing delta-encoded entries.
pub struct DataBlockIterator<'a, K, V> {
    block: &'a DataBlock<K, V>,
    cursor: Cursor<&'a Vec<u8>>,
    last_key_bytes: Vec<u8>,
    _phantom: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for DataBlockIterator<'a, K, V>
where
    K: DBKey,
    V: serde::de::DeserializeOwned,
{
    type Item = crate::Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.position() as usize >= self.block.data.len() {
            return None;
        }

        let mut buf_4 = [0u8; 4];

        self.cursor.read_exact(&mut buf_4).ok()?;
        let shared = u32::from_le_bytes(buf_4) as usize;

        self.cursor.read_exact(&mut buf_4).ok()?;
        let unshared = u32::from_le_bytes(buf_4) as usize;

        self.cursor.read_exact(&mut buf_4).ok()?;
        let val_len = u32::from_le_bytes(buf_4) as usize;

        let mut key_bytes = Vec::with_capacity(shared + unshared);
        if shared > 0 {
            key_bytes.extend_from_slice(&self.last_key_bytes[..shared]);
        }
        let mut suffix = vec![0u8; unshared];
        self.cursor.read_exact(&mut suffix).ok()?;
        key_bytes.extend_from_slice(&suffix);

        let mut val_bytes = vec![0u8; val_len];
        self.cursor.read_exact(&mut val_bytes).ok()?;

        let key: K = bincode::deserialize(&key_bytes).ok()?;
        let value: ValueEntry<V> = bincode::deserialize(&val_bytes).ok()?;

        self.last_key_bytes = key_bytes;
        Some(crate::Entry { key, value })
    }
}
