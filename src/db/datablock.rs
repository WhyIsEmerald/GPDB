use crate::Entry;
use serde::{Deserialize, Serialize};

/// Target size for a data block
const BLOCK_SIZE: usize = 4096;

/// A DataBlock is a collection of entries packed together for I/O efficiency.
#[derive(Debug, Serialize, Deserialize)]
pub struct DataBlock<K, V> {
    entries: Vec<Entry<K, V>>,
    restart_points: Vec<u32>,
}

/// BlockBuilder is a helper struct for grouping items into DataBlocks.
pub struct BlockBuilder<T> {
    items: Vec<T>,
    current_size: usize,
    target_size: usize,
}

impl<T: Serialize> BlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        Self {
            items: Vec::new(),
            current_size: 0,
            target_size,
        }
    }

    pub fn add(&mut self, item: T) {
        let item_size = bincode::serialize(&item).unwrap_or_default().len();
        self.items.push(item);
        self.current_size += item_size;
    }

    pub fn is_full(&self) -> bool {
        self.current_size >= self.target_size
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn finish(&mut self) -> Vec<T> {
        let finished = std::mem::take(&mut self.items);
        self.current_size = 0;
        finished
    }
}
