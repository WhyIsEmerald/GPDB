use crate::Entry;
use serde::{Deserialize, Serialize};

/// Target size for a data block (4KB)
pub const BLOCK_SIZE: usize = 4096;
/// Number of entries between restart points
pub const RESTART_INTERVAL: usize = 16;

/// A DataBlock is a collection of entries packed together with jump points.
#[derive(Debug, Serialize, Deserialize)]
pub struct DataBlock<K, V> {
    pub entries: Vec<Entry<K, V>>,
    /// Offsets into the `entries` vector to allow for binary search inside the block.
    pub restart_points: Vec<u32>,
}

/// BlockBuilder helps group items into DataBlocks by tracking estimated size and restart points.
pub struct BlockBuilder<T> {
    items: Vec<T>,
    restart_points: Vec<u32>,
    current_size: usize,
    target_size: usize,
}

impl<T: Serialize> BlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        Self {
            items: Vec::new(),
            restart_points: Vec::new(),
            current_size: 0,
            target_size,
        }
    }

    pub fn add(&mut self, item: T) {
        if self.items.len() % RESTART_INTERVAL == 0 {
            self.restart_points.push(self.items.len() as u32);
        }

        let item_size = bincode::serialize(&item).unwrap_or_default().len() + 12;
        self.items.push(item);
        self.current_size += item_size;
    }

    pub fn is_full(&self) -> bool {
        self.current_size >= self.target_size
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the entries and restart points to create a DataBlock.
    pub fn finish(&mut self) -> (Vec<T>, Vec<u32>) {
        let finished_items = std::mem::take(&mut self.items);
        let finished_restarts = std::mem::take(&mut self.restart_points);
        self.current_size = 0;
        (finished_items, finished_restarts)
    }
}
