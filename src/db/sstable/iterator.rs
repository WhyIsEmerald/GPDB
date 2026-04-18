use crate::db::io::read_record;
use crate::db::sstable::datablock::DataBlock;
use crate::{DBKey, Entry, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::{BufReader, Seek};
use std::marker::PhantomData;

pub struct SSTableIterator<K, V> {
    pub(crate) reader: BufReader<File>,
    pub(crate) data_end_offset: u64,
    pub(crate) current_block: Option<DataBlock<K, V>>,
    pub(crate) current_idx: usize,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<K, V> SSTableIterator<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    pub(crate) fn new(reader: BufReader<File>, data_end_offset: u64) -> Self {
        Self {
            reader,
            data_end_offset,
            current_block: None,
            current_idx: 0,
            _phantom: PhantomData,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        let current_pos = self.reader.stream_position()?;
        if current_pos >= self.data_end_offset {
            return Ok(false);
        }

        match read_record(&mut self.reader)? {
            Some(block) => {
                self.current_block = Some(block);
                self.current_idx = 0;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl<K, V> Iterator for SSTableIterator<K, V>
where
    K: DBKey,
    V: Serialize + DeserializeOwned,
{
    type Item = Result<Entry<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(block) = &self.current_block {
                if let Some(entry) = block.iter().nth(self.current_idx) {
                    self.current_idx += 1;
                    return Some(Ok(entry));
                }
            }

            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
