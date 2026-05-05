use crate::db::io::read_record;
use crate::db::sstable::datablock::{DataBlock, DataBlockIterator};
use crate::{DBKey, Entry, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::{BufReader, Seek};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct SSTableIterator<K, V> {
    pub(crate) reader: BufReader<File>,
    pub(crate) data_end_offset: u64,
    pub(crate) current_block: Option<Arc<DataBlock<K, V>>>,
    pub(crate) current_iter: Option<DataBlockIterator<K, V>>,
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
            current_iter: None,
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
                let block_arc: Arc<DataBlock<K, V>> = Arc::new(block);
                self.current_iter = Some(block_arc.iter());
                self.current_block = Some(block_arc);
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
            if let Some(iter) = &mut self.current_iter {
                if let Some(entry) = iter.next() {
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
