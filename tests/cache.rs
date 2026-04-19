use gpdb::SSTableId;
use gpdb::db::cache::BlockCache;
use gpdb::db::sstable::datablock::DataBlock;
use std::sync::Arc;

#[test]
fn test_block_cache_hit_miss() {
    let cache: BlockCache<String, String> = BlockCache::new(1024 * 1024); // 1MB
    let cache = Arc::new(cache);

    let sst_id = SSTableId(1);
    let offset = 100;

    // Miss
    assert!(cache.get(sst_id, offset).is_none());

    // Insert
    let block = Arc::new(DataBlock::new(vec![1, 2, 3], vec![0]));
    cache.insert(sst_id, offset, Arc::clone(&block));

    // Hit
    let cached = cache.get(sst_id, offset).expect("Should be in cache");
    assert_eq!(cached.data, vec![1, 2, 3]);

    // Different offset -> Miss
    assert!(cache.get(sst_id, 200).is_none());
}
