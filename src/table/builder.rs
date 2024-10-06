use std::path::Path;
use std::sync::Arc;

use crate::table::Bloom;
use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            let old_block =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

            // push metadata
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from(self.first_key.as_slice()),
                last_key: KeyBytes::from(self.last_key.as_slice()),
            };
            self.meta.push(block_meta);

            let mut block = old_block.build();
            self.data.append(&mut block.data);

            let result = self.builder.add(key, value);
            debug_assert!(result); // block should not be full here

            self.first_key = Vec::new();
            self.last_key = Vec::new();
        }

        if self.first_key.is_empty() {
            self.first_key = Vec::from(key.raw_ref());
        }
        // update last_key
        self.last_key = Vec::from(key.raw_ref());

        // add hash to key hashes
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // write block_meta and blocks
        let mut data = self.data;
        let mut meta = self.meta;

        let mut blk = self.builder.build();
        let block_meta = BlockMeta {
            offset: data.len(),
            first_key: KeyBytes::from(self.first_key.as_slice()),
            last_key: KeyBytes::from(self.last_key.as_slice()),
        };

        meta.push(block_meta);
        data.append(&mut blk.data);

        let offset = data.len();

        BlockMeta::encode_block_meta(&meta, &mut data);

        data.put_u32_le(offset as u32);

        // write bloom filter
        let bloom_offset = data.len();
        let bpk = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let filter = Bloom::build_from_key_hashes(self.key_hashes.as_slice(), bpk);
        filter.encode(&mut data);
        data.put_u64_le(bloom_offset as u64);

        let file_obj = FileObject::create(path.as_ref(), data)?;
        SsTable::open(id, block_cache, file_obj)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
