pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut inner_buf = BytesMut::new();

        let start = buf.len();
        let mut sum_key_len = 0;
        inner_buf.put_u32_le(block_meta.len() as u32);
        for meta in block_meta {
            inner_buf.put_u64_le(meta.offset as u64);

            inner_buf.put_u16_le(meta.first_key.len() as u16);
            inner_buf.extend_from_slice(meta.first_key.raw_ref());
            sum_key_len += meta.first_key.len();

            inner_buf.put_u16_le(meta.last_key.len() as u16);
            inner_buf.extend_from_slice(meta.last_key.raw_ref());

            sum_key_len += meta.last_key.len();
        }

        buf.extend(inner_buf);
        let end = buf.len();

        debug_assert_eq!(
            4 + block_meta.len() * (8 + 2 + 2) + sum_key_len,
            end - start
        );
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;

        let n = buf.get_u32_le() as usize;
        let mut block_meta = Vec::new();
        for _ in 0..n {
            let offset = buf.get_u64_le() as usize;

            let fk_len = buf.get_u16_le() as usize;

            let mut v = Vec::with_capacity(fk_len);
            for _ in 0..fk_len {
                v.push(buf.get_u8());
            }

            let first_key = KeyBytes::from(v.as_slice());

            let lk_len = buf.get_u16_le() as usize;

            let mut v = Vec::with_capacity(lk_len);
            for _ in 0..lk_len {
                v.push(buf.get_u8());
            }

            let last_key = KeyBytes::from(v.as_slice());

            block_meta.push(Self {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // decode bloom filter
        let bloom_offset = file.read(file.size() - 8, 8)?.as_slice().get_u64_le();
        let bloom_filter = Bloom::decode(
            file.read(bloom_offset, file.size() - 8 - bloom_offset)?
                .as_slice(),
        )?;

        // decode meta block
        let meta_block_offset = file.read(bloom_offset - 4, 4)?.as_slice().get_u32_le();
        let meta_block = BlockMeta::decode_block_meta(
            file.read(
                meta_block_offset as u64,
                bloom_offset - 4 - meta_block_offset as u64,
            )?
            .as_slice(),
        );

        let mut table = Self {
            file,
            block_meta: meta_block,
            block_meta_offset: meta_block_offset as usize,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::from("")),
            last_key: KeyBytes::from_bytes(Bytes::from("")),
            bloom: Some(bloom_filter),
            max_ts: 0,
        };

        let block_meta = &table.block_meta;

        table.first_key = block_meta
            .first()
            .expect("should have a block")
            .first_key
            .clone();
        table.last_key = block_meta
            .last()
            .expect("should have a block")
            .last_key
            .clone();

        Ok(table)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start = self.block_meta[block_idx].offset;
        let end = if block_idx == (self.num_of_blocks() - 1) {
            self.block_meta_offset
        } else {
            self.block_meta[block_idx + 1].offset
        };
        let block = Block::decode(
            self.file
                .read(start as u64, (end - start) as u64)?
                .as_slice(),
        );

        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref cache) = self.block_cache {
            let blk = cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let idx = self
            .block_meta
            .partition_point(|curr| curr.first_key.raw_ref() <= key.raw_ref());
        if idx == 0 {
            0
        } else {
            idx - 1
        }
    }

    pub fn find_lk_block_idx(&self, key: KeySlice) -> usize {
        let idx = self
            .block_meta
            .partition_point(|curr| curr.last_key.raw_ref() < key.raw_ref());
        if idx == self.num_of_blocks() {
            idx - 1
        } else {
            idx
        }
    }

    pub fn key_within(&self, _lower: &Bound<&[u8]>, _upper: &Bound<&[u8]>) -> bool {
        match _lower {
            Bound::Unbounded => match _upper {
                Bound::Unbounded => true,
                Bound::Included(upper_key) => self.first_key.raw_ref() <= *upper_key,
                Bound::Excluded(upper_key) => self.first_key.raw_ref() < *upper_key,
            },
            Bound::Included(lower_key) => match _upper {
                Bound::Unbounded => self.last_key.raw_ref() >= *lower_key,
                Bound::Included(upper_key) => {
                    (*lower_key >= self.first_key.raw_ref() 
                        && *lower_key <= self.last_key.raw_ref())
                        || (*upper_key >= self.first_key.raw_ref()
                            && *upper_key <= self.last_key.raw_ref())
                }
                Bound::Excluded(upper_key) => {
                    (*lower_key >= self.first_key.raw_ref() 
                        && *lower_key <= self.last_key.raw_ref())
                        || (*upper_key > self.first_key.raw_ref()
                            && *upper_key < self.last_key.raw_ref())
                }
            },
            Bound::Excluded(lower_key) => match _upper {
                Bound::Unbounded => self.last_key.raw_ref() > *lower_key,
                Bound::Included(upper_key) => {
                    (*lower_key > self.first_key.raw_ref() 
                        && *lower_key < self.last_key.raw_ref())
                        || (*upper_key >= self.first_key.raw_ref()
                            && *upper_key <= self.last_key.raw_ref())
                }
                Bound::Excluded(upper_key) => {
                    (*lower_key > self.first_key.raw_ref() 
                        && *lower_key < self.last_key.raw_ref())
                        || (*upper_key > self.first_key.raw_ref()
                            && *upper_key < self.last_key.raw_ref())
                }
            },
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
