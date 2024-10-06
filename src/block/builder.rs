use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: Key::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let entry_size = if self.first_key.is_empty() {
            key.len() + value.len() + 3 * 2
        } else {
            let common_len = self
                .first_key
                .raw_ref()
                .iter()
                .zip(key.raw_ref().iter())
                .take_while(|(x, y)| x == y)
                .count();

            key.len() - common_len + value.len() + 3 * 2
        };

        let entry_offset = self.data.len();
        let new_size = entry_size + self.data.len();

        // check if block is not empty and new key exceeds side
        if !self.first_key.is_empty() && new_size > self.block_size {
            return false;
        }

        // set the first key
        if self.first_key.is_empty() {
            self.first_key = Key::from_vec(Vec::from_iter(key.raw_ref().iter().cloned()));

            self.data.extend_from_slice(&0u16.to_le_bytes()[..]);
            self.data
                .extend_from_slice(&(key.len() as u16).to_le_bytes()[..]);
            self.data.extend_from_slice(key.raw_ref());
        } else {
            let common_len = self
                .first_key
                .raw_ref()
                .iter()
                .zip(key.raw_ref().iter())
                .take_while(|(x, y)| x == y)
                .count();
            self.data
                .extend_from_slice(&(common_len as u16).to_le_bytes()[..]);
            self.data
                .extend_from_slice(&((key.len() - common_len) as u16).to_le_bytes()[..]);
            self.data.extend_from_slice(&key.raw_ref()[common_len..]);
        }

        // put value
        self.data
            .extend_from_slice(&(value.len() as u16).to_le_bytes()[..]);
        self.data.extend_from_slice(value);

        // put offset
        self.offsets.push(entry_offset as u16);

        true
    }
    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block::decode(self.data.as_slice())
    }
}
