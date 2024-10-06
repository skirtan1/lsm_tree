use std::sync::Arc;

use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut biterator = Self::new(block);
        biterator.seek_to_first();
        biterator.first_key = biterator.key().to_key_vec();
        biterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut biterator = Self::create_and_seek_to_first(block);
        biterator.seek_to_key(key);
        biterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let curr_end = 2;
        let data = &self.block.data[curr_end..];
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let value_len = u16::from_le_bytes([data[2 + key_len], data[2 + key_len + 1]]) as usize;

        self.key = Key::from_vec(data[2..2 + key_len].to_vec());
        self.idx = 0;
        self.value_range = (curr_end + 4 + key_len, curr_end + 4 + key_len + value_len)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }

        if self.is_valid() && self.value_range.1 == self.block.data.len() {
            self.key = Key::new();
            return;
        }

        //println!("{}, {}", self.value_range.1, self.block.data.len());
        let offset = self.value_range.1;

        let data = &self.block.data[offset..];
        let prefix_length = u16::from_le_bytes([data[0], data[1]]) as usize;

        let curr_end = offset + 2;
        let data = &self.block.data[curr_end..];
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let value_len = u16::from_le_bytes([data[2 + key_len], data[2 + key_len + 1]]) as usize;

        let mut _key = Vec::new();
        _key.extend_from_slice(&self.first_key.raw_ref()[..prefix_length]);
        _key.extend_from_slice(&data[2..2 + key_len]);
        self.key = Key::from_vec(_key);
        self.idx += 1;
        self.value_range = (curr_end + 4 + key_len, curr_end + 4 + key_len + value_len);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && self.key.raw_ref() < key.to_key_vec().raw_ref() {
            self.next();
        }
    }
}
