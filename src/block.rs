#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len());
        // put data
        buf.put(self.data.as_slice());
        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut _data = Vec::new();
        let mut _offset = Vec::new();

        let mut offset_counter = 0;

        while offset_counter < data.len() {
            // push offset
            _offset.push(offset_counter as u16);

            // skip common length
            offset_counter += 2;

            // calculate length of key
            let kl = u16::from_le_bytes([data[offset_counter], data[offset_counter + 1]]);
            offset_counter += 2 + kl as usize;

            // calculate length of value
            let vl = u16::from_le_bytes([data[offset_counter], data[offset_counter + 1]]);
            offset_counter += 2 + vl as usize;
        }

        _data.put(data);
        Self {
            data: _data,
            offsets: _offset,
        }
    }
}
