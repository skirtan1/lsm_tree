#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};
use std::ops::Bound;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    is_valid: bool,
    upper: Bound<Vec<u8>>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        let valid = iter.is_valid();
        let mut ret = Self {
            inner: iter,
            is_valid: valid,
            upper: Bound::Unbounded,
        };
        if valid {
            ret.next_non_delete()?;
        }

        Ok(ret)
    }

    pub(crate) fn create_with_bound(
        iter: LsmIteratorInner,
        _upper: Bound<Vec<u8>>,
    ) -> Result<Self> {
        let mut iter = Self::new(iter)?;

        match _upper {
            Bound::Unbounded => Ok(iter),
            Bound::Included(key) => {
                iter.is_valid = iter.is_valid && iter.key() <= key.as_slice();
                iter.upper = Bound::Included(key);
                Ok(iter)
            }
            Bound::Excluded(key) => {
                iter.is_valid = iter.is_valid && iter.key() < key.as_slice();
                iter.upper = Bound::Excluded(key);
                Ok(iter)
            }
        }
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;

        match &mut self.upper {
            Bound::Unbounded => {
                self.is_valid = self.inner.is_valid();
                Ok(())
            }
            Bound::Included(key) => {
                self.is_valid =
                    self.inner.is_valid() && self.inner.key().raw_ref() <= key.as_slice();
                Ok(())
            }
            Bound::Excluded(key) => {
                self.is_valid =
                    self.inner.is_valid() && self.inner.key().raw_ref() < key.as_slice();
                Ok(())
            }
        }
    }

    fn next_non_delete(&mut self) -> Result<()> {
        while self.is_valid && self.inner.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.next_non_delete()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("next called on errored iterator")
        }

        if self.is_valid() {
            return match self.iter.next() {
                Ok(_) => Ok(()),
                Err(e) => {
                    self.has_errored = true;
                    Err(e)
                }
            };
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
