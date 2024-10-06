use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    is_a_smaller: bool,
    valid: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let is_a_smaller;

        if !a.is_valid() {
            is_a_smaller = false;
        } else if b.is_valid() {
            is_a_smaller = a.key() <= b.key();
        } else {
            is_a_smaller = true;
        }

        let valid = a.is_valid() || b.is_valid();
        Ok(Self {
            a,
            b,
            is_a_smaller,
            valid,
        })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.is_a_smaller {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.is_a_smaller {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn next(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }

        if self.is_a_smaller {
            if self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?;
            }

            self.a.next()?;

            if !self.b.is_valid() && !self.a.is_valid() {
                self.valid = false;
            } else if !self.a.is_valid() {
                self.is_a_smaller = false;
            } else if !self.b.is_valid() {
                self.is_a_smaller = true;
            } else {
                self.is_a_smaller = self.a.key() <= self.b.key();
            }
        } else {
            self.b.next()?;

            if !self.b.is_valid() && !self.a.is_valid() {
                self.valid = false;
            } else if !self.b.is_valid() {
                self.is_a_smaller = true;
            } else if !self.a.is_valid() {
                self.is_a_smaller = false;
            } else {
                self.is_a_smaller = self.a.key() <= self.b.key();
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
