use color_eyre::{
    eyre::{bail, ContextCompat},
    Result,
};

/// A storage for fragments.
#[derive(Debug)]
pub struct Fragments {
    full_message: Vec<u8>,
    filled: Vec<Region>,
}

#[derive(Debug)]
struct Region {
    begin: usize,
    length: usize,
}

impl Fragments {
    /// Creates an empty fragments container suitable to store a buffer of the
    /// given length.
    pub fn new(length: usize) -> Self {
        Self::with_placeholder(length, 0)
    }

    /// Creates an empty fragments container suitable to store a buffer of the
    /// given length, where gaps are willed with the provided placeholder.
    pub fn with_placeholder(length: usize, placeholder: u8) -> Self {
        Self {
            full_message: vec![placeholder; length],
            filled: vec![],
        }
    }

    /// Inserts a fragment at the given offset.
    pub fn insert<D>(&mut self, offset: usize, data: D) -> Result<()>
    where
        D: AsRef<[u8]>,
    {
        let data = data.as_ref();
        let begin = offset;
        let length = data.len();

        self.full_message
            .get_mut(begin..)
            .and_then(|slice| slice.get_mut(..length))
            .wrap_err("Fragment out of range")?
            .copy_from_slice(data);
        match self
            .filled
            .binary_search_by_key(&begin, |region| region.begin)
        {
            Ok(_) => bail!("Fragment already exists"),
            Err(0) => {
                // No previous fragments, no need to merge anything.
                self.filled.insert(0, Region { begin, length });
            }
            Err(position) => {
                // Not the first region => try to merge.
                let mut previous = &mut self.filled[position - 1];
                if previous.begin + previous.length == begin {
                    // Yay, they are adjacent!
                    previous.length += length;
                } else {
                    // There's a gap :(
                    self.filled.insert(position, Region { begin, length });
                }
            }
        };

        Ok(())
    }

    /// Checks if the data is fully received.
    pub fn is_done(&self) -> bool {
        let mut size = 0usize;
        for region in &self.filled {
            if region.begin != size {
                // A gap
                return false;
            }
            size += region.length;
        }

        // No gaps && all the bytes are set
        size == self.full_message.len()
    }

    /// Returns the internal data buffer.
    pub fn consume(self) -> Vec<u8> {
        self.full_message
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use rand::{distributions::Standard, prelude::SliceRandom, Rng, SeedableRng};
    use rand_xoshiro::Xoshiro256Plus;

    #[test]
    fn fill() {
        let mut fragments = Fragments::new(10);
        fragments.insert(5, vec![1; 4]).unwrap();
        assert!(!fragments.is_done());
        fragments.insert(9, vec![2]).unwrap();
        assert!(!fragments.is_done());
        assert_eq!(fragments.filled.len(), 1);

        assert_eq!(fragments.full_message, &[0, 0, 0, 0, 0, 1, 1, 1, 1, 2]);

        fragments.insert(0, vec![3; 5]).unwrap();

        assert!(fragments.is_done());

        assert_eq!(fragments.consume(), &[3, 3, 3, 3, 3, 1, 1, 1, 1, 2]);
    }

    #[test]
    fn empty() {
        let fragments = Fragments::new(0);
        assert!(fragments.is_done());
        assert_eq!(fragments.consume(), []);
    }

    #[test]
    fn shuffled() {
        let mut rand = Xoshiro256Plus::seed_from_u64(1);
        let all_data = (&mut rand)
            .sample_iter(Standard)
            .take(1024 * 1024)
            .collect::<Vec<u8>>();

        let fragments = {
            let mut fragments = vec![];
            let mut all_data = &all_data[..];
            let mut begin = 0;
            while !all_data.is_empty() {
                let length = rand.gen_range(1..=100usize).min(all_data.len());
                let (fragment, remaining) = all_data.split_at(length);
                all_data = remaining;
                fragments.push((fragment, begin));
                begin += length;
            }

            // Shuffle the fragments.
            fragments.shuffle(&mut rand);

            fragments
        };

        let mut reconstruction = Fragments::new(all_data.len());
        for (fragment, begin) in fragments {
            reconstruction.insert(begin, fragment).unwrap();
        }

        assert!(reconstruction.is_done());

        let reconstructed = reconstruction.consume();

        assert_eq!(reconstructed, all_data);
    }
}
