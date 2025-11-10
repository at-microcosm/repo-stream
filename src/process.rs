/*!
Record processor function output trait

The return type must satisfy the `Processable` trait, which requires:

- `Clone` because two rkeys can refer to the same record by CID, which may
  only appear once in the CAR file.
- `Serialize + DeserializeOwned` so it can be spilled to disk.

One required function must be implemented, `get_size()`: this should return the
approximate total off-stack size of the type. (the on-stack size will be added
automatically via `std::mem::get_size`).

Here's a silly processing function that just collects 'eyy's found in the raw
record bytes

```
# use repo_stream::Processable;
# use serde::{Serialize, Deserialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Eyy(usize, String);

impl Processable for Eyy {
    fn get_size(&self) -> usize {
        // don't need to compute the usize, it's on the stack
        self.1.capacity() // in-mem size from the string's capacity, in bytes
    }
}

fn process(raw: Vec<u8>) -> Vec<Eyy> {
    let mut out = Vec::new();
    let to_find = "eyy".as_bytes();
    for i in 0..(raw.len() - 3) {
        if &raw[i..(i+3)] == to_find {
            out.push(Eyy(i, "eyy".to_string()));
        }
    }
    out
}
```

The memory sizing stuff is a little sketch but probably at least approximately
works.
*/

use serde::{Serialize, de::DeserializeOwned};

/// Output trait for record processing
pub trait Processable: Clone + Serialize + DeserializeOwned {
    /// Any additional in-memory size taken by the processed type
    ///
    /// Do not include stack size (`std::mem::size_of`)
    fn get_size(&self) -> usize;
}

/// Processor that just returns the raw blocks
#[inline]
pub fn noop(block: Vec<u8>) -> Vec<u8> {
    block
}

impl Processable for u8 {
    fn get_size(&self) -> usize {
        0
    }
}

impl Processable for usize {
    fn get_size(&self) -> usize {
        0 // no additional space taken, just its stack size (newtype is free)
    }
}

impl<Item: Sized + Processable> Processable for Vec<Item> {
    fn get_size(&self) -> usize {
        let slot_size = std::mem::size_of::<Item>();
        let direct_size = slot_size * self.capacity();
        let items_referenced_size: usize = self.iter().map(|item| item.get_size()).sum();
        direct_size + items_referenced_size
    }
}
