use serde::{Serialize, de::DeserializeOwned};

pub trait Processable: Clone + Serialize + DeserializeOwned {
    /// the additional size taken up (not including its mem::size_of)
    fn get_size(&self) -> usize;
}

impl Processable for usize {
    fn get_size(&self) -> usize {
        0 // no additional space taken, just its stack size (newtype is free)
    }
}
