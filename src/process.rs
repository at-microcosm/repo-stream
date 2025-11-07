use serde::{Serialize, de::DeserializeOwned};

pub trait Processable: Clone + Serialize + DeserializeOwned {
    /// the additional size taken up (not including its mem::size_of)
    fn get_size(&self) -> usize;
}

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
