use crate::{Bytes, mst::MstNode};

#[derive(Debug, Clone)]
pub enum MaybeProcessedBlock {
    /// A block that's *probably* a Node (but we can't know yet)
    ///
    /// It *can be* a record that suspiciously looks a lot like a node, so we
    /// cannot eagerly turn it into a Node. We only know for sure what it is
    /// when we actually walk down the MST
    Raw(Bytes),
    /// A processed record from a block that was definitely not a Node
    ///
    /// Processing has to be fallible because the CAR can have totally-unused
    /// blocks, which can just be garbage. since we're eagerly trying to process
    /// record blocks without knowing for sure that they *are* records, we
    /// discard any definitely-not-nodes that fail processing and keep their
    /// error in the buffer for them. if we later try to retreive them as a
    /// record, then we can surface the error.
    ///
    /// If we _never_ needed this block, then we may have wasted a bit of effort
    /// trying to process it. Oh well.
    ///
    /// There's an alternative here, which would be to kick unprocessable blocks
    /// back to Raw, or maybe even a new RawUnprocessable variant. Then we could
    /// surface the typed error later if needed by trying to reprocess.
    Processed(Bytes),
}

impl MaybeProcessedBlock {
    pub fn to_node(&self) -> Option<MstNode> {
        let Self::Raw(bytes) = self else {
            return None;
        };
        serde_ipld_dagcbor::from_slice(bytes).ok()
    }
    pub fn unknown_depth(&self) -> bool {
        let Self::Raw(bytes) = self else {
            return false;
        };
        let Ok(node) = serde_ipld_dagcbor::from_slice::<MstNode>(bytes) else {
            return false;
        };
        node.depth.is_none()
    }
    pub(crate) fn maybe(process: fn(Bytes) -> Bytes, data: Bytes) -> Self {
        if MstNode::could_be(&data) {
            MaybeProcessedBlock::Raw(data)
        } else {
            MaybeProcessedBlock::Processed(process(data))
        }
    }
    pub(crate) fn len(&self) -> usize {
        match self {
            MaybeProcessedBlock::Raw(b) => b.len(),
            MaybeProcessedBlock::Processed(b) => b.len(),
        }
    }
    pub(crate) fn into_bytes(self) -> Bytes {
        match self {
            MaybeProcessedBlock::Raw(mut b) => {
                b.push(0x00);
                b
            }
            MaybeProcessedBlock::Processed(mut b) => {
                b.push(0x01);
                b
            }
        }
    }
    pub(crate) fn from_bytes(mut b: Bytes) -> Self {
        // TODO: make sure bytes is not empty, that it's explicitly 0 or 1, etc
        let suffix = b.pop().unwrap();
        if suffix == 0x00 {
            MaybeProcessedBlock::Raw(b)
        } else {
            MaybeProcessedBlock::Processed(b)
        }
    }
}

/// Processor that just returns the raw blocks
#[inline]
pub fn noop(block: Bytes) -> Bytes {
    block
}
