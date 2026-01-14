//! Depth-first MST traversal

use crate::mst::NodeThing;
use crate::mst::ThingKind;
use crate::mst::MstNode;
use crate::mst::Depth;
use crate::Bytes;
use crate::HashMap;
use crate::disk::DiskStore;
use crate::drive::MaybeProcessedBlock;
use cid::Cid;
use std::convert::Infallible;

/// Errors that can happen while walking
#[derive(Debug, thiserror::Error)]
pub enum WalkError {
    #[error("Failed to fingerprint commit block")]
    BadCommitFingerprint,
    #[error("Failed to decode commit block: {0}")]
    BadCommit(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("Action node error: {0}")]
    MstError(#[from] MstError),
    #[error("storage error: {0}")]
    StorageError(#[from] fjall::Error),
    #[error("block not found: {0}")]
    MissingBlock(Cid),
}

/// Errors from invalid Rkeys
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum MstError {
    #[error("RKey was not utf-8")]
    EntryRkeyNotUtf8(#[from] std::string::FromUtf8Error),
    #[error("Nodes cannot be empty (except for an entirely empty MST)")]
    EmptyNode,
    #[error("Expected node to be at depth {expected}, but it was at {depth}")]
    WrongDepth { depth: Depth, expected: Depth },
    #[error("MST depth underflow: depth-0 node with child trees")]
    DepthUnderflow,
    #[error("Encountered rkey {rkey:?} which cannot follow the previous: {prev:?}")]
    RkeyOutOfOrder { prev: String, rkey: String },
}

/// Walker outputs
#[derive(Debug, PartialEq)]
pub struct Output {
    pub rkey: String,
    pub cid: Cid,
    pub data: Bytes,
}

/// Traverser of an atproto MST
///
/// Walks the tree from left-to-right in depth-first order
#[derive(Debug)]
pub struct Walker {
    prev_rkey: String,
    root_depth: Depth,
    todo: Vec<Vec<NodeThing>>,
}

impl Walker {
    pub fn new(
        root_node: MstNode,
    ) -> Option<Self> {
        Some(Self {
            prev_rkey: "".to_string(),
            root_depth: root_node.depth?,
            todo: vec![root_node.things],
        })
    }

    fn next_todo(&mut self) -> Option<NodeThing> {
        while let Some(last) = self.todo.last_mut() {
            let Some(thing) = last.pop() else {
                self.todo.pop();
                continue;
            };
            return Some(thing);
        }
        None
    }

    fn mpb_step(
        &mut self,
        kind: ThingKind,
        cid: Cid,
        mpb: &MaybeProcessedBlock,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<Output>, WalkError> {
        match kind {
            ThingKind::Value { rkey } => {
                let data = match mpb {
                    MaybeProcessedBlock::Raw(data) => process(data.clone()),
                    MaybeProcessedBlock::Processed(t) => t.clone(),
                };

                if rkey <= self.prev_rkey {
                    return Err(WalkError::MstError(MstError::RkeyOutOfOrder {
                        rkey,
                        prev: self.prev_rkey.clone(),
                    }));
                }
                self.prev_rkey = rkey.clone();

                log::trace!("val @ {rkey}");
                Ok(Some(Output {
                    rkey,
                    cid,
                    data,
                }))
            }
            ThingKind::Tree => {
                let MaybeProcessedBlock::Raw(data) = mpb else {
                    return Err(WalkError::BadCommitFingerprint);
                };

                let node: MstNode = serde_ipld_dagcbor::from_slice(&data)
                    .map_err(WalkError::BadCommit)?;

                if node.is_empty() {
                    return Err(WalkError::MstError(MstError::EmptyNode));
                }

                let current_depth = self.root_depth - (self.todo.len() - 1) as u32;
                let next_depth = current_depth.checked_sub(1).ok_or(MstError::DepthUnderflow)?;
                if let Some(d) = node.depth {
                    if d != next_depth {
                        return Err(WalkError::MstError(MstError::WrongDepth {
                            depth: d,
                            expected: next_depth,
                        }));
                    }
                }

                log::trace!("node into depth {next_depth}");
                self.todo.push(node.things);
                Ok(None)
            }
        }
    }

    /// Advance through nodes until we find a record or can't go further
    pub fn step(
        &mut self,
        blocks: &mut HashMap<Cid, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<Output>, WalkError> {

        while let Some(NodeThing { cid, kind }) = self.next_todo() {
            let Some(mpb) = blocks.get(&cid) else {
                return Err(WalkError::MissingBlock(cid));
            };

            if let Some(out) = self.mpb_step(kind, cid, mpb, &process)? {
                return Ok(Some(out));
            }
        }
        Ok(None)
    }

    /// blocking!!!!!!
    pub fn disk_step(
        &mut self,
        blocks: &mut DiskStore,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<Output>, WalkError> {
        while let Some(NodeThing { cid, kind }) = self.next_todo() {
            let Some(block_slice) = blocks.get(&cid.to_bytes())? else {
                return Err(WalkError::MissingBlock(cid));
            };
            let mpb = MaybeProcessedBlock::from_bytes(block_slice.to_vec());
            if let Some(out) = self.mpb_step(kind, cid, &mpb, &process)? {
                return Ok(Some(out));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // fn cid1() -> Cid {
    //     "bafyreihixenvk3ahqbytas4hk4a26w43bh6eo3w6usjqtxkpzsvi655a3m"
    //         .parse()
    //         .unwrap()
    // }

    // #[test]
    // fn test_push_empty_fails() {
    //     let empty_node = Node {
    //         left: None,
    //         entries: vec![],
    //     };
    //     let mut stack = vec![];
    //     let err = push_from_node(&mut stack, &empty_node, Depth::Depth(4));
    //     assert_eq!(err, Err(MstError::EmptyNode));
    // }

    // #[test]
    // fn test_push_one_node() {
    //     let node = Node {
    //         left: Some(cid1()),
    //         entries: vec![],
    //     };
    //     let mut stack = vec![];
    //     push_from_node(&mut stack, &node, Depth::Depth(4)).unwrap();
    //     assert_eq!(
    //         stack.last(),
    //         Some(Need::Node {
    //             depth: Depth::Depth(3),
    //             cid: cid1()
    //         })
    //         .as_ref()
    //     );
    // }
}
