//! Depth-first MST traversal

use crate::mst::{Depth, MstNode, NodeThing, ThingKind};
use crate::{Bytes, HashMap, Rkey, noop, disk::DiskStore, drive::MaybeProcessedBlock};
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
    #[error("block not found: {0:?}")]
    MissingBlock(NodeThing),
}

/// Errors from invalid Rkeys
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum MstError {
    #[error("Nodes cannot be empty (except for an entirely empty MST)")]
    EmptyNode,
    #[error("Expected node to be at depth {expected}, but it was at {depth}")]
    WrongDepth { depth: Depth, expected: Depth },
    #[error("MST depth underflow: depth-0 node with child trees")]
    DepthUnderflow,
    #[error("Encountered rkey {rkey:?} which cannot follow the previous: {prev:?}")]
    RkeyOutOfOrder { prev: Rkey, rkey: Rkey },
}

/// Walker outputs
#[derive(Debug, PartialEq)]
pub struct Output {
    pub rkey: Rkey,
    pub cid: Cid,
    pub data: Bytes,
}

#[derive(Debug, PartialEq)]
pub enum Step<T = Output> {
    Value(T),
    End(Option<Rkey>),
}

/// Traverser of an atproto MST
///
/// Walks the tree from left-to-right in depth-first order
#[derive(Debug, Clone)]
pub struct Walker {
    prev_rkey: Rkey,
    root_depth: Depth,
    todo: Vec<Vec<NodeThing>>,
}

impl Walker {
    pub fn new(root_node: MstNode) -> Self {
        Self {
            prev_rkey: "".to_string(),
            root_depth: root_node.depth.unwrap_or(0), // empty root node = empty mst
            todo: vec![root_node.things],
        }
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
                Ok(Some(Output { rkey, cid, data }))
            }
            ThingKind::Tree => {
                let MaybeProcessedBlock::Raw(data) = mpb else {
                    return Err(WalkError::BadCommitFingerprint);
                };

                let node: MstNode =
                    serde_ipld_dagcbor::from_slice(data).map_err(WalkError::BadCommit)?;

                if node.is_empty() {
                    return Err(WalkError::MstError(MstError::EmptyNode));
                }

                let current_depth = self.root_depth - (self.todo.len() - 1) as u32;
                let next_depth = current_depth
                    .checked_sub(1)
                    .ok_or(MstError::DepthUnderflow)?;
                if let Some(d) = node.depth
                    && d != next_depth
                {
                    return Err(WalkError::MstError(MstError::WrongDepth {
                        depth: d,
                        expected: next_depth,
                    }));
                }

                log::trace!("node into depth {next_depth}");
                self.todo.push(node.things);
                Ok(None)
            }
        }
    }

    #[inline(always)]
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

    /// Advance through nodes until we find a record or can't go further
    pub fn step(
        &mut self,
        blocks: &mut HashMap<Cid, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Step, WalkError> {
        while let Some(NodeThing { cid, kind }) = self.next_todo() {
            let Some(mpb) = blocks.get(&cid) else {
                return Err(WalkError::MissingBlock(NodeThing { cid, kind }));
            };
            if let Some(out) = self.mpb_step(kind, cid, mpb, &process)? {
                return Ok(Step::Value(out));
            }
        }
        Ok(Step::End(None))
    }

    #[allow(dead_code)]
    pub fn edge(
        &mut self,
        blocks: &mut HashMap<Cid, MaybeProcessedBlock>,
    ) -> Result<Option<Rkey>, WalkError> {
        let mut ant = self.clone();
        let mut rkey_prev: Option<Rkey> = None;
        let dummy_mpb = MaybeProcessedBlock::Raw(vec![].into());

        while let Some(NodeThing { cid, kind }) = ant.next_todo() {
            match kind {
                ThingKind::Tree => {
                    let mpb = blocks
                        .get(&cid)
                        .ok_or_else(|| WalkError::MissingBlock(NodeThing { cid, kind: ThingKind::Tree }))?;
                    assert!(ant.mpb_step(kind, cid, mpb, noop)?.is_none());
                }
                ThingKind::Value { ref rkey } => {
                    if blocks.get(&cid).is_some() {
                        return Ok(rkey_prev);
                    }
                    rkey_prev = Some(rkey.clone());
                    assert!(ant.mpb_step(kind, cid, &dummy_mpb, noop)?.is_some());

                    *self = ant;
                    ant = self.clone();
                }
            };

        }
        Ok(None)
    }

    /// blocking!!!!!!
    pub fn disk_step(
        &mut self,
        blocks: &mut DiskStore,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Step, WalkError> {
        while let Some(NodeThing { cid, kind }) = self.next_todo() {
            let Some(block_slice) = blocks.get(&cid.to_bytes())? else {
                return Err(WalkError::MissingBlock(NodeThing { cid, kind }));
            };
            let mpb = MaybeProcessedBlock::from_bytes(block_slice.to_vec());
            if let Some(out) = self.mpb_step(kind, cid, &mpb, &process)? {
                return Ok(Step::Value(out));
            }
        }
        Ok(Step::End(None))
    }
}
