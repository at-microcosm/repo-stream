//! Depth-first MST traversal

use crate::link::{NodeThing, ObjectLink, ThingKind};
use crate::mst::{Depth, MstNode};
use crate::{Bytes, HashMap, Rkey, disk::DiskStore, block::MaybeProcessedBlock, noop};
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
    MissingBlock(Box<NodeThing>),
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
///
/// TODO: rename to "Record" or "Entry" or something
#[derive(Debug, PartialEq)]
pub struct Output<T = Bytes> {
    pub rkey: Rkey, // TODO: aaa it's not really rkey, it's just "key" (or split to collection/rkey??)
    pub cid: Cid,
    pub data: T,
}

#[derive(Debug, PartialEq)]
pub enum Step<T = Output> {
    Value(T),
    End(Option<Rkey>),
}

// #[derive(Debug, PartialEq)]
// pub struct LowStep {
//     pub cid: Cid,
//     pub kind: LowKind,
// }

// #[derive(Debug, PartialEq)]
// pub enum LowKind {
//     Node {
//         children: Option<Vec<NodeThing>>,
//     },
//     Record {
//         key: Rkey,
//         data: Option<Bytes>,
//     },
// }

/// Traverser of an atproto MST
///
/// Walks the tree from left-to-right in depth-first order
#[derive(Debug, Clone)]
pub struct Walker {
    links: usize,
    prev_rkey: Rkey,
    root_depth: Depth,
    todo: Vec<Vec<NodeThing>>,
}

impl Walker {
    pub fn new(root_node: MstNode) -> Self {
        Self {
            links: 0,
            prev_rkey: "".to_string(),
            root_depth: root_node.depth.unwrap_or(0), // empty root node = empty mst
            todo: vec![root_node.things],
        }
    }

    fn mpb_step(
        &mut self,
        thing: NodeThing,
        mpb: &MaybeProcessedBlock,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<Output>, WalkError> {
        match thing.kind {
            ThingKind::Record(rkey) => {
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
                    cid: thing.link.into(),
                    data,
                }))
            }
            ThingKind::ChildNode => {
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

                let n = node.things.len();
                log::trace!("node into depth {next_depth} with {n} links");
                self.todo.push(node.things);
                self.links += n;
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
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Step, WalkError> {
        while let Some(NodeThing { link, kind }) = self.next_todo() {
            let Some(mpb) = blocks.get(&link) else {
                return Err(WalkError::MissingBlock(NodeThing { link, kind }.into()));
            };
            if let Some(out) = self.mpb_step(NodeThing { link, kind }, mpb, &process)? {
                return Ok(Step::Value(out));
            }
        }
        log::debug!("total links: {}", self.links);
        Ok(Step::End(None))
    }

    // /// Emit every step including MST nodes
    // pub fn step_low(
    //     &mut self,
    //     blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    //     process: impl Fn(Bytes) -> Bytes,
    // ) -> Result<Option<LowStep>, WalkError> {
    //     let Some(NodeThing { link, kind }) = self.next_todo() else {
    //         return Ok(None);
    //     };
    //     let Some(mpb) = blocks.get(&link) else {

    //     }
    // }

    /// Advance through nodes, allowing for missing records
    pub fn step_sparse(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Step<Output<Option<Bytes>>>, WalkError> {
        while let Some(NodeThing { link, kind }) = self.next_todo() {
            let mut dummy = false;
            let mpb = match blocks.get(&link) {
                Some(mpb) => mpb,
                None => {
                    if let ThingKind::Record(_) = kind {
                        dummy = true;
                        &MaybeProcessedBlock::Processed(vec![])
                    } else {
                        continue;
                    }
                }
            };
            if let Some(out) = self.mpb_step(NodeThing { link, kind }, mpb, |bytes| {
                if dummy { bytes } else { process(bytes) }
            })? {
                // eprintln!(" ----- {}", out.rkey);
                return Ok(Step::Value(Output {
                    cid: out.cid,
                    rkey: out.rkey,
                    data: if dummy { None } else { Some(out.data) },
                }));
            }
        }
        Ok(Step::End(None))
    }

    pub fn step_to_edge(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<Option<Rkey>, WalkError> {
        let mut ant = self.clone();
        let mut rkey_prev = None;
        loop {
            match ant.step(blocks, noop) {
                Err(WalkError::MissingBlock(thing)) => {
                    if let ThingKind::Record(rkey) = thing.kind {
                        rkey_prev = Some(rkey);
                    }
                    *self = ant;
                    ant = self.clone();
                }
                Err(anyother) => return Err(anyother),
                Ok(z) => {
                    log::info!("apparently we are too far at {z:?}");
                    return Ok(rkey_prev); // oop real record, mutant went too far
                }
            }
        }
    }

    /// Skip forward to the first record at or after `target`, without emitting anything.
    ///
    /// Uses the tree structure to skip entire subtrees that are provably before `target`,
    /// only loading child nodes on the path to `target`. O(depth × branching_factor).
    ///
    /// After this returns `Ok(())`, the next call to `step` will yield the first record
    /// at or after `target`, or `Step::End` if no such record exists.
    pub fn seek(
        &mut self,
        target: &str,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<(), WalkError> {
        // Classify what to do next without holding a borrow through the action
        enum SeekStep {
            Done,
            EmptyLevel,
            SkipRecord(Rkey),
            SkipSubtree,
            Descend,
        }

        loop {
            let next = match self.todo.last() {
                None => return Ok(()),
                Some(level) => {
                    let n = level.len();
                    if n == 0 {
                        SeekStep::EmptyLevel
                    } else {
                        match &level[n - 1].kind {
                            ThingKind::Record(k) if k.as_str() >= target => SeekStep::Done,
                            ThingKind::Record(k) => SeekStep::SkipRecord(k.clone()),
                            ThingKind::ChildNode => {
                                // The right-bounding record for this child node is at n-2.
                                // All keys in this subtree are < right_bound, so we can skip
                                // the whole subtree if right_bound <= target.
                                let can_skip = n >= 2
                                    && matches!(
                                        &level[n - 2].kind,
                                        ThingKind::Record(k) if k.as_str() <= target
                                    );
                                if can_skip {
                                    SeekStep::SkipSubtree
                                } else {
                                    SeekStep::Descend
                                }
                            }
                        }
                    }
                }
            }; // borrow of self.todo released here

            match next {
                SeekStep::Done => return Ok(()),
                SeekStep::EmptyLevel => {
                    self.todo.pop();
                }
                SeekStep::SkipRecord(key) => {
                    self.todo.last_mut().unwrap().pop();
                    self.prev_rkey = key;
                }
                SeekStep::SkipSubtree => {
                    self.todo.last_mut().unwrap().pop();
                }
                SeekStep::Descend => {
                    let child = self.todo.last_mut().unwrap().pop().unwrap();
                    // Note: self.todo borrow released before push below

                    let Some(mpb) = blocks.get(&child.link) else {
                        return Err(WalkError::MissingBlock(child.into()));
                    };
                    let MaybeProcessedBlock::Raw(data) = mpb else {
                        return Err(WalkError::BadCommitFingerprint);
                    };
                    let node: MstNode =
                        serde_ipld_dagcbor::from_slice(data).map_err(WalkError::BadCommit)?;
                    if node.is_empty() {
                        return Err(WalkError::MstError(MstError::EmptyNode));
                    }
                    // Depth validation mirrors mpb_step: todo still has the (possibly empty)
                    // parent level, so todo.len()-1 is the parent's depth delta from root.
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
                    self.links += node.things.len();
                    self.todo.push(node.things);
                }
            }
        }
    }

    /// blocking!!!!!!
    pub fn disk_step(
        &mut self,
        blocks: &DiskStore,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Step, WalkError> {
        while let Some(NodeThing { link, kind }) = self.next_todo() {
            let Some(block_slice) = blocks.get(&link.to_bytes())? else {
                return Err(WalkError::MissingBlock(NodeThing { link, kind }.into()));
            };
            let mpb = MaybeProcessedBlock::from_bytes(block_slice.to_vec());
            if let Some(out) = self.mpb_step(NodeThing { link, kind }, &mpb, &process)? {
                return Ok(Step::Value(out));
            }
        }
        Ok(Step::End(None))
    }
}
