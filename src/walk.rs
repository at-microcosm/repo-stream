//! Depth-first MST traversal

use crate::link::{NodeThing, ObjectLink, ThingKind};
use crate::mst::{Depth, MstNode};
use crate::{Bytes, HashMap, RepoPath, disk::DiskStore, block::MaybeProcessedBlock, noop};
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
}

/// Errors from invalid repo path keys
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum MstError {
    #[error("Nodes cannot be empty (except for an entirely empty MST)")]
    EmptyNode,
    #[error("Expected node to be at depth {expected}, but it was at {depth}")]
    WrongDepth { depth: Depth, expected: Depth },
    #[error("MST depth underflow: depth-0 node with child trees")]
    DepthUnderflow,
    #[error("Encountered key {key:?} which cannot follow the previous: {prev:?}")]
    KeyOutOfOrder { prev: RepoPath, key: RepoPath },
}

/// An item yielded by `Walker::step`.
#[derive(Debug, PartialEq)]
pub enum WalkItem {
    /// A record with its (processed) block data.
    Record(Output),
    /// A record whose block was absent from the loaded blocks.
    MissingRecord { key: RepoPath, cid: Cid },
    /// A child subtree whose root block was absent; its key range is unknown.
    MissingSubtree { cid: Cid },
}

/// Walker outputs
#[derive(Debug, PartialEq)]
pub struct Output<T = Bytes> {
    pub key: RepoPath,
    pub cid: Cid,
    pub data: T,
}

#[derive(Debug, PartialEq)]
pub enum Step<T = Output> {
    Value(T),
    End(Option<RepoPath>),
}

/// Traverser of an atproto MST
///
/// Walks the tree from left-to-right in depth-first order
#[derive(Debug, Clone)]
pub struct Walker {
    links: usize,
    prev_key: RepoPath,
    root_depth: Depth,
    todo: Vec<Vec<NodeThing>>,
}

impl Walker {
    pub fn new(root_node: MstNode) -> Self {
        Self {
            links: 0,
            prev_key: "".to_string(),
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
            ThingKind::Record(key) => {
                let data = match mpb {
                    MaybeProcessedBlock::Raw(data) => process(data.clone()),
                    MaybeProcessedBlock::Processed(t) => t.clone(),
                };

                if key <= self.prev_key {
                    return Err(WalkError::MstError(MstError::KeyOutOfOrder {
                        key,
                        prev: self.prev_key.clone(),
                    }));
                }
                self.prev_key = key.clone();

                log::trace!("val @ {key}");
                Ok(Some(Output {
                    key,
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

    /// Advance one step through the MST.
    ///
    /// Returns `Ok(Some(item))` for each block encountered (record, missing
    /// record, or missing subtree), or `Ok(None)` when traversal is complete.
    /// Only errors on structural MST violations (wrong depth, out-of-order keys).
    pub fn step(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<WalkItem>, WalkError> {
        while let Some(thing) = self.next_todo() {
            let Some(mpb) = blocks.get(&thing.link) else {
                return Ok(Some(match thing.kind {
                    ThingKind::Record(key) => {
                        WalkItem::MissingRecord { key, cid: thing.link.into() }
                    }
                    ThingKind::ChildNode => WalkItem::MissingSubtree { cid: thing.link.into() },
                }));
            };
            if let Some(out) = self.mpb_step(thing, mpb, &process)? {
                return Ok(Some(WalkItem::Record(out)));
            }
        }
        log::debug!("total links: {}", self.links);
        Ok(None)
    }

    /// Advance past leading missing blocks to find the first present record.
    ///
    /// Returns the key of the last missing *record* encountered before the
    /// first present record — i.e., the `prev_key` for a CAR slice's leading
    /// edge. After this returns, the next `step` call yields the first present
    /// record (or `None` if the whole tree is absent).
    pub fn step_to_edge(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<Option<RepoPath>, WalkError> {
        let mut ant = self.clone();
        let mut prev_key = None;
        loop {
            match ant.step(blocks, noop)? {
                Some(WalkItem::Record(_)) => {
                    // ant went one step too far; self holds the leading-edge position
                    return Ok(prev_key);
                }
                Some(WalkItem::MissingRecord { key, .. }) => {
                    prev_key = Some(key);
                    *self = ant;
                    ant = self.clone();
                }
                Some(WalkItem::MissingSubtree { .. }) => {
                    *self = ant;
                    ant = self.clone();
                }
                None => return Ok(prev_key),
            }
        }
    }

    /// Skip forward to the first record at or after `target`, without emitting anything.
    ///
    /// Uses the tree structure to skip entire subtrees that are provably before `target`,
    /// only loading child nodes on the path to `target`. O(depth × branching_factor).
    ///
    /// After this returns `Ok(())`, the next call to `step` will yield the first record
    /// at or after `target`, or `None` if no such record exists.
    pub fn seek(
        &mut self,
        target: &str,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<(), WalkError> {
        // Classify what to do next without holding a borrow through the action
        enum SeekStep {
            Done,
            EmptyLevel,
            SkipRecord(RepoPath),
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
                    self.prev_key = key;
                }
                SeekStep::SkipSubtree => {
                    self.todo.last_mut().unwrap().pop();
                }
                SeekStep::Descend => {
                    let child = self.todo.last_mut().unwrap().pop().unwrap();
                    // Note: self.todo borrow released before push below

                    let Some(mpb) = blocks.get(&child.link) else {
                        // Missing subtree on the seek path; skip it and continue
                        // (seek is best-effort for sparse trees)
                        continue;
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

    /// blocking!!!!!
    pub fn disk_step(
        &mut self,
        blocks: &DiskStore,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<WalkItem>, WalkError> {
        while let Some(thing) = self.next_todo() {
            let Some(block_slice) = blocks.get(&thing.link.to_bytes())? else {
                return Ok(Some(match thing.kind {
                    ThingKind::Record(key) => {
                        WalkItem::MissingRecord { key, cid: thing.link.into() }
                    }
                    ThingKind::ChildNode => WalkItem::MissingSubtree { cid: thing.link.into() },
                }));
            };
            let mpb = MaybeProcessedBlock::from_bytes(block_slice.to_vec());
            if let Some(out) = self.mpb_step(thing, &mpb, &process)? {
                return Ok(Some(WalkItem::Record(out)));
            }
        }
        Ok(None)
    }
}
