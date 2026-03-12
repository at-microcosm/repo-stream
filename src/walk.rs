//! Depth-first MST traversal

use crate::mst::{Layer, MstNode, NodeThing, ObjectLink, ThingKind};
use crate::{Bytes, HashMap, RepoPath};
use cid::Cid;
use std::convert::Infallible;

// ---------------------------------------------------------------------------
// Block representation (formerly block.rs)
// ---------------------------------------------------------------------------

/// A block that may or may not have been passed through the user's processor.
///
/// `Raw` means we haven't processed it yet (it could still be an MST node).
/// `Processed` means it's definitely a record and the processor has already run.
#[derive(Debug, Clone)]
pub enum MaybeProcessedBlock {
    Raw(Bytes),
    Processed(Bytes),
}

impl MaybeProcessedBlock {
    /// Apply `process` to `data` unless the block looks like an MST node.
    pub fn maybe(process: fn(Bytes) -> Bytes, data: Bytes) -> Self {
        if MstNode::could_be(&data) {
            MaybeProcessedBlock::Raw(data)
        } else {
            MaybeProcessedBlock::Processed(process(data))
        }
    }

    pub fn from_bytes(data: Bytes) -> Self {
        if MstNode::could_be(&data) {
            MaybeProcessedBlock::Raw(data)
        } else {
            MaybeProcessedBlock::Processed(data)
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            MaybeProcessedBlock::Raw(b) | MaybeProcessedBlock::Processed(b) => b.len(),
        }
    }

    pub fn into_bytes(self) -> Bytes {
        match self {
            MaybeProcessedBlock::Raw(b) | MaybeProcessedBlock::Processed(b) => b,
        }
    }
}

/// Identity block processor — returns the block unchanged.
pub fn noop(block: Bytes) -> Bytes {
    block
}

// ---------------------------------------------------------------------------
// Walker errors
// ---------------------------------------------------------------------------

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
    /// Returned by `next_strict`/`next_chunk_strict` when a record block is absent.
    #[error("record block absent: key={key:?} cid={cid}")]
    MissingBlock {
        key: crate::RepoPath,
        cid: Box<cid::Cid>,
    },
    /// Returned by `next_strict`/`next_chunk_strict` when an MST node block is absent.
    #[error("MST node block absent: cid={cid}")]
    MissingNode { cid: Box<cid::Cid> },
}

/// Errors from invalid repo path keys
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum MstError {
    #[error("Nodes cannot be empty (except for an entirely empty MST)")]
    EmptyNode,
    #[error("Expected node to be at layer {expected}, but it was at {layer}")]
    WrongLayer { layer: Layer, expected: Layer },
    #[error("MST layer underflow: layer-0 node with child trees")]
    LayerUnderflow,
    #[error("Encountered key {key:?} which cannot follow the previous: {prev:?}")]
    KeyOutOfOrder { prev: RepoPath, key: RepoPath },
}

// ---------------------------------------------------------------------------
// Walker output types
// ---------------------------------------------------------------------------

/// An item yielded by `Walker::step`.
#[derive(Debug, PartialEq)]
pub enum WalkItem {
    /// A raw MST node block (root first, then each child as it is descended into).
    Node { cid: Cid, data: Bytes },
    /// A record with its (processed) block data.
    Record(Output),
    /// A record whose block was absent from the loaded blocks.
    MissingRecord { key: RepoPath, cid: Cid },
    /// A child subtree whose root block was absent; its key range is unknown.
    MissingSubtree { cid: Cid },
}

/// A single record emitted by the walker.
#[derive(Debug, PartialEq)]
pub struct Output<T = Bytes> {
    pub key: RepoPath,
    pub cid: Cid,
    pub data: T,
}

/// Walker: traverser of an atproto MST
///
/// Walks the tree left-to-right in depth-first order (is also lexicographic order)
#[derive(Debug, Clone)]
pub struct Walker {
    pub(crate) prev_key: Option<RepoPath>,
    pub(crate) root_layer: Layer,
    pub(crate) todo: Vec<Vec<NodeThing>>,
    /// The root MST node block, emitted as the first `WalkItem::Node` before any records.
    pending_root: Option<(Cid, Bytes)>,
}

impl Walker {
    pub fn new(root_node: MstNode, root_cid: Cid, root_bytes: Bytes) -> Self {
        Self {
            prev_key: None,
            root_layer: root_node.layer.unwrap_or(0), // empty root node = empty mst
            todo: vec![root_node.things],
            pending_root: Some((root_cid, root_bytes)),
        }
    }

    pub(crate) fn mpb_step(
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

                if Some(&key) <= self.prev_key.as_ref() {
                    return Err(WalkError::MstError(MstError::KeyOutOfOrder {
                        key,
                        prev: self.prev_key.clone().unwrap_or("[no prev key]".to_string()),
                    }));
                }
                self.prev_key = Some(key.clone());

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

                let current_layer = self.root_layer - (self.todo.len() - 1) as u32;
                let next_layer = current_layer
                    .checked_sub(1)
                    .ok_or(MstError::LayerUnderflow)?;
                if let Some(d) = node.layer
                    && d != next_layer
                {
                    return Err(WalkError::MstError(MstError::WrongLayer {
                        layer: d,
                        expected: next_layer,
                    }));
                }

                self.todo.push(node.things);
                Ok(None)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn next_todo(&mut self) -> Option<NodeThing> {
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
    /// Only errors on structural MST violations (wrong layer, out-of-order keys).
    /// Advance one step through the MST.
    ///
    /// Returns `Ok(Some(item))` for each block encountered (record, missing
    /// record, or missing subtree), or `Ok(None)` when traversal is complete.
    /// Only errors on structural MST violations (wrong layer, out-of-order keys).
    ///
    /// MST node blocks are **not** emitted; use [`step_with_nodes`] for that.
    pub fn step(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<WalkItem>, WalkError> {
        while let Some(thing) = self.next_todo() {
            let Some(mpb) = blocks.get(&thing.link) else {
                return Ok(Some(match thing.kind {
                    ThingKind::Record(key) => WalkItem::MissingRecord {
                        key,
                        cid: thing.link.into(),
                    },
                    ThingKind::ChildNode => WalkItem::MissingSubtree {
                        cid: thing.link.into(),
                    },
                }));
            };
            if let Some(out) = self.mpb_step(thing, mpb, &process)? {
                return Ok(Some(WalkItem::Record(out)));
            }
        }
        Ok(None)
    }

    /// Like [`step`], but also emits `WalkItem::Node` for every MST node block
    /// that is descended into (root first, then children in traversal order).
    ///
    /// Node bytes are cloned from the in-memory block map on each descent, so
    /// this is measurably more expensive than [`step`] for large trees.
    pub fn step_with_nodes(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        process: impl Fn(Bytes) -> Bytes,
    ) -> Result<Option<WalkItem>, WalkError> {
        // Emit the root MST node block before any records.
        if let Some((cid, data)) = self.pending_root.take() {
            return Ok(Some(WalkItem::Node { cid, data }));
        }

        while let Some(thing) = self.next_todo() {
            let Some(mpb) = blocks.get(&thing.link) else {
                return Ok(Some(match thing.kind {
                    ThingKind::Record(key) => WalkItem::MissingRecord {
                        key,
                        cid: thing.link.into(),
                    },
                    ThingKind::ChildNode => WalkItem::MissingSubtree {
                        cid: thing.link.into(),
                    },
                }));
            };

            // Capture what we need to emit a Node item after mpb_step consumes `thing`.
            let child_link = if matches!(thing.kind, ThingKind::ChildNode) {
                Some(thing.link)
            } else {
                None
            };

            if let Some(out) = self.mpb_step(thing, mpb, &process)? {
                return Ok(Some(WalkItem::Record(out)));
            }

            // mpb_step returns None only for ChildNode descent; emit the node block.
            // This clones the raw bytes — the main cost of step_with_nodes vs step.
            if let Some(link) = child_link {
                let MaybeProcessedBlock::Raw(data) = mpb else {
                    unreachable!("mpb_step already errored on Processed ChildNode");
                };
                return Ok(Some(WalkItem::Node {
                    cid: link.into(),
                    data: data.clone(),
                }));
            }
        }
        Ok(None)
    }

    /// Like [`step`], but skips record block lookups entirely.
    ///
    /// Returns the key and CID of each record directly from the MST node entries.
    /// MST node blocks are still fetched to traverse the tree structure.
    ///
    /// If a child MST node block is absent, the subtree is silently skipped.
    /// Use [`step_keys_strict`] to error instead.
    pub fn step_keys(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<Option<(RepoPath, Cid)>, WalkError> {
        self.step_keys_impl(blocks, false)
    }

    /// Like [`step_keys`], but returns `Err(WalkError::MissingNode)` if a child
    /// MST node block is absent rather than silently skipping the subtree.
    pub fn step_keys_strict(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<Option<(RepoPath, Cid)>, WalkError> {
        self.step_keys_impl(blocks, true)
    }

    fn step_keys_impl(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        strict: bool,
    ) -> Result<Option<(RepoPath, Cid)>, WalkError> {
        while let Some(NodeThing { link, kind }) = self.next_todo() {
            match kind {
                ThingKind::Record(key) => {
                    if Some(&key) <= self.prev_key.as_ref() {
                        return Err(WalkError::MstError(MstError::KeyOutOfOrder {
                            key,
                            prev: self.prev_key.clone().unwrap_or("[no prev key]".to_string()),
                        }));
                    }
                    self.prev_key = Some(key.clone());
                    return Ok(Some((key, link.into())));
                }
                ThingKind::ChildNode => {
                    let Some(mpb) = blocks.get(&link) else {
                        if strict {
                            return Err(WalkError::MissingNode {
                                cid: Box::new(link.into()),
                            });
                        } else {
                            continue;
                        }
                    };
                    let MaybeProcessedBlock::Raw(data) = mpb else {
                        return Err(WalkError::BadCommitFingerprint);
                    };
                    let node: MstNode =
                        serde_ipld_dagcbor::from_slice(data).map_err(WalkError::BadCommit)?;
                    if node.is_empty() {
                        return Err(WalkError::MstError(MstError::EmptyNode));
                    }
                    let current_layer = self.root_layer - (self.todo.len() - 1) as u32;
                    let next_layer = current_layer
                        .checked_sub(1)
                        .ok_or(MstError::LayerUnderflow)?;
                    if let Some(d) = node.layer
                        && d != next_layer
                    {
                        return Err(WalkError::MstError(MstError::WrongLayer {
                            layer: d,
                            expected: next_layer,
                        }));
                    }
                    self.todo.push(node.things);
                }
            }
        }
        Ok(None)
    }

    /// Skip forward to the first record at or after `target`, without emitting anything.
    ///
    /// Uses the tree structure to skip entire subtrees that are provably before `target`,
    /// only loading child nodes on the path to `target`. O(layer × branching_factor).
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
                    self.prev_key = Some(key);
                }
                SeekStep::SkipSubtree => {
                    self.todo.last_mut().unwrap().pop();
                }
                SeekStep::Descend => {
                    let child = self.todo.last_mut().unwrap().pop().unwrap();

                    let Some(mpb) = blocks.get(&child.link) else {
                        // Missing subtree on the seek path; skip it and continue
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
                    let current_layer = self.root_layer - (self.todo.len() - 1) as u32;
                    let next_layer = current_layer
                        .checked_sub(1)
                        .ok_or(MstError::LayerUnderflow)?;
                    if let Some(d) = node.layer
                        && d != next_layer
                    {
                        return Err(WalkError::MstError(MstError::WrongLayer {
                            layer: d,
                            expected: next_layer,
                        }));
                    }
                    self.todo.push(node.things);
                }
            }
        }
    }
}
