//! Depth-first MST traversal

use crate::link::{NodeThing, ObjectLink, ThingKind};
use crate::mst::{Depth, MstNode};
use crate::{Bytes, HashMap, Rkey, disk::DiskStore, drive::MaybeProcessedBlock, noop};
use std::collections::BTreeMap;
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
            todo: vec![root_node.things.into_iter().filter(|t| !t.is_record()).collect()],
        }
    }

    pub fn count_entries(
        &mut self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
    ) -> Result<BTreeMap<usize, usize>, WalkError> {
        let mut counts = BTreeMap::new();

        while let Some(NodeThing { link, kind }) = self.next_todo() {
            let Some(mpb) = blocks.get(&link) else {
                return Err(WalkError::MissingBlock(NodeThing { link, kind }.into()));
            };
            match kind {
                ThingKind::Record(_) => unreachable!(),
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

                    let mut entries = 0;
                    let mut links = Vec::new();
                    for thing in node.things {
                        if thing.is_record() {
                            entries += 1;
                        } else {
                            links.push(thing);
                        }
                    }
                    self.todo.push(links);
                    if entries > 0 {
                        *counts.entry(entries).or_default() += 1;
                        if entries > 10_000 {
                            eprintln!("whoa, found a {}-entry node", entries);
                        }
                    }
                }
            }
        }

        Ok(counts)
    }

    pub fn viz(
        &self,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        root_link: ObjectLink,
    ) -> Result<(), WalkError> {
        let root_block = blocks.get(&root_link).ok_or(WalkError::MissingBlock(
            NodeThing {
                link: root_link.clone(),
                kind: ThingKind::ChildNode,
            }
            .into(),
        ))?;

        let root_node: MstNode = match root_block {
            MaybeProcessedBlock::Processed(_) => return Err(WalkError::BadCommitFingerprint),
            MaybeProcessedBlock::Raw(bytes) => serde_ipld_dagcbor::from_slice(bytes)?,
        };

        let mut positions = HashMap::new();
        let mut w = Walker::new(root_node.clone());

        let mut pos_idx = 0;
        while let Step::Value(Output { rkey, .. }) = w.step_sparse(blocks, noop)? {
            positions.insert(rkey, pos_idx);
            pos_idx += 1;
        }

        Self::vnext(
            root_node.depth.unwrap(),
            vec![root_link],
            blocks,
            &positions,
        )?;

        Ok(())
    }

    pub fn vnext(
        level: u32,
        links: Vec<ObjectLink>,
        blocks: &HashMap<ObjectLink, MaybeProcessedBlock>,
        positions: &HashMap<Rkey, usize>,
    ) -> Result<Vec<usize>, WalkError> {
        let mut offsets = Vec::new();
        let mut level_keys = Vec::new();
        let mut child_links = Vec::new();

        for link in links {
            println!(
                "\n{level}~{}..",
                link.to_bytes()
                    .iter()
                    .take(5)
                    .map(|c| format!("{c:02x}"))
                    .collect::<Vec<_>>()
                    .join("")
            );

            let Some(mpb) = blocks.get(&link) else {
                // TODO: drop an 'x' for missing node
                continue;
            };
            let node: MstNode = match mpb {
                MaybeProcessedBlock::Processed(_) => return Err(WalkError::BadCommitFingerprint),
                MaybeProcessedBlock::Raw(bytes) => serde_ipld_dagcbor::from_slice(bytes)?,
            };

            let mut last_key = "".to_string();
            let mut last_was_record = true;
            for thing in node.things {
                let mut node_keys = Vec::new();

                let has = blocks.contains_key(&thing.link);

                match thing.kind {
                    ThingKind::ChildNode => {
                        if has {
                            child_links.push(thing.link);
                            last_was_record = false;
                        }
                    }
                    ThingKind::Record(key) => {
                        let us = positions[&key];

                        if !last_was_record && last_key.is_empty() {
                            let them = positions[&last_key];
                            for i in 0..(them - 1) {
                                if i < (us + 1) {
                                    print!("  ");
                                } else {
                                    print!("~~");
                                }
                            }
                            println!("~");
                        }

                        for _ in 0..us {
                            print!("  ");
                        }
                        if has {
                            print!("O");
                        } else {
                            print!("x");
                        }
                        println!(" {key}");
                        node_keys.push(key.clone());
                        last_key = key;
                        last_was_record = true;
                    }
                }
                level_keys.push(node_keys);
            }

            offsets.push(1);
        }

        if !child_links.is_empty() {
            Self::vnext(level - 1, child_links, blocks, positions)?; // TODO use offsets
        }

        Ok(offsets)
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
