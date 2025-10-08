//! Depth-first MST traversal

use crate::mst::Node;
use ipld_core::cid::Cid;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum Trip {
    #[error("empty mst nodes are not allowed")]
    NodeEmpty,
    #[error("Failed to decode commit block: {0}")]
    BadCommit(Box<dyn std::error::Error>),
}

#[derive(Debug)]
pub enum Step {
    Rest,
    Finish,
    Step { rkey: Vec<u8>, data: Vec<u8> },
}

#[derive(Debug)]
enum Need {
    Node(Cid),
    Record { rkey: Vec<u8>, cid: Cid },
    AcutallyDone,
}

fn needs_from_node(node: Node) -> Vec<Need> {
    let mut out = vec![];
    if let Some(left_cid) = node.left {
        out.push(Need::Node(left_cid));
    }
    let mut prefix = vec![];
    for (i, entry) in node.entries.into_iter().enumerate() {
        let suffix = entry.keysuffix;
        let mut rkey = Vec::with_capacity(prefix.len() + suffix.len());
        rkey.extend_from_slice(&prefix);
        rkey.extend_from_slice(&suffix);
        if i == 0 {
            prefix.extend_from_slice(&suffix);
        }
        out.push(Need::Record {
            rkey,
            cid: entry.value,
        });
        if let Some(child_cid) = entry.tree {
            out.push(Need::Node(child_cid));
        }
    }
    // stack is right-to-left, for our left-to-right traversal
    out.reverse();
    out
}

#[derive(Debug)]
pub struct Walker {
    current: Need,
    stack: Vec<Need>,
}

impl Walker {
    pub fn new(tree_root_cid: Cid) -> Self {
        Self {
            current: Need::Node(tree_root_cid),
            stack: Vec::new(),
        }
    }

    pub fn walk(&mut self, blocks: &mut HashMap<Cid, Vec<u8>>) -> Result<Step, Trip> {
        loop {
            match &mut self.current {
                Need::Node(cid) => {
                    log::trace!("need node {cid:?}");
                    let Some(block) = blocks.remove(cid) else {
                        log::trace!("node not found, resting");
                        return Ok(Step::Rest);
                    };
                    let node = serde_ipld_dagcbor::from_slice::<Node>(&block)
                        .map_err(|e| Trip::BadCommit(e.into()))?;
                    let mut needs = needs_from_node(node);
                    self.stack.append(&mut needs);
                    if let Some(need) = self.stack.pop() {
                        log::trace!("found a need from the stack {need:?}");
                        self.current = need;
                    } else {
                        log::trace!("no more needs from stack, ig we are done?");
                        return Ok(Step::Finish);
                    }
                }
                Need::Record { rkey, cid } => {
                    log::trace!("need record {cid:?}");
                    let Some(data) = blocks.get(cid) else {
                        log::trace!("record block not found, resting");
                        return Ok(Step::Rest);
                    };
                    let rkey = rkey.to_vec();
                    let data = data.to_vec();
                    if let Some(next) = self.stack.pop() {
                        log::trace!("updated current from stack");
                        self.current = next;
                    } else {
                        log::trace!("nothing left on the stack, making us done");
                        self.current = Need::AcutallyDone;
                    }
                    log::trace!("providing a block as a step");
                    return Ok(Step::Step { rkey, data });
                }
                Need::AcutallyDone => {
                    log::trace!("tried to walk but we're actually done.");
                    return Ok(Step::Finish);
                }
            }
        }
    }
}
