//! Depth-first MST traversal

use crate::disk::SqliteReader;
use crate::drive::{MaybeProcessedBlock, Processable};
use crate::mst::Node;
use ipld_core::cid::Cid;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
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
    StorageError(#[from] rusqlite::Error),
    #[error("Decode error: {0}")]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
}

/// Errors from invalid Rkeys
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum MstError {
    #[error("Failed to compute an rkey due to invalid prefix_len")]
    EntryPrefixOutOfbounds,
    #[error("RKey was not utf-8")]
    EntryRkeyNotUtf8(#[from] std::string::FromUtf8Error),
    #[error("Nodes cannot be empty (except for an entirely empty MST)")]
    EmptyNode,
    #[error("Found an entry with rkey at the wrong depth")]
    WrongDepth,
    #[error("Lost track of our depth (possible bug?)")]
    LostDepth,
    #[error("MST depth underflow: depth-0 node with child trees")]
    DepthUnderflow,
    #[error("Encountered an rkey out of order while walking the MST")]
    RkeyOutOfOrder,
}

/// Walker outputs
#[derive(Debug)]
pub enum Step<T> {
    /// We needed this CID but it's not in the block store
    Missing(Cid),
    /// Reached the end of the MST! yay!
    Finish,
    /// A record was found!
    Step { rkey: String, data: T },
}

#[derive(Debug, Clone, PartialEq)]
enum Need {
    Node { depth: Depth, cid: Cid },
    Record { rkey: String, cid: Cid },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Depth {
    Root,
    Depth(u32),
}

impl Depth {
    fn from_key(key: &[u8]) -> Self {
        let mut zeros = 0;
        for byte in Sha256::digest(key) {
            let leading = byte.leading_zeros();
            zeros += leading;
            if leading < 8 {
                break;
            }
        }
        Self::Depth(zeros / 2) // truncating divide (rounds down)
    }
    fn next_expected(&self) -> Result<Option<u32>, MstError> {
        match self {
            Self::Root => Ok(None),
            Self::Depth(d) => d.checked_sub(1).ok_or(MstError::DepthUnderflow).map(Some),
        }
    }
}

fn push_from_node(stack: &mut Vec<Need>, node: &Node, parent_depth: Depth) -> Result<(), MstError> {
    // empty nodes are not allowed in the MST
    // ...except for a single one for empty MST, but we wouldn't be pushing that
    if node.is_empty() {
        return Err(MstError::EmptyNode);
    }

    let mut entries = Vec::with_capacity(node.entries.len());
    let mut prefix = vec![];
    let mut this_depth = parent_depth.next_expected()?;

    for entry in &node.entries {
        let mut rkey = vec![];
        let pre_checked = prefix
            .get(..entry.prefix_len)
            .ok_or(MstError::EntryPrefixOutOfbounds)?;
        rkey.extend_from_slice(pre_checked);
        rkey.extend_from_slice(&entry.keysuffix);

        let Depth::Depth(key_depth) = Depth::from_key(&rkey) else {
            return Err(MstError::WrongDepth);
        };

        // this_depth is `none` if we are the deepest child (directly below root)
        // in that case we accept whatever highest depth is claimed
        let expected_depth = match this_depth {
            Some(d) => d,
            None => {
                this_depth = Some(key_depth);
                key_depth
            }
        };

        // all keys we find should be this depth
        if key_depth != expected_depth {
            return Err(MstError::DepthUnderflow);
        }

        prefix = rkey.clone();

        entries.push(Need::Record {
            rkey: String::from_utf8(rkey)?,
            cid: entry.value,
        });
        if let Some(ref tree) = entry.tree {
            entries.push(Need::Node {
                depth: Depth::Depth(key_depth),
                cid: *tree,
            });
        }
    }

    entries.reverse();
    stack.append(&mut entries);

    let d = this_depth.ok_or(MstError::LostDepth)?;

    if let Some(tree) = node.left {
        stack.push(Need::Node {
            depth: Depth::Depth(d),
            cid: tree,
        });
    }
    Ok(())
}

/// Traverser of an atproto MST
///
/// Walks the tree from left-to-right in depth-first order
#[derive(Debug)]
pub struct Walker {
    stack: Vec<Need>,
    prev: String,
}

impl Walker {
    pub fn new(tree_root_cid: Cid) -> Self {
        Self {
            stack: vec![Need::Node {
                depth: Depth::Root,
                cid: tree_root_cid,
            }],
            prev: "".to_string(),
        }
    }

    /// Advance through nodes until we find a record or can't go further
    pub fn step<T: Processable>(
        &mut self,
        blocks: &mut HashMap<Cid, MaybeProcessedBlock<T>>,
        process: impl Fn(Vec<u8>) -> T,
    ) -> Result<Step<T>, WalkError> {
        loop {
            let Some(need) = self.stack.last_mut() else {
                log::trace!("tried to walk but we're actually done.");
                return Ok(Step::Finish);
            };

            match need {
                &mut Need::Node { depth, cid } => {
                    log::trace!("need node {cid:?}");
                    let Some(block) = blocks.remove(&cid) else {
                        log::trace!("node not found, resting");
                        return Ok(Step::Missing(cid));
                    };

                    let MaybeProcessedBlock::Raw(data) = block else {
                        return Err(WalkError::BadCommitFingerprint);
                    };
                    let node = serde_ipld_dagcbor::from_slice::<Node>(&data)
                        .map_err(WalkError::BadCommit)?;

                    // found node, make sure we remember
                    self.stack.pop();

                    // queue up work on the found node next
                    push_from_node(&mut self.stack, &node, depth)?;
                }
                Need::Record { rkey, cid } => {
                    log::trace!("need record {cid:?}");
                    // note that we cannot *remove* a record block, sadly, since
                    // there can be multiple rkeys pointing to the same cid.
                    let Some(data) = blocks.get_mut(cid) else {
                        return Ok(Step::Missing(*cid));
                    };
                    let rkey = rkey.clone();
                    let data = match data {
                        MaybeProcessedBlock::Raw(data) => process(data.to_vec()),
                        MaybeProcessedBlock::Processed(t) => t.clone(),
                    };

                    // found node, make sure we remember
                    self.stack.pop();

                    // rkeys *must* be in order or else the tree is invalid (or
                    // we have a bug)
                    if rkey <= self.prev {
                        return Err(MstError::RkeyOutOfOrder)?;
                    }
                    self.prev = rkey.clone();

                    return Ok(Step::Step { rkey, data });
                }
            }
        }
    }

    /// blocking!!!!!!
    pub fn disk_step<T: Processable>(
        &mut self,
        reader: &mut SqliteReader,
        process: impl Fn(Vec<u8>) -> T,
    ) -> Result<Step<T>, WalkError> {
        loop {
            let Some(need) = self.stack.last_mut() else {
                log::trace!("tried to walk but we're actually done.");
                return Ok(Step::Finish);
            };

            match need {
                &mut Need::Node { depth, cid } => {
                    let cid_bytes = cid.to_bytes();
                    log::trace!("need node {cid:?}");
                    let Some(block_bytes) = reader.get(cid_bytes)? else {
                        log::trace!("node not found, resting");
                        return Ok(Step::Missing(cid));
                    };

                    let block: MaybeProcessedBlock<T> = crate::drive::decode(&block_bytes)?;

                    let MaybeProcessedBlock::Raw(data) = block else {
                        return Err(WalkError::BadCommitFingerprint);
                    };
                    let node = serde_ipld_dagcbor::from_slice::<Node>(&data)
                        .map_err(WalkError::BadCommit)?;

                    // found node, make sure we remember
                    self.stack.pop();

                    // queue up work on the found node next
                    push_from_node(&mut self.stack, &node, depth).map_err(WalkError::MstError)?;
                }
                Need::Record { rkey, cid } => {
                    log::trace!("need record {cid:?}");
                    let cid_bytes = cid.to_bytes();
                    let Some(data_bytes) = reader.get(cid_bytes)? else {
                        log::trace!("record block not found, resting");
                        return Ok(Step::Missing(*cid));
                    };
                    let data: MaybeProcessedBlock<T> = crate::drive::decode(&data_bytes)?;
                    let rkey = rkey.clone();
                    let data = match data {
                        MaybeProcessedBlock::Raw(data) => process(data),
                        MaybeProcessedBlock::Processed(t) => t.clone(),
                    };

                    // found node, make sure we remember
                    self.stack.pop();

                    log::trace!("emitting a block as a step. depth={}", self.stack.len());

                    // rkeys *must* be in order or else the tree is invalid (or
                    // we have a bug)
                    if rkey <= self.prev {
                        return Err(MstError::RkeyOutOfOrder)?;
                    }
                    self.prev = rkey.clone();

                    return Ok(Step::Step { rkey, data });
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    // use crate::mst::Entry;

    fn cid1() -> Cid {
        "bafyreihixenvk3ahqbytas4hk4a26w43bh6eo3w6usjqtxkpzsvi655a3m"
            .parse()
            .unwrap()
    }
    //     fn cid2() -> Cid {
    //         "QmY7Yh4UquoXHLPFo2XbhXkhBvFoPwmQUSa92pxnxjQuPU"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid3() -> Cid {
    //         "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid4() -> Cid {
    //         "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid5() -> Cid {
    //         "QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid6() -> Cid {
    //         "QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid7() -> Cid {
    //         "bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid8() -> Cid {
    //         "bafyreif3tfdpr5n4jdrbielmcapwvbpcthepfkwq2vwonmlhirbjmotedi"
    //             .parse()
    //             .unwrap()
    //     }
    //     fn cid9() -> Cid {
    //         "bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq"
    //             .parse()
    //             .unwrap()
    //     }

    #[test]
    fn test_depth_spec_0() {
        let d = Depth::from_key(b"2653ae71");
        assert_eq!(d, Depth::Depth(0))
    }

    #[test]
    fn test_depth_spec_1() {
        let d = Depth::from_key(b"blue");
        assert_eq!(d, Depth::Depth(1))
    }

    #[test]
    fn test_depth_spec_4() {
        let d = Depth::from_key(b"app.bsky.feed.post/454397e440ec");
        assert_eq!(d, Depth::Depth(4))
    }

    #[test]
    fn test_depth_spec_8() {
        let d = Depth::from_key(b"app.bsky.feed.post/9adeb165882c");
        assert_eq!(d, Depth::Depth(8))
    }

    #[test]
    fn test_depth_ietf_draft_0() {
        let d = Depth::from_key(b"key1");
        assert_eq!(d, Depth::Depth(0))
    }

    #[test]
    fn test_depth_ietf_draft_1() {
        let d = Depth::from_key(b"key7");
        assert_eq!(d, Depth::Depth(1))
    }

    #[test]
    fn test_depth_ietf_draft_4() {
        let d = Depth::from_key(b"key515");
        assert_eq!(d, Depth::Depth(4))
    }

    #[test]
    fn test_depth_interop() {
        // examples from https://github.com/bluesky-social/atproto-interop-tests/blob/main/mst/key_heights.json
        for (k, expected) in [
            ("", 0),
            ("asdf", 0),
            ("blue", 1),
            ("2653ae71", 0),
            ("88bfafc7", 2),
            ("2a92d355", 4),
            ("884976f5", 6),
            ("app.bsky.feed.post/454397e440ec", 4),
            ("app.bsky.feed.post/9adeb165882c", 8),
        ] {
            let d = Depth::from_key(k.as_bytes());
            assert_eq!(d, Depth::Depth(expected), "key: {}", k);
        }
    }

    #[test]
    fn test_push_empty_fails() {
        let empty_node = Node {
            left: None,
            entries: vec![],
        };
        let mut stack = vec![];
        let err = push_from_node(&mut stack, &empty_node, Depth::Depth(4));
        assert_eq!(err, Err(MstError::EmptyNode));
    }

    #[test]
    fn test_push_one_node() {
        let node = Node {
            left: Some(cid1()),
            entries: vec![],
        };
        let mut stack = vec![];
        push_from_node(&mut stack, &node, Depth::Depth(4)).unwrap();
        assert_eq!(
            stack.last(),
            Some(Need::Node {
                depth: Depth::Depth(3),
                cid: cid1()
            })
            .as_ref()
        );
    }

    //     #[test]
    //     fn test_needs_from_node_just_one_record() {
    //         let node = Node {
    //             left: None,
    //             entries: vec![Entry {
    //                 keysuffix: "asdf".into(),
    //                 prefix_len: 0,
    //                 value: cid1(),
    //                 tree: None,
    //             }],
    //         };
    //         assert_eq!(
    //             needs_from_node(node).unwrap(),
    //             vec![Need::Record {
    //                 rkey: "asdf".into(),
    //                 cid: cid1(),
    //             },]
    //         );
    //     }

    //     #[test]
    //     fn test_needs_from_node_two_records() {
    //         let node = Node {
    //             left: None,
    //             entries: vec![
    //                 Entry {
    //                     keysuffix: "asdf".into(),
    //                     prefix_len: 0,
    //                     value: cid1(),
    //                     tree: None,
    //                 },
    //                 Entry {
    //                     keysuffix: "gh".into(),
    //                     prefix_len: 2,
    //                     value: cid2(),
    //                     tree: None,
    //                 },
    //             ],
    //         };
    //         assert_eq!(
    //             needs_from_node(node).unwrap(),
    //             vec![
    //                 Need::Record {
    //                     rkey: "asdf".into(),
    //                     cid: cid1(),
    //                 },
    //                 Need::Record {
    //                     rkey: "asgh".into(),
    //                     cid: cid2(),
    //                 },
    //             ]
    //         );
    //     }

    //     #[test]
    //     fn test_needs_from_node_with_both() {
    //         let node = Node {
    //             left: None,
    //             entries: vec![Entry {
    //                 keysuffix: "asdf".into(),
    //                 prefix_len: 0,
    //                 value: cid1(),
    //                 tree: Some(cid2()),
    //             }],
    //         };
    //         assert_eq!(
    //             needs_from_node(node).unwrap(),
    //             vec![
    //                 Need::Record {
    //                     rkey: "asdf".into(),
    //                     cid: cid1(),
    //                 },
    //                 Need::Node(cid2()),
    //             ]
    //         );
    //     }

    //     #[test]
    //     fn test_needs_from_node_left_and_record() {
    //         let node = Node {
    //             left: Some(cid1()),
    //             entries: vec![Entry {
    //                 keysuffix: "asdf".into(),
    //                 prefix_len: 0,
    //                 value: cid2(),
    //                 tree: None,
    //             }],
    //         };
    //         assert_eq!(
    //             needs_from_node(node).unwrap(),
    //             vec![
    //                 Need::Node(cid1()),
    //                 Need::Record {
    //                     rkey: "asdf".into(),
    //                     cid: cid2(),
    //                 },
    //             ]
    //         );
    //     }

    //     #[test]
    //     fn test_needs_from_full_node() {
    //         let node = Node {
    //             left: Some(cid1()),
    //             entries: vec![
    //                 Entry {
    //                     keysuffix: "asdf".into(),
    //                     prefix_len: 0,
    //                     value: cid2(),
    //                     tree: Some(cid3()),
    //                 },
    //                 Entry {
    //                     keysuffix: "ghi".into(),
    //                     prefix_len: 1,
    //                     value: cid4(),
    //                     tree: Some(cid5()),
    //                 },
    //                 Entry {
    //                     keysuffix: "jkl".into(),
    //                     prefix_len: 2,
    //                     value: cid6(),
    //                     tree: Some(cid7()),
    //                 },
    //                 Entry {
    //                     keysuffix: "mno".into(),
    //                     prefix_len: 4,
    //                     value: cid8(),
    //                     tree: Some(cid9()),
    //                 },
    //             ],
    //         };
    //         assert_eq!(
    //             needs_from_node(node).unwrap(),
    //             vec![
    //                 Need::Node(cid1()),
    //                 Need::Record {
    //                     rkey: "asdf".into(),
    //                     cid: cid2(),
    //                 },
    //                 Need::Node(cid3()),
    //                 Need::Record {
    //                     rkey: "aghi".into(),
    //                     cid: cid4(),
    //                 },
    //                 Need::Node(cid5()),
    //                 Need::Record {
    //                     rkey: "agjkl".into(),
    //                     cid: cid6(),
    //                 },
    //                 Need::Node(cid7()),
    //                 Need::Record {
    //                     rkey: "agjkmno".into(),
    //                     cid: cid8(),
    //                 },
    //                 Need::Node(cid9()),
    //             ]
    //         );
    //     }
}
