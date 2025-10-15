//! Depth-first MST traversal

use crate::mst::Node;
use std::convert::Infallible;

use ipld_core::cid::Cid;
use serde::{Serialize, de::DeserializeOwned};

/// Errors that can happen while walking
#[derive(Debug, thiserror::Error)]
pub enum Trip {
    #[error("empty mst nodes are not allowed")]
    NodeEmpty,
    #[error("Failed to decode commit block: {0}")]
    BadCommit(serde_ipld_dagcbor::DecodeError<Infallible>),
    #[error("Action node error: {0}")]
    RkeyError(#[from] RkeyError),
    #[error("Process failed: {0}")]
    ProcessFailed(String),
}

/// Errors from invalid Rkeys
#[derive(Debug, thiserror::Error)]
pub enum RkeyError {
    #[error("Failed to compute an rkey due to invalid prefix_len")]
    EntryPrefixOutOfbounds,
    #[error("RKey was not utf-8")]
    EntryRkeyNotUtf8(#[from] std::string::FromUtf8Error),
    #[error("Encountered an rkey out of order while walking the MST")]
    RkeyOutOfOrder,
    #[error("Failed to decode node block: {0}")]
    NodeDecodeError(#[from] serde_ipld_dagcbor::DecodeError<Infallible>),
}

/// Walker outputs
#[derive(Debug)]
pub enum Step<T: Serialize + DeserializeOwned> {
    /// We need a CID but it's not in the block store
    ///
    /// Give the needed CID to the driver so it can load blocks until it's found
    Rest(Cid),
    /// Reached the end of the MST! yay!
    Finish,
    /// A record was found!
    Step { rkey: String, data: T },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Need {
    Node(Cid),
    Record { rkey: String, cid: Cid },
}

impl Need {
    pub fn cid(&self) -> Cid {
        match self {
            Need::Node(cid) => *cid,
            Need::Record { cid, .. } => *cid,
        }
    }
}

fn push_from_node(stack: &mut Vec<Need>, node: &Node) -> Result<(), RkeyError> {
    let mut entries = Vec::with_capacity(node.entries.len());

    let mut prefix = vec![];
    for entry in &node.entries {
        let mut rkey = vec![];
        let pre_checked = prefix
            .get(..entry.prefix_len)
            .ok_or(RkeyError::EntryPrefixOutOfbounds)?;
        rkey.extend_from_slice(pre_checked);
        rkey.extend_from_slice(&entry.keysuffix);
        prefix = rkey.clone();

        entries.push(Need::Record {
            rkey: String::from_utf8(rkey)?,
            cid: entry.value,
        });
        if let Some(ref tree) = entry.tree {
            entries.push(Need::Node(*tree));
        }
    }

    entries.reverse();
    stack.append(&mut entries);

    if let Some(tree) = node.left {
        stack.push(Need::Node(tree));
    }
    Ok(())
}

/// Traverser of an atproto MST
///
/// Walks the tree from left-to-right in depth-first order
///
/// (turning into more of a navigator)
/// it doesn't quite feel like the divisions of responsibility are right around
/// here yet.
#[derive(Debug, Default)]
pub struct Walker {
    stack: Vec<Need>,
    prev: String,
}

impl Walker {
    pub fn new(tree_root_cid: Cid) -> Self {
        Self {
            stack: vec![Need::Node(tree_root_cid)],
            prev: "".to_string(),
        }
    }

    pub fn next_needed(&mut self) -> Result<Option<Need>, RkeyError> {
        let Some(need) = self.stack.pop() else {
            return Ok(None);
        };
        if let Need::Record { ref rkey, .. } = need {
            // rkeys *must* be in order or else the tree is invalid (or
            // we have a bug)
            if *rkey <= self.prev {
                return Err(RkeyError::RkeyOutOfOrder);
            }
            self.prev = rkey.clone();
        }
        Ok(Some(need))
    }

    /// hacky: this must be called after next_needed if it was a node
    pub fn handle_node(&mut self, block: &[u8]) -> Result<(), RkeyError> {
        let node = serde_ipld_dagcbor::from_slice::<Node>(block)?;
        push_from_node(&mut self.stack, &node)?;
        Ok(())
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
    fn test_next_from_node_empty() {
        let node = Node {
            left: None,
            entries: vec![],
        };
        let mut stack = vec![];
        push_from_node(&mut stack, &node).unwrap();
        assert_eq!(stack.last(), None);
    }

    #[test]
    fn test_needs_from_node_just_left() {
        let node = Node {
            left: Some(cid1()),
            entries: vec![],
        };
        let mut stack = vec![];
        push_from_node(&mut stack, &node).unwrap();
        assert_eq!(stack.last(), Some(Need::Node(cid1())).as_ref());
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
