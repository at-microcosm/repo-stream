//! Depth-first MST traversal

use crate::drive::{MaybeProcessedBlock, ProcRes};
use crate::mst::Node;
use ipld_core::cid::Cid;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum Trip<E: Error> {
    #[error("empty mst nodes are not allowed")]
    NodeEmpty,
    #[error("Failed to decode commit block: {0}")]
    BadCommit(Box<dyn std::error::Error>),
    #[error("Failed to process record: {0}")]
    RecordFailedProcessing(Box<dyn Error>),
    #[error("Action node error: {0}")]
    ActionNode(#[from] ActionNodeError),
    #[error("Process failed: {0}")]
    ProcessFailed(E),
}

#[derive(Debug, thiserror::Error)]
pub enum ActionNodeError {
    #[error("Failed to compute an rkey due to invalid prefix_len")]
    EntryPrefixOutOfbounds,
    #[error("RKey was not utf-8")]
    EntryRkeyNotUtf8(#[from] std::string::FromUtf8Error),
}

#[derive(Debug)]
pub enum Step<T> {
    Rest(Cid),
    Finish,
    Step { rkey: String, data: T },
}

/// some block we need (or have found)
#[derive(Clone, PartialEq)]
struct FindableLink {
    cid: Cid,
    found: bool,
}

impl From<&Cid> for FindableLink {
    fn from(cid: &Cid) -> Self {
        let cid = *cid;
        let found = false;
        FindableLink { cid, found }
    }
}

impl fmt::Debug for FindableLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = if self.found { "??" } else { "ok" };
        let cid_s = self.cid.to_string_of_base(multibase::Base::Base64).unwrap();
        let split_at = cid_s.char_indices().nth_back(4).unwrap().0;
        let cid_s = &cid_s[split_at..];
        write!(f, "({status} [{cid_s}])")
    }
}

/// a transformed mst::Entry with decompressed rkeys and which we can mutate
///
/// contains exactly one record link, and one optional subtree whose contents
/// are all to the right of this record's rkey
#[derive(Debug, PartialEq)]
struct ActionEntry {
    rkey: String,
    record: FindableLink,
    tree: Option<FindableLink>,
}

/// a transformed mst::Node which we can mutate and track progress on
///
/// contains an optional left subtree, whose contents are all to the left of
/// every entry in the entries array.
#[derive(Debug, PartialEq)]
struct ActionNode {
    left_tree: Option<FindableLink>,
    entries: Vec<ActionEntry>,
}

impl ActionNode {
    fn from_root(tree_root_cid: Cid) -> Self {
        ActionNode {
            left_tree: Some((&tree_root_cid).into()),
            entries: vec![],
        }
    }
    fn next(&self) -> Option<Need> {
        if let Some(findable) = &self.left_tree
            && !findable.found
        {
            return Some(Need::Node(findable.cid));
        }
        for ActionEntry { rkey, record, tree } in &self.entries {
            if !record.found {
                return Some(Need::Record {
                    rkey: rkey.to_string(),
                    cid: record.cid,
                });
            }
            if let Some(findable) = tree
                && !findable.found
            {
                return Some(Need::Node(findable.cid));
            }
        }
        None
    }
    fn found(&mut self, cid: &Cid) {
        // just be horrible for now, whatever
        if let Some(findable) = &mut self.left_tree
            && !findable.found
        {
            assert_eq!(*cid, findable.cid, "wrong found for next");
            findable.found = true;
            return;
        }
        for ActionEntry { record, tree, .. } in &mut self.entries {
            if !record.found {
                assert_eq!(*cid, record.cid, "wrong found for next, expected record");
                record.found = true;
                return;
            }
            if let Some(findable) = tree
                && !findable.found
            {
                assert_eq!(
                    *cid, findable.cid,
                    "wrong found for next, expected entry tree"
                );
                findable.found = true;
                return;
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Need {
    Node(Cid),
    Record { rkey: String, cid: Cid },
}

impl TryFrom<&Node> for ActionNode {
    type Error = ActionNodeError;

    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        let left_tree = node.left.as_ref().map(Into::into);

        let mut entries = vec![];
        let mut prefix = vec![];
        for entry in &node.entries {
            let mut rkey = vec![];
            let pre_checked = prefix
                .get(..entry.prefix_len)
                .ok_or(ActionNodeError::EntryPrefixOutOfbounds)?;
            rkey.extend_from_slice(pre_checked);
            rkey.extend_from_slice(&entry.keysuffix);
            prefix = rkey.clone();

            entries.push(ActionEntry {
                rkey: String::from_utf8(rkey)?, // TODO this has to be try_from
                record: (&entry.value).into(),
                tree: entry.tree.as_ref().map(Into::into),
            });
        }

        Ok(ActionNode { left_tree, entries })
    }
}

#[derive(Debug)]
pub struct Walker {
    stack: Vec<ActionNode>,
}

impl Walker {
    pub fn new(tree_root_cid: Cid) -> Self {
        Self {
            stack: vec![ActionNode::from_root(tree_root_cid)],
        }
    }

    pub fn walk<T: Clone, E: Error>(
        &mut self,
        blocks: &mut HashMap<Cid, MaybeProcessedBlock<T, E>>,
        process: impl Fn(&[u8]) -> ProcRes<T, E>,
    ) -> Result<Step<T>, Trip<E>> {
        loop {
            let Some(current_node) = self.stack.last_mut() else {
                log::trace!("tried to walk but we're actually done.");
                return Ok(Step::Finish);
            };
            let Some(mut need) = current_node.next() else {
                self.stack.pop();
                continue;
            };

            match &mut need {
                Need::Node(cid) => {
                    log::trace!("need node {cid:?}");
                    let Some(block) = blocks.remove(cid) else {
                        log::trace!("node not found, resting");
                        return Ok(Step::Rest(*cid));
                    };

                    let MaybeProcessedBlock::Raw(data) = block else {
                        return Err(Trip::BadCommit("failed commit fingerprint".into()));
                    };
                    let node = serde_ipld_dagcbor::from_slice::<Node>(&data)
                        .map_err(|e| Trip::BadCommit(e.into()))?;

                    // found node, make sure we remember
                    current_node.found(cid);

                    // queue up work on the found node next
                    self.stack.push((&node).try_into()?);
                }
                Need::Record { rkey, cid } => {
                    log::trace!("need record {cid:?}");
                    let Some(data) = blocks.get(cid) else {
                        log::trace!("record block not found, resting");
                        return Ok(Step::Rest(*cid));
                    };
                    let rkey = rkey.clone();
                    let data = match data {
                        MaybeProcessedBlock::Raw(data) => process(data),
                        MaybeProcessedBlock::Processed(Ok(t)) => Ok(t.clone()),
                        MaybeProcessedBlock::Processed(_e) => {
                            return Err(Trip::RecordFailedProcessing("booo".into()));
                        } // TODO
                    };

                    // found node, make sure we remember
                    current_node.found(cid);

                    log::trace!("emitting a block as a step. depth={}", self.stack.len());
                    let data = data.map_err(Trip::ProcessFailed)?;
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
    fn test_next_from_node_empty() {
        let node = Node {
            left: None,
            entries: vec![],
        };
        let action_node: ActionNode = (&node).try_into().unwrap();
        assert_eq!(action_node.next(), None);
    }

    #[test]
    fn test_needs_from_node_just_left() {
        let node = Node {
            left: Some(cid1()),
            entries: vec![],
        };
        let action_node: ActionNode = (&node).try_into().unwrap();
        assert_eq!(action_node.next(), Some(Need::Node(cid1())));
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
