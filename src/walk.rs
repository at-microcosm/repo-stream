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

#[derive(Debug, PartialEq)]
enum Need {
    Node(Cid),
    Record { rkey: Vec<u8>, cid: Cid },
}

fn needs_from_node(node: Node) -> Vec<Need> {
    let mut out = vec![];
    if let Some(left_cid) = node.left {
        out.push(Need::Node(left_cid));
    }
    let prefix = if !node.entries.is_empty() {
        String::from_utf8(node.entries[0].keysuffix.to_vec()).unwrap()
    } else {
        "".to_string()
    };

    for entry in &node.entries {
        let pre = &prefix[..entry.prefix_len];
        let suf = String::from_utf8(entry.keysuffix.to_vec()).unwrap();
        let rkey = format!("{pre}{suf}");
        // if !rkey.contains('/') {
        //     println!("weird for entries: {:?}", node.entries);
        // }
        out.push(Need::Record {
            rkey: rkey.into_bytes(),
            cid: entry.value,
        });
        // let suffix = entry.keysuffix;
        // let mut rkey = String::new();
        // rkey.extend_from_slice(&prefix[..entry.prefix_len]);
        // rkey.extend_from_slice(&suffix);
        // if i == 0 {
        //     prefix.extend_from_slice(&suffix);
        // }
        // out.push(Need::Record {
        //     rkey,
        //     cid: entry.value,
        // });
        if let Some(child_cid) = entry.tree {
            out.push(Need::Node(child_cid));
        }
    }
    out
}

#[derive(Debug)]
pub struct Walker {
    stack: Vec<Need>,
}

impl Walker {
    pub fn new(tree_root_cid: Cid) -> Self {
        Self {
            stack: vec![Need::Node(tree_root_cid)],
        }
    }

    pub fn walk(&mut self, blocks: &mut HashMap<Cid, Vec<u8>>) -> Result<Step, Trip> {
        // for (i, need) in self.stack.iter().enumerate() {
        //     let k = if let Need::Record { rkey, .. } = need {
        //         String::from_utf8(rkey.to_vec()).unwrap()
        //     } else {
        //         "#".to_string()
        //     };
        //     println!("{: <1$} {k}", "", i)
        // }
        // let current = if let Need::Record { rkey, .. } = &self.current {
        //     String::from_utf8(rkey.to_vec()).unwrap()
        // } else {
        //     "#".to_string()
        // };
        // println!("{: <1$} @{current}", "", self.stack.len() - 1);
        loop {
            let Some(mut current) = self.stack.last() else {
                log::trace!("tried to walk but we're actually done.");
                return Ok(Step::Finish);
            };
            match &mut current {
                Need::Node(cid) => {
                    log::trace!("need node {cid:?}");
                    let Some(block) = blocks.remove(cid) else {
                        log::trace!("node not found, resting");
                        return Ok(Step::Rest);
                    };
                    let node = serde_ipld_dagcbor::from_slice::<Node>(&block)
                        .map_err(|e| Trip::BadCommit(e.into()))?;

                    // found node, make sure we pop it from the stack
                    self.stack.pop();

                    // stash future work so that the right-most work is leftest in the stack
                    for need in needs_from_node(node).into_iter().rev() {
                        self.stack.push(need);
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

                    // found node, make sure we pop it from the stack
                    self.stack.pop();

                    log::trace!("emitting a block as a step. depth={}", self.stack.len());
                    return Ok(Step::Step { rkey, data });
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mst::Entry;

    fn cid1() -> Cid {
        "bafyreihixenvk3ahqbytas4hk4a26w43bh6eo3w6usjqtxkpzsvi655a3m"
            .parse()
            .unwrap()
    }
    fn cid2() -> Cid {
        "QmY7Yh4UquoXHLPFo2XbhXkhBvFoPwmQUSa92pxnxjQuPU"
            .parse()
            .unwrap()
    }
    fn cid3() -> Cid {
        "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse()
            .unwrap()
    }
    fn cid4() -> Cid {
        "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR"
            .parse()
            .unwrap()
    }
    fn cid5() -> Cid {
        "QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D"
            .parse()
            .unwrap()
    }
    fn cid6() -> Cid {
        "QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm"
            .parse()
            .unwrap()
    }
    fn cid7() -> Cid {
        "bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze"
            .parse()
            .unwrap()
    }
    fn cid8() -> Cid {
        "bafyreif3tfdpr5n4jdrbielmcapwvbpcthepfkwq2vwonmlhirbjmotedi"
            .parse()
            .unwrap()
    }
    fn cid9() -> Cid {
        "bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq"
            .parse()
            .unwrap()
    }

    #[test]
    fn test_needs_from_node_empty() {
        let node = Node {
            left: None,
            entries: vec![],
        };
        assert_eq!(needs_from_node(node), vec![]);
    }

    #[test]
    fn test_needs_from_node_just_left() {
        let node = Node {
            left: Some(cid1()),
            entries: vec![],
        };
        assert_eq!(needs_from_node(node), vec![Need::Node(cid1()),]);
    }

    #[test]
    fn test_needs_from_node_just_one_record() {
        let node = Node {
            left: None,
            entries: vec![Entry {
                keysuffix: "asdf".into(),
                prefix_len: 0,
                value: cid1(),
                tree: None,
            }],
        };
        assert_eq!(
            needs_from_node(node),
            vec![Need::Record {
                rkey: "asdf".into(),
                cid: cid1(),
            },]
        );
    }

    #[test]
    fn test_needs_from_node_two_records() {
        let node = Node {
            left: None,
            entries: vec![
                Entry {
                    keysuffix: "asdf".into(),
                    prefix_len: 0,
                    value: cid1(),
                    tree: None,
                },
                Entry {
                    keysuffix: "gh".into(),
                    prefix_len: 2,
                    value: cid2(),
                    tree: None,
                },
            ],
        };
        assert_eq!(
            needs_from_node(node),
            vec![
                Need::Record {
                    rkey: "asdf".into(),
                    cid: cid1(),
                },
                Need::Record {
                    rkey: "asgh".into(),
                    cid: cid2(),
                },
            ]
        );
    }

    #[test]
    fn test_needs_from_node_with_both() {
        let node = Node {
            left: None,
            entries: vec![Entry {
                keysuffix: "asdf".into(),
                prefix_len: 0,
                value: cid1(),
                tree: Some(cid2()),
            }],
        };
        assert_eq!(
            needs_from_node(node),
            vec![
                Need::Record {
                    rkey: "asdf".into(),
                    cid: cid1(),
                },
                Need::Node(cid2()),
            ]
        );
    }

    #[test]
    fn test_needs_from_node_left_and_record() {
        let node = Node {
            left: Some(cid1()),
            entries: vec![Entry {
                keysuffix: "asdf".into(),
                prefix_len: 0,
                value: cid2(),
                tree: None,
            }],
        };
        assert_eq!(
            needs_from_node(node),
            vec![
                Need::Node(cid1()),
                Need::Record {
                    rkey: "asdf".into(),
                    cid: cid2(),
                },
            ]
        );
    }

    #[test]
    fn test_needs_from_full_node() {
        let node = Node {
            left: Some(cid1()),
            entries: vec![
                Entry {
                    keysuffix: "asdf".into(),
                    prefix_len: 0,
                    value: cid2(),
                    tree: Some(cid3()),
                },
                Entry {
                    keysuffix: "ghi".into(),
                    prefix_len: 1,
                    value: cid4(),
                    tree: Some(cid5()),
                },
                Entry {
                    keysuffix: "jkl".into(),
                    prefix_len: 2,
                    value: cid6(),
                    tree: Some(cid7()),
                },
                Entry {
                    keysuffix: "mno".into(),
                    prefix_len: 4,
                    value: cid8(),
                    tree: Some(cid9()),
                },
            ],
        };
        assert_eq!(
            needs_from_node(node),
            vec![
                Need::Node(cid1()),
                Need::Record {
                    rkey: "asdf".into(),
                    cid: cid2(),
                },
                Need::Node(cid3()),
                Need::Record {
                    rkey: "aghi".into(),
                    cid: cid4(),
                },
                Need::Node(cid5()),
                Need::Record {
                    rkey: "asjkl".into(),
                    cid: cid6(),
                },
                Need::Node(cid7()),
                Need::Record {
                    rkey: "asdfmno".into(),
                    cid: cid8(),
                },
                Need::Node(cid9()),
            ]
        );
    }
}
