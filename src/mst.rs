//! Low-level types for parsing raw atproto MST CARs
//!
//! The primary aim is to work through the **tree** structure. Non-node blocks
//! are left as raw bytes, for upper levels to parse into DAG-CBOR or whatever.

use sha2::{Digest, Sha256};
use cid::Cid;
use serde::Deserialize;

/// The top-level data object in a repository's tree is a signed commit.
#[derive(Debug, Deserialize)]
// #[serde(deny_unknown_fields)]
pub struct Commit {
    /// the account DID associated with the repo, in strictly normalized form
    /// (eg, lowercase as appropriate)
    pub did: String,
    /// fixed value of 3 for this repo format version
    pub version: u64,
    /// pointer to the top of the repo contents tree structure (MST)
    pub data: Cid,
    /// revision of the repo, used as a logical clock.
    ///
    /// TID format. Must increase monotonically. Recommend using current
    /// timestamp as TID; rev values in the "future" (beyond a fudge factor)
    /// should be ignored and not processed
    pub rev: String,
    /// pointer (by hash) to a previous commit object for this repository.
    ///
    /// Could be used to create a chain of history, but largely unused (included
    /// for v2 backwards compatibility). In version 3 repos, this field must
    /// exist in the CBOR object, but is virtually always null. NOTE: previously
    /// specified as nullable and optional, but this caused interoperability
    /// issues.
    pub prev: Option<Cid>,
    /// cryptographic signature of this commit, as raw bytes
    #[serde(with = "serde_bytes")]
    pub sig: serde_bytes::ByteBuf,
}

use serde::de::{self, Deserializer, Visitor, MapAccess, SeqAccess, Unexpected};
use std::fmt;

pub type Depth = u32;

#[inline(always)]
pub fn atproto_mst_depth(key: &str) -> Depth {
    // 128 bits oughta be enough: https://bsky.app/profile/retr0.id/post/3jwwbf4izps24
    u128::from_be_bytes(Sha256::digest(key).split_at(16).0.try_into().unwrap()).leading_zeros() / 2
}

#[derive(Debug)]
pub(crate) struct MstNode {
    pub depth: Option<Depth>, // known for nodes with entries (required for root)
    pub things: Vec<NodeThing>,
}

#[derive(Debug)]
pub(crate) struct NodeThing {
    pub(crate) cid: Cid,
    pub(crate) kind: ThingKind,
}

#[derive(Debug)]
pub(crate) enum ThingKind {
    Tree,
    Value { rkey: String },
}

pub(crate) struct Entries(Vec<NodeThing>, Option<Depth>);

impl<'de> Deserialize<'de> for Entries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntriesVisitor;
        impl<'de> Visitor<'de> for EntriesVisitor {
            type Value = Entries;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("seq MstEntries")
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
            where
                S: SeqAccess<'de>,
            {
                let mut children: Vec<NodeThing> = Vec::with_capacity(seq.size_hint().unwrap_or(5));
                let mut prefix: Vec<u8> = vec![];
                let mut depth = None;
                while let Some(entry) = seq.next_element::<Entry>()? {
                    let mut rkey: Vec<u8> = vec![];
                    let pre_checked = prefix
                        .get(..entry.prefix_len)
                        .ok_or_else(|| de::Error::invalid_value(
                            Unexpected::Bytes(&prefix),
                            &"a prefix at least as long as the prefix_len",
                        ))?;

                    rkey.extend_from_slice(pre_checked);
                    rkey.extend_from_slice(&entry.keysuffix);

                    let rkey_s = String::from_utf8(rkey.clone())
                        .map_err(|_| de::Error::invalid_value(
                            Unexpected::Bytes(&rkey),
                            &"a valid utf-8 rkey",
                        ))?;

                    let key_depth = atproto_mst_depth(&rkey_s);
                    if depth.is_none() {
                        depth = Some(key_depth);
                    } else if Some(key_depth) != depth {
                        return Err(de::Error::invalid_value(
                            Unexpected::Bytes(&prefix),
                            &"all rkeys to have equal MST depth",
                        ));
                    }

                    children.push(NodeThing {
                        cid: entry.value,
                        kind: ThingKind::Value { rkey: rkey_s },
                    });

                    if let Some(cid) = entry.tree {
                        children.push(NodeThing {
                            cid,
                            kind: ThingKind::Tree,
                        });
                    }

                    prefix = rkey;
                }

                Ok(Entries(children, depth))
            }
        }
        deserializer.deserialize_seq(EntriesVisitor)
    }
}

impl<'de> Deserialize<'de> for MstNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NodeVisitor;
        impl<'de> Visitor<'de> for NodeVisitor {
            type Value = MstNode;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct MstNode")
            }

            fn visit_map<V>(self, mut map: V) -> Result<MstNode, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut found_left = false;
                let mut left = None;
                let mut found_entries = false;
                let mut things = Vec::with_capacity(4); // "fanout of 4" so does this make sense????
                let mut depth = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "l" => {
                            if found_left {
                                return Err(de::Error::duplicate_field("l"));
                            }
                            found_left = true;
                            if let Some(cid) = map.next_value()? {
                                left = Some(NodeThing { cid, kind: ThingKind::Tree });
                            }
                        }
                        "e" => {
                            if found_entries {
                                return Err(de::Error::duplicate_field("e"));
                            }
                            found_entries = true;
                            let Entries(mut child_entries, d) = map.next_value()?;
                            things.append(&mut child_entries);
                            depth = d;
                        },
                        f => return Err(de::Error::unknown_field(f, NODE_FIELDS))
                    }
                }
                if !found_left {
                    return Err(de::Error::missing_field("l"));
                }
                if !found_entries {
                    return Err(de::Error::missing_field("e"));
                }

                things.reverse();
                if let Some(l) = left {
                    things.push(l);
                }

                Ok(MstNode { depth, things })
            }
        }

        const NODE_FIELDS: &[&str] = &["l", "e"];
        deserializer.deserialize_struct("MstNode", NODE_FIELDS, NodeVisitor)
    }
}

impl MstNode {
    pub(crate) fn is_empty(&self) -> bool {
        self.things.is_empty()
    }
}

/// MST node data schema
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Node {
    /// link to sub-tree Node on a lower level and with all keys sorting before
    /// keys at this node
    #[serde(rename = "l")]
    pub left: Option<Cid>,
    /// ordered list of TreeEntry objects
    ///
    /// atproto MSTs have a fanout of 4, so there can be max 4 entries.
    #[serde(rename = "e")]
    pub entries: Vec<Entry>, // maybe we can do [Option<Entry>; 4]?
}

impl Node {
    /// test if a block could possibly be a node
    ///
    /// we can't eagerly decode records except where we're *sure* they cannot be
    /// an mst node (and even then we can only attempt) because you can't know
    /// with certainty what a block is supposed to be without actually walking
    /// the tree.
    ///
    /// so if a block *could be* a node, any record converter must postpone
    /// processing. if it turns out it happens to be a very node-looking record,
    /// well, sorry, it just has to only be processed later when that's known.
    #[inline(always)]
    pub(crate) fn could_be(bytes: impl AsRef<[u8]>) -> bool {
        const NODE_FINGERPRINT: [u8; 3] = [
            0xA2, // map length 2 (for "l" and "e" keys)
            0x61, // text length 1
            b'e', // "e" before "l" because map keys have to be lex-sorted
                  // 0x8?: "e" has array (0x100 upper 3 bits) of some length
        ];
        let bytes = bytes.as_ref();
        bytes.starts_with(&NODE_FINGERPRINT)
            && bytes
                .get(3)
                .map(|b| b & 0b1110_0000 == 0x80)
                .unwrap_or(false)
    }

    // /// Check if a node has any entries
    // ///
    // /// An empty repository with no records is represented as a single MST node
    // /// with an empty array of entries. This is the only situation in which a
    // /// tree may contain an empty leaf node which does not either contain keys
    // /// ("entries") or point to a sub-tree containing entries.
    // pub(crate) fn is_empty(&self) -> bool {
    //     self.left.is_none() && self.entries.is_empty()
    // }
}

/// TreeEntry object
#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    /// count of bytes shared with previous TreeEntry in this Node (if any)
    #[serde(rename = "p")]
    pub prefix_len: usize,
    /// remainder of key for this TreeEntry, after "prefixlen" have been removed
    #[serde(rename = "k")]
    pub keysuffix: serde_bytes::ByteBuf,
    /// link to the record data (CBOR) for this entry
    #[serde(rename = "v")]
    pub value: Cid,
    /// link to a sub-tree Node at a lower level
    ///
    /// the lower level must have keys sorting after this TreeEntry's key (to
    /// the "right"), but before the next TreeEntry's key in this Node (if any)
    #[serde(rename = "t")]
    pub tree: Option<Cid>,
}
