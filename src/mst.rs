//! Low-level types for parsing raw atproto MST CARs
//!
//! The primary aim is to work through the **tree** structure. Non-node blocks
//! are left as raw bytes, for upper levels to parse into DAG-CBOR or whatever.

use ipld_core::ipld::Ipld;
use ipld_core::cid::Cid;
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
    pub sig: ipld_core::ipld::Ipld, // TODO (vec<u8> fails with Mismatch { expect_major: 4, byte: 88 })
}

/// MST node data schema
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
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
    /// Check if a node has any entries
    ///
    /// An empty repository with no records is represented as a single MST node
    /// with an empty array of entries. This is the only situation in which a
    /// tree may contain an empty leaf node which does not either contain keys
    /// ("entries") or point to a sub-tree containing entries.
    ///
    /// TODO: to me this is slightly unclear with respect to `l` (ask someone).
    /// ...is that what "The top of the tree must not be a an empty node which
    /// only points to a sub-tree." is referring to?
    pub fn is_empty(&self) -> bool {
        self.left.is_none() && self.entries.is_empty()
    }
}

/// TreeEntry object
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    /// count of bytes shared with previous TreeEntry in this Node (if any)
    #[serde(rename = "p")]
    pub prefix_len: usize,
    /// remainder of key for this TreeEntry, after "prefixlen" have been removed
    #[serde(rename = "k")]
    pub keysuffix: Ipld, // can we String this here?
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
