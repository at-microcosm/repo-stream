//! Proven-range walking of CAR slices

use crate::{
    RepoPath,
    mem::MemCar,
    walk::{Output, WalkError, WalkItem},
};
use cid::Cid;
use std::ops::{Bound, RangeBounds};

/// Errors from [`MemCar::walk_slice`]
#[derive(Debug, thiserror::Error)]
pub enum SliceError {
    #[error("walk error: {0}")]
    Walk(#[from] WalkError),
    /// A record within the requested range has no block in the CAR
    #[error("record block absent within range: key={key:?} cid={cid}")]
    IncompleteRange { key: RepoPath, cid: Cid },
    /// An MST node block is absent within the requested range
    #[error("MST node block absent within range: cid={cid}")]
    MissingNode { cid: Cid },
    /// Proof failed: preceding key does not bound the lower end of the range
    #[error("preceding key {preceding:?} violates lower bound")]
    BadPrecedingKey { preceding: RepoPath },
    /// Proof failed: following key does not bound the upper end of the range
    #[error("following key {following:?} violates upper bound")]
    BadFollowingKey { following: RepoPath },
}

/// Proof that the walked range is complete.
///
/// Returned by [`SliceWalker::finish`].
pub struct SliceProof {
    /// Key immediately before the lower bound in the full tree,
    /// or `None` if the range starts at the tree's leftmost record.
    pub preceding_key: Option<RepoPath>,
    /// Key immediately after the upper bound in the full tree,
    /// or `None` if the range ends at the tree's rightmost record.
    pub following_key: Option<RepoPath>,
}

enum SliceState {
    Before,
    In,
    Done,
}

/// Iterator-like walker over a proven range of the MST.
///
/// Created by [`MemCar::walk_slice`]. Call [`SliceWalker::next`] to yield
/// records, then [`SliceWalker::finish`] to validate the proof.
pub struct SliceWalker<'a> {
    mem_car: &'a mut MemCar,
    lower: Bound<String>,
    upper: Bound<String>,
    preceding_key: Option<RepoPath>,
    following_key: Option<RepoPath>,
    state: SliceState,
}

impl SliceWalker<'_> {
    /// Yield the next in-range record.
    ///
    /// Transparently skips boundary items outside the range. Returns
    /// `Ok(None)` when the range is exhausted. Errors on any missing block
    /// within the range, or on an MST node absent after the first in-range
    /// record (which would leave the range unproven).
    /// Yield the next in-range record.
    ///
    /// Transparently skips boundary items outside the range. Returns
    /// `Ok(None)` when the range is exhausted — proof validation runs
    /// automatically before returning `None`, so the `while let` pattern
    /// is sufficient:
    ///
    /// ```ignore
    /// while let Some(output) = walker.next()? { ... }
    /// // proof has been validated; any violation surfaces as Err before None
    /// ```
    ///
    /// Errors on any missing block within the range, on an MST node absent
    /// after the first in-range record, or on a proof violation.
    pub fn next(&mut self) -> Result<Option<Output>, SliceError> {
        if matches!(self.state, SliceState::Done) {
            return Ok(None);
        }
        loop {
            match self.mem_car.next()? {
                None => {
                    self.state = SliceState::Done;
                    validate_lower(self.preceding_key.as_deref(), &self.lower)?;
                    validate_upper(self.following_key.as_deref(), &self.upper)?;
                    return Ok(None);
                }
                Some(WalkItem::MissingSubtree { cid }) => {
                    if matches!(self.state, SliceState::In) {
                        return Err(SliceError::MissingNode { cid });
                    }
                    // Before: boundary subtree outside the range, skip
                }
                Some(WalkItem::MissingRecord { key, cid }) => {
                    if is_before(&key, &self.lower) {
                        self.preceding_key = Some(key);
                    } else if is_after(&key, &self.upper) {
                        self.following_key = Some(key);
                        self.state = SliceState::Done;
                        validate_lower(self.preceding_key.as_deref(), &self.lower)?;
                        validate_upper(self.following_key.as_deref(), &self.upper)?;
                        return Ok(None);
                    } else {
                        return Err(SliceError::IncompleteRange { key, cid });
                    }
                }
                Some(WalkItem::Record(out)) => {
                    if is_before(&out.key, &self.lower) {
                        self.preceding_key = Some(out.key);
                    } else if is_after(&out.key, &self.upper) {
                        self.following_key = Some(out.key);
                        self.state = SliceState::Done;
                        validate_lower(self.preceding_key.as_deref(), &self.lower)?;
                        validate_upper(self.following_key.as_deref(), &self.upper)?;
                        return Ok(None);
                    } else {
                        self.state = SliceState::In;
                        return Ok(Some(out));
                    }
                }
            }
        }
    }

    /// Drive any remaining walk to completion and return the proof keys.
    ///
    /// Useful when breaking out of the [`next`] loop early and still wanting
    /// the proof. Drives remaining boundary items (O(log n) at most), with
    /// proof validation happening inside `next` as usual.
    pub fn finish(mut self) -> Result<SliceProof, SliceError> {
        while self.next()?.is_some() {}
        Ok(SliceProof {
            preceding_key: self.preceding_key,
            following_key: self.following_key,
        })
    }
}

impl MemCar {
    /// Walk a proven range of the MST.
    ///
    /// Returns a [`SliceWalker`] that yields records within `range` in key
    /// order. After the loop, call [`SliceWalker::finish`] to validate that
    /// the adjacent keys bound the range correctly.
    ///
    /// Accepts standard Rust range expressions:
    /// - `"a".."b"` — exclusive upper bound
    /// - `"a"..="b"` — inclusive upper bound
    /// - `"a"..` — from `a` to end of tree
    /// - `.."b"` — from start of tree to just before `b`
    /// - `..` — entire tree
    pub fn walk_slice<'r>(&mut self, range: impl RangeBounds<&'r str>) -> SliceWalker<'_> {
        let lower = bound_to_owned(range.start_bound());
        let upper = bound_to_owned(range.end_bound());
        SliceWalker {
            mem_car: self,
            lower,
            upper,
            preceding_key: None,
            following_key: None,
            state: SliceState::Before,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bound_to_owned(b: Bound<&&str>) -> Bound<String> {
    match b {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(s) => Bound::Included((*s).to_owned()),
        Bound::Excluded(s) => Bound::Excluded((*s).to_owned()),
    }
}

fn is_before(key: &str, lower: &Bound<String>) -> bool {
    match lower {
        Bound::Unbounded => false,
        Bound::Included(l) => key < l.as_str(),
        Bound::Excluded(l) => key <= l.as_str(),
    }
}

fn is_after(key: &str, upper: &Bound<String>) -> bool {
    match upper {
        Bound::Unbounded => false,
        Bound::Included(u) => key > u.as_str(),
        Bound::Excluded(u) => key >= u.as_str(),
    }
}

fn validate_lower(preceding: Option<&str>, lower: &Bound<String>) -> Result<(), SliceError> {
    let ok = match (preceding, lower) {
        (None, _) => true,
        (Some(p), Bound::Unbounded) => {
            unreachable!("is_before always false for Unbounded, but got {p:?}")
        }
        (Some(p), Bound::Included(l)) => p < l.as_str(),
        (Some(p), Bound::Excluded(l)) => p <= l.as_str(),
    };
    if ok {
        Ok(())
    } else {
        Err(SliceError::BadPrecedingKey {
            preceding: preceding.unwrap().to_owned(),
        })
    }
}

fn validate_upper(following: Option<&str>, upper: &Bound<String>) -> Result<(), SliceError> {
    let ok = match (following, upper) {
        (None, _) => true,
        (Some(f), Bound::Unbounded) => {
            unreachable!("is_after always false for Unbounded, but got {f:?}")
        }
        (Some(f), Bound::Included(u)) => f > u.as_str(),
        (Some(f), Bound::Excluded(u)) => f >= u.as_str(),
    };
    if ok {
        Ok(())
    } else {
        Err(SliceError::BadFollowingKey {
            following: following.unwrap().to_owned(),
        })
    }
}
