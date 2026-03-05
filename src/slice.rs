//! Proven-range walking of CAR slices

use crate::{
    RepoPath,
    mem::MemCar,
    walk::{Output, WalkError, WalkItem},
};
use cid::Cid;
use std::ops::{Bound, RangeBounds};

/// Errors from [`MemCar::walk_slice`] and friends
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

/// Iterator-like walker over a proven range of the MST.
///
/// Created by [`MemCar::walk_slice`] and related methods. Call
/// [`SliceWalker::next`] to yield records; proof validation runs
/// automatically before `next` returns `Ok(None)`.
pub struct SliceWalker<'a> {
    mem_car: &'a mut MemCar,
    upper: Bound<String>,
    preceding_key: Option<RepoPath>,
    following_key: Option<RepoPath>,
    /// First in-range item found during construction, buffered for the first `next()` call.
    buffered: Option<Output>,
    done: bool,
}

impl<'a> SliceWalker<'a> {
    /// Walk to the lower bound, establishing `preceding_key` from boundary items.
    ///
    /// Consumes all pre-range items here so that `next` only ever sees
    /// in-range or post-range items.
    ///
    /// We walk rather than seek so that boundary nodes are fully visited and
    /// `preceding_key` is set correctly for CAR-slice proofs. (Walker::seek's
    /// SkipSubtree optimisation would skip the boundary node whose MissingRecord
    /// entry carries the preceding key.)
    fn new(
        mem_car: &'a mut MemCar,
        lower: Bound<String>,
        upper: Bound<String>,
    ) -> Result<Self, SliceError> {
        let mut preceding_key = None;
        let mut following_key = None;
        let mut buffered = None;
        let mut done = false;

        loop {
            match mem_car.next()? {
                None => {
                    done = true;
                    break;
                }
                Some(WalkItem::MissingSubtree { .. }) => {
                    // Boundary subtree entirely before the range — safe to skip.
                }
                Some(WalkItem::MissingRecord { key, cid }) => {
                    if is_before(&key, &lower) {
                        preceding_key = Some(key);
                    } else if is_after(&key, &upper) {
                        following_key = Some(key);
                        done = true;
                        break;
                    } else {
                        return Err(SliceError::IncompleteRange { key, cid });
                    }
                }
                Some(WalkItem::Record(out)) => {
                    if is_before(&out.key, &lower) {
                        preceding_key = Some(out.key);
                    } else if is_after(&out.key, &upper) {
                        following_key = Some(out.key);
                        done = true;
                        break;
                    } else {
                        buffered = Some(out);
                        break;
                    }
                }
            }
        }

        validate_lower(preceding_key.as_deref(), &lower)?;
        if done {
            validate_upper(following_key.as_deref(), &upper)?;
        }

        Ok(Self {
            mem_car,
            upper,
            preceding_key,
            following_key,
            buffered,
            done,
        })
    }

    /// Yield the next in-range record.
    ///
    /// Returns `Ok(None)` when the range is exhausted — proof validation runs
    /// automatically before returning `None`, so the `while let` pattern
    /// is sufficient and safe:
    ///
    /// ```ignore
    /// while let Some(output) = walker.next()? { ... }
    /// // any proof violation surfaced as Err before None was returned
    /// ```
    ///
    /// Errors on any missing block within the range, on an MST node absent
    /// within the range, or on a proof violation.
    pub fn next(&mut self) -> Result<Option<Output>, SliceError> {
        if self.done {
            return Ok(None);
        }

        if let Some(out) = self.buffered.take() {
            return Ok(Some(out));
        }

        match self.mem_car.next()? {
            None => {
                self.done = true;
                validate_upper(self.following_key.as_deref(), &self.upper)?;
                Ok(None)
            }
            Some(WalkItem::MissingSubtree { cid }) => {
                // Any missing subtree after the range starts is an error:
                // we can't prove the range is complete without it.
                Err(SliceError::MissingNode { cid })
            }
            Some(WalkItem::MissingRecord { key, cid }) => {
                if is_after(&key, &self.upper) {
                    self.following_key = Some(key);
                    self.done = true;
                    validate_upper(self.following_key.as_deref(), &self.upper)?;
                    Ok(None)
                } else {
                    Err(SliceError::IncompleteRange { key, cid })
                }
            }
            Some(WalkItem::Record(out)) => {
                if is_after(&out.key, &self.upper) {
                    self.following_key = Some(out.key);
                    self.done = true;
                    validate_upper(self.following_key.as_deref(), &self.upper)?;
                    Ok(None)
                } else {
                    Ok(Some(out))
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
    /// order. Proof validation runs automatically when `next` returns `None`.
    ///
    /// Accepts standard Rust range expressions:
    /// - `"a".."b"` — exclusive upper bound
    /// - `"a"..="b"` — inclusive upper bound
    /// - `"a"..` — from `a` to end of tree
    /// - `.."b"` — from start of tree to just before `b`
    /// - `..` — entire tree (equivalent to [`full`](MemCar::full))
    pub fn walk_slice<'r>(
        &mut self,
        range: impl RangeBounds<&'r str>,
    ) -> Result<SliceWalker<'_>, SliceError> {
        let lower = bound_to_owned(range.start_bound());
        let upper = bound_to_owned(range.end_bound());
        SliceWalker::new(self, lower, upper)
    }

    /// Walk the entire MST, proving that no records are missing.
    pub fn full(&mut self) -> Result<SliceWalker<'_>, SliceError> {
        SliceWalker::new(self, Bound::Unbounded, Bound::Unbounded)
    }

    /// Walk all records whose key starts with `pre`, proving the range is complete.
    ///
    /// The exclusive upper bound is computed by incrementing the last character
    /// of `pre`, so all keys with that prefix — and only those keys — are in range.
    pub fn prefix(&mut self, pre: &str) -> Result<SliceWalker<'_>, SliceError> {
        let lower = Bound::Included(pre.to_owned());
        let upper = prefix_upper(pre);
        SliceWalker::new(self, lower, upper)
    }

    /// Fetch a single record by exact key, proving its presence or absence.
    ///
    /// - `Ok(Some(output))` — record is present
    /// - `Ok(None)` — record is provably absent (adjacent MST keys bound it)
    /// - `Err(SliceError::IncompleteRange)` — the MST has an entry for this
    ///   key but the block is absent; absence cannot be proven
    /// - Other `Err` variants for MST structural issues
    pub fn get(&mut self, key: &str) -> Result<Option<Output>, SliceError> {
        let mut walker = SliceWalker::new(
            self,
            Bound::Included(key.to_owned()),
            Bound::Included(key.to_owned()),
        )?;
        let record = walker.next()?;
        walker.finish()?;
        Ok(record)
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

/// Compute the exclusive upper bound for a prefix: the smallest string that
/// does not start with `pre`. Found by incrementing the last character.
fn prefix_upper(pre: &str) -> Bound<String> {
    let mut s = pre.to_owned();
    while let Some(last) = s.chars().next_back() {
        s.pop();
        if let Some(next) = char::from_u32(last as u32 + 1) {
            s.push(next);
            return Bound::Excluded(s);
        }
        // last char was U+10FFFF; try the previous one
    }
    Bound::Unbounded // pre was empty or all U+10FFFF
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
