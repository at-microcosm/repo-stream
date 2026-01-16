use cid::Cid;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
#[error("The CID is not a valid strict atproto SHA256 CID")]
pub struct NotStrictError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CidStrict32([u8; 32]);

pub const ATPROTO_HASHLINK_PREFIX: [u8; 4] = [0x01, 0x71, 0x12, 0x20];

impl TryFrom<&[u8]> for CidStrict32 {
    type Error = NotStrictError;
    fn try_from(raw: &[u8]) -> Result<CidStrict32, Self::Error> {
        let (pre, sha) = raw.split_first_chunk::<4>().ok_or(NotStrictError)?;
        if pre != &ATPROTO_HASHLINK_PREFIX {
            return Err(NotStrictError);
        }
        let inner = sha.try_into().map_err(|_| NotStrictError)?;
        Ok(CidStrict32(inner))
    }
}

impl TryFrom<Cid> for CidStrict32 {
    type Error = NotStrictError;
    fn try_from(cid: Cid) -> Result<CidStrict32, Self::Error> {
        cid.to_bytes().as_slice().try_into()
    }
}

impl From<CidStrict32> for Cid {
    fn from(CidStrict32(sha): CidStrict32) -> Cid {
        let mut bytes = Vec::from(ATPROTO_HASHLINK_PREFIX);
        bytes.extend_from_slice(&sha);
        bytes.try_into().unwrap() // this prefix + sha is always a valid Cid
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectLink {
    Should(CidStrict32),
    Allowed(Box<Cid>),
}

impl ObjectLink {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ObjectLink::Should(CidStrict32(sha)) => {
                let mut bytes = vec![0xFF]; // prefix that's never valid for CID
                bytes.extend_from_slice(sha);
                bytes
            }
            ObjectLink::Allowed(cid) => cid.to_bytes(),
        }
    }
}

impl From<&CidStrict32> for ObjectLink {
    fn from(strict: &CidStrict32) -> ObjectLink {
        ObjectLink::Should(*strict)
    }
}

impl From<Cid> for ObjectLink {
    fn from(cid: Cid) -> ObjectLink {
        if let Ok(strict) = cid.try_into() {
            ObjectLink::Should(strict)
        } else {
            ObjectLink::Allowed(cid.into())
        }
    }
}

impl From<ObjectLink> for Cid {
    fn from(link: ObjectLink) -> Cid {
        match link {
            ObjectLink::Should(strict) => strict.into(),
            ObjectLink::Allowed(boxed) => *boxed,
        }
    }
}

#[derive(Debug, Clone)]
pub enum NodeThing {
    ChildNode(CidStrict32),
    Record(crate::Rkey, ObjectLink),
}

impl NodeThing {
    pub fn link(&self) -> ObjectLink {
        match self {
            Self::ChildNode(strict) => strict.into(),
            Self::Record(_, link) => link.clone(),
        }
    }
}
