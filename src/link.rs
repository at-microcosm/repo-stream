use cid::Cid;

#[derive(Debug, serde::Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ObjectLink(Cid);

impl ObjectLink {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }
}

impl From<Cid> for ObjectLink {
    fn from(cid: Cid) -> ObjectLink {
        ObjectLink(cid)
    }
}

impl From<ObjectLink> for Cid {
    fn from(link: ObjectLink) -> Cid {
        link.0
    }
}

#[derive(Debug, Clone)]
pub struct NodeThing {
    pub link: ObjectLink,
    pub kind: ThingKind,
}

impl NodeThing {
    pub fn is_record(&self) -> bool {
        match self.kind {
            ThingKind::ChildNode => false,
            ThingKind::Record(_) => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ThingKind {
    ChildNode,
    Record(crate::Rkey),
}
