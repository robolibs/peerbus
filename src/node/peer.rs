use super::*;

/// A peer's address. peerbus addresses peers **only by their 32-byte
/// `EndpointId`** — that id is both the iroh dial target and the seed
/// for the shared-memory rendezvous name. Friendly names and `did:key`
/// strings live in the higher-level crate, which resolves them to an
/// `EndpointId` before calling in here.
#[derive(Clone, Debug)]
pub struct Peer {
    pub endpoint_id: EndpointId,
    /// Full transport address. When set, used as the dial target;
    /// otherwise the node falls back to `EndpointAddr::new(id)`, which
    /// requires DNS / relay discovery.
    pub addr: Option<EndpointAddr>,
}

/// Anything that names a peer by its id. `EndpointId` and `EndpointAddr`
/// pass through directly; there is no string form (that is the higher
/// crate's job).
pub trait IntoPeer {
    fn into_peer(self) -> Peer;
}

impl IntoPeer for Peer {
    fn into_peer(self) -> Peer {
        self
    }
}

impl IntoPeer for EndpointId {
    fn into_peer(self) -> Peer {
        Peer {
            endpoint_id: self,
            addr: None,
        }
    }
}

impl IntoPeer for EndpointAddr {
    fn into_peer(self) -> Peer {
        Peer {
            endpoint_id: self.id,
            addr: Some(self),
        }
    }
}
