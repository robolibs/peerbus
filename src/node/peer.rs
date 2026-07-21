use super::*;

/// A peer's address. We need an iroh `EndpointId` for the remote
/// path. The optional `name` lets us route same-host without any
/// discovery service: both publisher and subscriber name the
/// service the same way → local service open succeeds → SHM.
#[derive(Clone, Debug)]
pub struct Peer {
    pub endpoint_id: EndpointId,
    pub name: Option<String>,
    /// Full transport address. When set, used as the dial target;
    /// otherwise the node falls back to `EndpointAddr::new(id)`,
    /// which requires DNS / relay discovery.
    pub addr: Option<EndpointAddr>,
}

/// `&str` / `String` hashes to the same deterministic `EndpointId`
/// as [`NodeBuilder::identity`]. `EndpointId` and `EndpointAddr`
/// pass through.
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
            name: None,
            addr: None,
        }
    }
}

impl IntoPeer for EndpointAddr {
    fn into_peer(self) -> Peer {
        Peer {
            endpoint_id: self.id,
            name: None,
            addr: Some(self),
        }
    }
}

impl IntoPeer for &str {
    fn into_peer(self) -> Peer {
        // `did:key:z…` strings carry the literal ed25519 public key
        // (no derivation) and are routed locally by EndpointId
        // rather than identity name. Bare strings still hash to a
        // deterministic key by name for the trusted-LAN path.
        if crate::did_key::looks_like_did_key(self)
            && let Ok(id) = crate::did_key::did_key_to_endpoint_id(self)
        {
            return Peer {
                endpoint_id: id,
                name: None,
                addr: None,
            };
        }
        let secret = derive_secret_from_name(self);
        Peer {
            endpoint_id: secret.public(),
            name: Some(self.to_string()),
            addr: None,
        }
    }
}

impl IntoPeer for String {
    fn into_peer(self) -> Peer {
        if crate::did_key::looks_like_did_key(&self)
            && let Ok(id) = crate::did_key::did_key_to_endpoint_id(&self)
        {
            return Peer {
                endpoint_id: id,
                name: None,
                addr: None,
            };
        }
        let secret = derive_secret_from_name(&self);
        Peer {
            endpoint_id: secret.public(),
            name: Some(self),
            addr: None,
        }
    }
}

// ---- builder ----
