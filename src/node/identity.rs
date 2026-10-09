use super::*;

// ---- inbound peer policy ----

/// Who is allowed to open an inbound connection to this node.
///
/// The default is [`InboundPolicy::DenyAll`]: a node that configures
/// neither [`NodeBuilder::allow_peer`] nor
/// [`NodeBuilder::allow_any_peer`] refuses every incoming connection.
#[derive(Debug, Clone)]
pub(crate) enum InboundPolicy {
    /// Deny every inbound peer. The secure default.
    DenyAll,
    /// Accept only these endpoint ids.
    Allowlist(HashSet<[u8; 32]>),
    /// Accept anybody who speaks the ALPN. Explicit opt-in via
    /// [`NodeBuilder::allow_any_peer`].
    AnyPeer,
}

impl InboundPolicy {
    pub(crate) fn allows(&self, remote: &EndpointId) -> bool {
        match self {
            InboundPolicy::DenyAll => false,
            InboundPolicy::Allowlist(set) => set.contains(remote.as_bytes()),
            InboundPolicy::AnyPeer => true,
        }
    }

    /// Human-readable reason for a rejection, used in the warn log and
    /// in the error surfaced to the accept loop.
    pub(crate) fn reject_reason(&self) -> &'static str {
        match self {
            InboundPolicy::DenyAll => {
                "no inbound peers configured (deny-by-default): call \
                 .allow_peer(<id>) or .allow_any_peer() on Node::builder()"
            }
            InboundPolicy::Allowlist(_) => "peer not in allowlist",
            // Unreachable in practice: `AnyPeer` never rejects.
            InboundPolicy::AnyPeer => "peer rejected",
        }
    }
}
