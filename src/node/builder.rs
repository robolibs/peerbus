use super::*;

pub struct NodeBuilder {
    pub(crate) identity: IdentitySource,
    pub(crate) system_did: Option<String>,
    pub(crate) alpn: Vec<u8>,
    pub(crate) no_relay: bool,
    pub(crate) local_cfg: LocalConfig,
    /// Allowlist of peers permitted to open inbound connections.
    /// `None` means "no allowlist configured", which — unless
    /// [`NodeBuilder::allow_any_peer`] is set — denies every inbound
    /// peer.
    pub(crate) allowed_peers: Option<HashSet<[u8; 32]>>,
    /// Explicit opt-out of the deny-by-default inbound policy. See
    /// [`NodeBuilder::allow_any_peer`].
    pub(crate) allow_any_peer: bool,
}

impl NodeBuilder {
    /// Join a logical multi-process system namespace identified by
    /// a `did:key:z...` URI. In system mode, high-level
    /// [`Node::publisher`] / [`Node::subscribe`] routes are keyed by
    /// `(system_did, topic)` instead of this process' transport id.
    pub fn system_did(mut self, did: impl Into<String>) -> Self {
        self.system_did = Some(did.into());
        self
    }

    /// Literal name → deterministic `SecretKey` via blake3.
    /// Same name on two machines → same `EndpointId`.
    ///
    /// # Security: this identity is impersonable
    ///
    /// **The identity string *is* the private key material.** The
    /// 32-byte ed25519 secret key is a domain-separated blake3 hash of
    /// `name` and nothing else — no salt, no local entropy. Anyone who
    /// learns the string (from a config file, a log line, a `ps`
    /// listing, a screenshot, a git history) can recompute this node's
    /// secret key and impersonate it to every peer that allowlists it.
    ///
    /// Use this only on a trusted network, for local development, or
    /// in tests. On production / untrusted networks use
    /// [`identity_file`](Self::identity_file), which stores 32 random
    /// bytes on disk with `0600` and is *not* derivable from any
    /// human-readable name.
    ///
    /// [`bind`](Self::bind) emits a `WARN` whenever this path is used.
    pub fn identity(mut self, name: impl Into<String>) -> Self {
        self.identity = IdentitySource::Name(name.into());
        self
    }

    /// Read 32 raw bytes as the `SecretKey`; generate + write on
    /// first run (mode `0600`). Cryptographically meaningful; this is
    /// the production knob, and the only identity source that is not
    /// derivable from a guessable string.
    pub fn identity_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.identity = IdentitySource::File(path.into());
        self
    }

    /// Read the env var `var` and feed its value through
    /// [`Self::identity`].
    ///
    /// # Security: this identity is impersonable
    ///
    /// Inherits every weakness of [`identity`](Self::identity): the
    /// env var's *value* is hashed straight into the ed25519 secret
    /// key, so anyone who knows the value can impersonate this peer.
    /// An env var is not a secret store — it leaks through `ps`,
    /// `/proc/<pid>/environ`, CI logs and crash dumps. Prefer
    /// [`identity_file`](Self::identity_file) in production.
    ///
    /// [`bind`](Self::bind) emits a `WARN` whenever this path is used.
    pub fn identity_env(mut self, var: impl Into<String>) -> Self {
        self.identity = IdentitySource::Env(var.into());
        self
    }

    pub fn alpn(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.alpn = alpn.into();
        self
    }

    /// Add `peer` to the inbound allowlist.
    ///
    /// Inbound connections are **denied by default**: a node that
    /// calls neither `allow_peer` / [`allow_peers`](Self::allow_peers)
    /// nor [`allow_any_peer`](Self::allow_any_peer) refuses every
    /// incoming connection. Each call extends the allowlist.
    ///
    /// Only inbound accepts are affected. Connections this node dials
    /// *out* (`subscriber(...)`, `req_client(...)`, …) are not
    /// filtered — the peer we dial decides whether to accept us.
    pub fn allow_peer(mut self, peer: impl IntoPeer) -> Self {
        let p = peer.into_peer();
        self.allowed_peers
            .get_or_insert_with(std::collections::HashSet::new)
            .insert(*p.endpoint_id.as_bytes());
        self
    }

    /// Add many peers to the inbound allowlist at once. See
    /// [`allow_peer`](Self::allow_peer).
    pub fn allow_peers<I, P>(mut self, peers: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: IntoPeer,
    {
        let set = self
            .allowed_peers
            .get_or_insert_with(std::collections::HashSet::new);
        for p in peers {
            set.insert(*p.into_peer().endpoint_id.as_bytes());
        }
        self
    }

    /// Disable the peer allowlist entirely: **accept connections from
    /// any peer that knows the ALPN.**
    ///
    /// # Security
    ///
    /// This is an explicit opt-out of peerbus' deny-by-default inbound
    /// policy. With it set, *any* process that can reach this node's
    /// UDP socket and speaks the ALPN (`peerbus/1` unless changed via
    /// [`alpn`](Self::alpn)) may connect, subscribe to every topic this
    /// node publishes, and call every req/res, que/ans, put/ack and pip
    /// server it hosts. The ALPN is not a secret — it is sent in the
    /// clear in the TLS ClientHello.
    ///
    /// Only appropriate on a trusted network (an isolated robot LAN, a
    /// loopback-only test, a container network with no external
    /// ingress). On anything else, enumerate the peers with
    /// [`allow_peer`](Self::allow_peer) instead.
    ///
    /// [`bind`](Self::bind) emits a `WARN` whenever this is in effect.
    /// It takes precedence over any allowlist configured with
    /// [`allow_peer`](Self::allow_peer).
    pub fn allow_any_peer(mut self) -> Self {
        self.allow_any_peer = true;
        self
    }

    pub fn no_relay(mut self) -> Self {
        self.no_relay = true;
        self
    }

    /// Tune the default local SHM QoS used for publishers / subscribers
    /// this node creates. Per-`publisher`/`subscriber` overrides are
    /// not exposed yet.
    pub fn local_config(mut self, cfg: LocalConfig) -> Self {
        self.local_cfg = cfg;
        self
    }

    /// Bind the node's endpoint and start the inbound accept loop.
    ///
    /// Warns loudly on the two insecure-but-supported postures:
    /// [`allow_any_peer`](Self::allow_any_peer) (no peer ACL) and a
    /// string-derived, impersonable identity
    /// ([`identity`](Self::identity) / [`identity_env`](Self::identity_env)).
    pub fn bind(self) -> Result<Node> {
        let rt = runtime::shared()?;
        let identity_is_derived = self.identity.is_derived_from_string();
        let (secret, identity_name) = resolve_identity(&self.identity)?;
        let system_did = self
            .system_did
            .map(|did| validate_system_did(&did).map(|_| did))
            .transpose()?;
        let endpoint_id = secret.public();
        let secret_bytes = secret.to_bytes();
        let alpn = self.alpn.clone();
        let no_relay = self.no_relay;

        let endpoint: Endpoint = {
            const BIND_ATTEMPTS: usize = 80;
            let mut last_bind_err = None;
            let mut bound = None;
            for attempt in 0..BIND_ATTEMPTS {
                let secret = SecretKey::from_bytes(&secret_bytes);
                let alpn = alpn.clone();
                let bind_result = rt.block_on(async move {
                    let builder = if no_relay {
                        Endpoint::builder(presets::N0DisableRelay)
                    } else {
                        Endpoint::builder(presets::N0)
                    };
                    builder
                        .secret_key(secret)
                        .alpns(vec![alpn])
                        .bind()
                        .await
                        .map_err(|e| Error::Remote(format!("Node::bind: {e}")))
                });
                match bind_result {
                    Ok(endpoint) => {
                        bound = Some(endpoint);
                        break;
                    }
                    Err(err) => {
                        last_bind_err = Some(err);
                        if attempt + 1 < BIND_ATTEMPTS {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
            bound.ok_or_else(|| last_bind_err.expect("bind loop always records an error"))?
        };

        // Deny-by-default. An explicit `.allow_any_peer()` wins over a
        // configured allowlist (it is the louder, more explicit knob);
        // an allowlist alone restricts to those peers; neither means
        // "refuse everyone".
        let inbound_policy = match (self.allow_any_peer, self.allowed_peers) {
            (true, _) => InboundPolicy::AnyPeer,
            (false, Some(set)) => InboundPolicy::Allowlist(set),
            (false, None) => InboundPolicy::DenyAll,
        };

        qb_info!(
            target: "peerbus::node",
            endpoint_id = %endpoint_id,
            identity = identity_name.as_deref().unwrap_or("<ephemeral>"),
            no_relay,
            "node bound"
        );

        if matches!(inbound_policy, InboundPolicy::AnyPeer) {
            qb_warn!(
                target: "peerbus::node",
                endpoint_id = %endpoint_id,
                "INSECURE: allow_any_peer() is in effect — this node accepts \
                 connections from ANY peer that knows the ALPN, which can then \
                 subscribe to every topic and call every server it hosts. The \
                 ALPN is not a secret. Only appropriate on a trusted network; \
                 use Node::builder().allow_peer(<endpoint id>) otherwise"
            );
        }

        if identity_is_derived {
            qb_warn!(
                target: "peerbus::node",
                endpoint_id = %endpoint_id,
                identity = identity_name.as_deref().unwrap_or("<unknown>"),
                "INSECURE identity: this node's ed25519 secret key is derived \
                 from the identity string, so the string IS the key material. \
                 Anyone who learns it can derive this key and impersonate this \
                 peer. Use Node::builder().identity_file(<path>) on production \
                 or untrusted networks"
            );
        }

        let inner = Arc::new(NodeInner {
            endpoint,
            endpoint_id,
            identity_name,
            system_did,
            alpn: self.alpn,
            local_cfg: self.local_cfg,
            publisher_topics: Mutex::new(HashMap::new()),
            request_topics: Mutex::new(HashMap::new()),
            que_topics: Mutex::new(HashMap::new()),
            put_topics: Mutex::new(HashMap::new()),
            pip_topics: Mutex::new(HashMap::new()),
            system_routes: Mutex::new(HashMap::new()),
            system_peers: Mutex::new(Vec::new()),
            peer_connections: Mutex::new(HashMap::new()),
            pubsub_datagram_routes: Mutex::new(HashMap::new()),
            pubsub_datagram_readers: Mutex::new(HashMap::new()),
            inbound_policy,
            rt: rt.clone(),
            accept_handle: Mutex::new(None),
            closed: AtomicBool::new(false),
        });

        let accept_handle = {
            let inner = inner.clone();
            rt.spawn(async move {
                let _ = run_accept_loop(inner).await;
            })
        };
        *crate::trace::recover_poison(inner.accept_handle.lock(), "Node::accept_handle") =
            Some(accept_handle);

        Ok(Node { inner, rt })
    }
}
