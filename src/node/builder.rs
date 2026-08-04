use super::*;

pub struct NodeBuilder {
    /// The ed25519 key this node binds its iroh endpoint with. peerbus
    /// only *consumes* a key; producing / naming / persisting keys is
    /// the higher-level crate's job. `None` until `secret_key` /
    /// `ephemeral` is called; `bind` errors if still `None`.
    pub(crate) secret: Option<SecretKey>,
    /// Optional cosmetic label used only in logs — never in routing or
    /// the shared-memory rendezvous name.
    pub(crate) label: Option<String>,
    pub(crate) alpn: Vec<u8>,
    pub(crate) no_relay: bool,
    /// Disable the local shared-memory fast path completely. Publishers and
    /// servers do not create SHM services, and subscribers and clients always
    /// dial the addressed peer through iroh, including same-host peers.
    pub(crate) skip_shm: bool,
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
    /// Bind with this exact ed25519 key. Its public half is the node's
    /// `EndpointId` — the single id used for both iroh dialing and the
    /// shared-memory rendezvous name. Required (unless [`ephemeral`] is
    /// used).
    ///
    /// [`ephemeral`]: Self::ephemeral
    pub fn secret_key(mut self, key: SecretKey) -> Self {
        self.secret = Some(key);
        self
    }

    /// Bind with a fresh random key. Convenience for tests, examples,
    /// and short-lived nodes whose id need not persist across runs.
    pub fn ephemeral(mut self) -> Self {
        self.secret = Some(SecretKey::generate());
        self
    }

    /// Optional human-readable label for this node. Used **only** in log
    /// lines to make a node recognisable; it is never part of routing or
    /// the shared-memory rendezvous name (those key off the id).
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
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

    /// Disable shared memory for this node and force all messaging through
    /// iroh, even when the addressed peer is on the same host.
    ///
    /// This is useful for exercising the real iroh path in local tests and for
    /// applications that require transport behavior to be independent of
    /// host locality. Publishers and servers created by this node do not
    /// create SHM services; subscribers and clients do not probe for them.
    pub fn skip_shm(mut self) -> Self {
        self.skip_shm = true;
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
    /// Requires a key ([`secret_key`](Self::secret_key) or
    /// [`ephemeral`](Self::ephemeral)). Warns loudly on the one
    /// insecure-but-supported posture: [`allow_any_peer`](Self::allow_any_peer)
    /// (no peer ACL).
    pub fn bind(self) -> Result<Node> {
        let rt = runtime::shared()?;
        let secret = self.secret.ok_or_else(|| {
            Error::invalid_argument(
                "Node::builder: no key set; call .secret_key(<SecretKey>) or .ephemeral()",
            )
        })?;
        let endpoint_id = secret.public();
        let secret_bytes = secret.to_bytes();
        let label = self.label;
        let alpn = self.alpn.clone();
        let no_relay = self.no_relay;
        let skip_shm = self.skip_shm;

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
            label = label.as_deref().unwrap_or("<none>"),
            no_relay,
            skip_shm,
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

        let inner = Arc::new(NodeInner {
            endpoint,
            endpoint_id,
            label,
            alpn: self.alpn,
            local_cfg: self.local_cfg,
            skip_shm,
            publisher_topics: Mutex::new(HashMap::new()),
            request_topics: Mutex::new(HashMap::new()),
            que_topics: Mutex::new(HashMap::new()),
            put_topics: Mutex::new(HashMap::new()),
            pip_topics: Mutex::new(HashMap::new()),
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
