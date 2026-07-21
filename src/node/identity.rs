use super::*;

#[derive(Debug, Clone)]
pub(crate) enum IdentitySource {
    Ephemeral,
    Name(String),
    File(PathBuf),
    Env(String),
}

impl IdentitySource {
    /// True when the ed25519 secret key is derived from a
    /// human-readable string (`identity` / `identity_env`), i.e. the
    /// string *is* the key material and the peer is impersonable by
    /// anyone who knows it.
    pub(crate) fn is_derived_from_string(&self) -> bool {
        matches!(self, IdentitySource::Name(_) | IdentitySource::Env(_))
    }
}

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

// ---- peer addressing ----

pub(crate) fn derive_secret_from_name(name: &str) -> SecretKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(IDENTITY_DERIVATION_TAG);
    hasher.update(b"\0");
    hasher.update(name.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(hasher.finalize().as_bytes());
    SecretKey::from_bytes(&out)
}

pub(crate) fn resolve_identity(src: &IdentitySource) -> Result<(SecretKey, Option<String>)> {
    match src {
        IdentitySource::Ephemeral => Ok((SecretKey::generate(), None)),
        IdentitySource::Name(name) => Ok((derive_secret_from_name(name), Some(name.clone()))),
        IdentitySource::Env(var) => {
            let name = std::env::var(var).map_err(|_| {
                Error::invalid_argument(format!("identity_env: env var '{var}' is not set"))
            })?;
            if name.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "identity_env: '{var}' is empty"
                )));
            }
            Ok((derive_secret_from_name(&name), Some(name)))
        }
        IdentitySource::File(path) => {
            let key = load_or_generate_key(path)?;
            Ok((key, None))
        }
    }
}

pub(crate) fn load_or_generate_key(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let mut buf = [0u8; 32];
        let mut f =
            fs::File::open(path).map_err(|e| Error::Other(format!("open key file: {e}")))?;
        f.read_exact(&mut buf)
            .map_err(|e| Error::Other(format!("read key file: {e}")))?;
        // Re-enforce 0600 on every read. If an operator copied the
        // file without preserving mode, the next bind corrects it
        // and logs a warning. Failures (mounted read-only, foreign
        // FS) downgrade to a warn so the bind doesn't refuse on
        // pre-existing keys.
        enforce_key_perms(path);
        Ok(SecretKey::from_bytes(&buf))
    } else {
        let key = SecretKey::generate();
        let bytes: [u8; 32] = key.to_bytes();
        let mut f =
            fs::File::create(path).map_err(|e| Error::Other(format!("create key file: {e}")))?;
        f.write_all(&bytes)
            .map_err(|e| Error::Other(format!("write key file: {e}")))?;
        enforce_key_perms(path);
        Ok(key)
    }
}

#[cfg(unix)]
pub(crate) fn enforce_key_perms(path: &Path) {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let needs_chmod = match fs::metadata(path) {
        Ok(meta) => (meta.mode() & 0o777) != 0o600,
        Err(_) => true,
    };
    if needs_chmod {
        if let Err(e) = fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            let _ = &e;
            qb_warn!(
                target: "peerbus::node",
                path = %path.display(),
                error = %e,
                "could not enforce 0600 on identity key file; check permissions manually"
            );
        }
    }
}

#[cfg(not(unix))]
pub(crate) fn enforce_key_perms(_path: &Path) {}

// ---- service-name composition ----
