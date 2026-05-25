//! `did:key` <-> iroh `EndpointId` adapter.
//!
//! Iroh's `EndpointId` is a 32-byte Ed25519 public key. The W3C
//! `did:key` method for Ed25519 wraps the same 32 bytes in
//! `did:key:z<base58btc(0xed 0x01 || pub32)>`. Conversion is pure
//! string formatting — no cryptography, no key derivation.
//!
//! Encode/parse is delegated to [`authbox::did`] so the wire format
//! stays in lockstep with the rest of the robolibs DID surface.

use iroh::EndpointId;

use crate::error::{Error, Result};

/// Prefix every Ed25519 `did:key` URI starts with.
pub const DID_KEY_PREFIX: &str = "did:key:";

/// Encode an iroh `EndpointId` as a `did:key:z6Mk…` URI.
pub fn endpoint_id_to_did_key(id: &EndpointId) -> String {
    // `authbox::did::encode_ed25519_did_key` only errors on
    // unsupported key types; we pass an Ed25519 key by construction.
    authbox::did::encode_ed25519_did_key(*id.as_bytes())
        .expect("ed25519 did:key encoding cannot fail for a 32-byte key")
}

/// Parse a `did:key:z…` URI into an iroh `EndpointId`. Rejects
/// non-Ed25519 key types (e.g. X25519 `did:key:z6LS…`) since iroh's
/// addressing requires an Ed25519 verifying key.
pub fn did_key_to_endpoint_id(did_uri: &str) -> Result<EndpointId> {
    let info = authbox::did::parse_did_key(did_uri)
        .map_err(|e| Error::invalid_argument(format!("invalid did:key '{did_uri}': {e}")))?;
    if info.type_ != authbox::did::DidKeyType::Ed25519 {
        return Err(Error::invalid_argument(format!(
            "did:key '{did_uri}' is not Ed25519; iroh EndpointIds require Ed25519"
        )));
    }
    let pub32: [u8; 32] = info.public_key.as_slice().try_into().map_err(|_| {
        Error::invalid_argument(format!(
            "did:key '{did_uri}' has {}-byte payload; expected 32",
            info.public_key.len()
        ))
    })?;
    EndpointId::from_bytes(&pub32)
        .map_err(|e| Error::invalid_argument(format!("did:key '{did_uri}' is not a valid ed25519 point: {e}")))
}

/// Cheap heuristic check used by `IntoPeer` before falling back to
/// the deterministic-name-hash path.
pub fn looks_like_did_key(s: &str) -> bool {
    s.starts_with(DID_KEY_PREFIX)
}
