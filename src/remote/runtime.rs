//! Internal multi-threaded tokio runtime that drives iroh.
//!
//! iroh requires a tokio runtime; we own one so the crate's
//! blocking core API stays runtime-agnostic. Lazily initialized,
//! shared by all [`crate::remote::RemoteTransport`]s in the
//! process via an `Arc`.

use std::sync::Arc;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

use crate::error::Error;

static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

/// Acquire the shared runtime, building it on first call.
pub fn shared() -> Result<Arc<Runtime>, Error> {
    if let Some(rt) = RUNTIME.get() {
        return Ok(rt.clone());
    }
    let rt = Builder::new_multi_thread()
        .enable_all()
        .thread_name("quicbit-iroh")
        .build()
        .map_err(|e| Error::Remote(format!("tokio runtime: {e}")))?;
    let arc = Arc::new(rt);
    Ok(RUNTIME.get_or_init(|| arc).clone())
}
