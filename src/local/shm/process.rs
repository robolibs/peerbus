use super::*;


pub(crate) fn validate_config(cfg: &LocalConfig) -> Result<()> {
    if cfg.max_publishers as usize > MAX_TRACKED_PUBLISHERS {
        return Err(Error::invalid_argument(format!(
            "max_publishers {} exceeds tracked-process cap {}",
            cfg.max_publishers, MAX_TRACKED_PUBLISHERS
        )));
    }
    if cfg.max_subscribers as usize > MAX_TRACKED_SUBSCRIBERS {
        return Err(Error::invalid_argument(format!(
            "max_subscribers {} exceeds tracked-process cap {}",
            cfg.max_subscribers, MAX_TRACKED_SUBSCRIBERS
        )));
    }
    Ok(())
}

pub(crate) fn decrement_counter(counter: &AtomicU32) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_sub(1))
    });
}

pub(crate) fn current_pid() -> u32 {
    std::process::id()
}

pub(crate) fn current_process_token() -> u64 {
    process_start_token(current_pid()).unwrap_or(0)
}

/// Read a process' start-time token from `/proc/<pid>/stat`, distinguishing
/// "the process is gone" (`NotFound`) from "we couldn't read it right now"
/// (any other error — e.g. `EMFILE`/`ENFILE` fd exhaustion, `EACCES`, a
/// malformed line). The distinction is load-bearing: mapping a *transient*
/// read failure to "dead" would let a fully-alive process be reaped or, worse,
/// its half-initialised segment condemned (the split-brain trigger).
#[cfg(target_os = "linux")]
pub(crate) fn read_start_token(pid: u32) -> std::io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let after_comm = stat.rsplit_once(") ").ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed /proc stat")
    })?;
    // Field 22 (`starttime`) is index 19 after stripping fields 1 and 2
    // (`pid` and `comm`). See `proc_pid_stat(5)`.
    after_comm
        .1
        .split_whitespace()
        .nth(19)
        .and_then(|f| f.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "missing starttime field")
        })
}

#[cfg(target_os = "linux")]
pub(crate) fn process_start_token(pid: u32) -> Option<u64> {
    read_start_token(pid).ok()
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(crate) fn process_start_token(_pid: u32) -> Option<u64> {
    None
}

#[cfg(not(unix))]
pub(crate) fn process_start_token(_pid: u32) -> Option<u64> {
    None
}

#[cfg(unix)]
pub(crate) fn process_alive(pid: u32, token: u64) -> bool {
    // A value that cannot be a valid PID is never a live process — and must
    // NEVER reach `kill()`, whose non-positive arguments mean "signal a process
    // group / every process" (`0`, `-1`, `< -1`). A `pid` of 0 or one that
    // would be negative as `pid_t` (high bit set — e.g. corruption, or the
    // `u32::MAX` sentinel older peers stored) is therefore treated as dead.
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    if token != 0 {
        #[cfg(target_os = "linux")]
        match read_start_token(pid) {
            // Definitive: the token either matches this incarnation or a
            // different process reused the pid (mismatch => original is dead).
            Ok(observed) => return observed == token,
            // The process is genuinely gone.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
            // Inconclusive (fd exhaustion, permissions, parse): never conclude
            // "dead" from a transient failure — fall through to the robust
            // `kill(pid, 0)` probe below, which errs toward "alive".
            Err(_) => {}
        }
    }
    // Untokened, or an inconclusive token read: ask the kernel directly.
    // SAFETY: `kill(pid, 0)` does not deliver a signal; it only asks whether
    // the process exists and is visible to us.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    matches!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(not(unix))]
pub(crate) fn process_alive(pid: u32, token: u64) -> bool {
    // Conservative fallback for platforms where this module has not grown a
    // native liveness probe yet: never reap another process' slots.
    pid == current_pid() && (token == 0 || token == current_process_token())
}
