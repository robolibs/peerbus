//! Python bindings for quicbit (pyo3).
//!
//! Mirrors the C ABI: opaque byte messages (a `kind: int` tag + `bytes`)
//! over both transports. Classes wrap the high-level `Node` API:
//!
//! ```python
//! import quicbit
//! node = quicbit.Node(identity="rover-a", no_relay=True)
//! pub  = node.publisher("rover/pose")
//! sub  = node.subscriber("rover-a", "rover/pose")
//! pub.send(b"\x01\x02", kind=7)
//! msg = sub.take()            # -> (kind, data) or None
//! ```
//!
//! Talking to a *native* Rust `T` would require matching `T`'s type
//! hash; the bindings use a single opaque wire type ([`crate::RawMsg`])
//! for every node, so Python↔Python and Python↔C interoperate.

use std::time::{Duration, Instant};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};

use crate::{Node, Publisher, RawMsg, ReqClient, ReqServer, Subscriber};

fn py_err(err: crate::Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[pyclass(name = "Node")]
pub struct PyNode {
    node: Node,
}

#[pymethods]
impl PyNode {
    #[new]
    #[pyo3(signature = (identity=None, no_relay=false))]
    fn new(identity: Option<String>, no_relay: bool) -> PyResult<Self> {
        let mut builder = Node::builder();
        if let Some(id) = identity {
            builder = builder.identity(id);
        }
        if no_relay {
            builder = builder.no_relay();
        }
        let node = builder.bind().map_err(py_err)?;
        Ok(Self { node })
    }

    /// This node's identity as a `did:key:z6Mk…` string.
    fn did_key(&self) -> String {
        self.node.endpoint_did_key()
    }

    fn publisher(&self, topic: &str) -> PyResult<PyPublisher> {
        let publisher = self.node.publisher::<RawMsg>(topic).map_err(py_err)?;
        Ok(PyPublisher { publisher })
    }

    fn subscriber(&self, peer: &str, topic: &str) -> PyResult<PySubscriber> {
        let subscriber = self
            .node
            .subscriber::<RawMsg>(peer, topic)
            .map_err(py_err)?;
        Ok(PySubscriber { subscriber })
    }

    fn req_client(&self, peer: &str, topic: &str) -> PyResult<PyReqClient> {
        let client = self
            .node
            .req_client::<RawMsg, RawMsg>(peer, topic)
            .map_err(py_err)?;
        Ok(PyReqClient { client })
    }

    fn req_server(&self, topic: &str) -> PyResult<PyReqServer> {
        let server = self
            .node
            .req_server::<RawMsg, RawMsg>(topic)
            .map_err(py_err)?;
        Ok(PyReqServer { server })
    }
}

#[pyclass(name = "Publisher")]
pub struct PyPublisher {
    publisher: Publisher<RawMsg>,
}

#[pymethods]
impl PyPublisher {
    /// Publish `data` with user tag `kind`.
    #[pyo3(signature = (data, kind=0))]
    fn send(&mut self, data: &[u8], kind: u64) -> PyResult<()> {
        self.publisher
            .send(&RawMsg::new(kind, data))
            .map(|_| ())
            .map_err(py_err)
    }
}

#[pyclass(name = "Subscriber")]
pub struct PySubscriber {
    subscriber: Subscriber<RawMsg>,
}

#[pymethods]
impl PySubscriber {
    /// Poll for the next sample. Returns `(kind, data)` or `None`.
    fn take<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(u64, Bound<'py, PyBytes>)>> {
        match self.subscriber.take().map_err(py_err)? {
            Some(sample) => Ok(Some((
                sample.header().kind,
                PyBytes::new(py, sample.payload()),
            ))),
            None => Ok(None),
        }
    }
}

#[pyclass(name = "ReqClient")]
pub struct PyReqClient {
    client: ReqClient<RawMsg, RawMsg>,
}

#[pymethods]
impl PyReqClient {
    /// Send a request and block for the response `(kind, data)`.
    #[pyo3(signature = (data, kind=0))]
    fn call<'py>(
        &mut self,
        py: Python<'py>,
        data: &[u8],
        kind: u64,
    ) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        let request = RawMsg::new(kind, data);
        // Release the GIL while blocking on the round trip so other
        // Python threads (e.g. a server's serve_one) can run.
        let res = py
            .allow_threads(|| self.client.call(&request))
            .map_err(py_err)?;
        Ok((res.header().kind, PyBytes::new(py, res.payload())))
    }
}

#[pyclass(name = "ReqServer")]
pub struct PyReqServer {
    server: ReqServer<RawMsg, RawMsg>,
}

#[pymethods]
impl PyReqServer {
    /// Serve at most one request within `timeout_ms`. `handler` receives
    /// `(kind, data)` and returns either `bytes` (kind 0) or
    /// `(kind, bytes)`. Returns `True` if a request was served, `False`
    /// on timeout.
    #[pyo3(signature = (handler, timeout_ms=1000))]
    fn serve_one(&mut self, py: Python<'_>, handler: Py<PyAny>, timeout_ms: u64) -> PyResult<bool> {
        // Poll/respond off the GIL so client threads can run; re-acquire
        // the GIL only to invoke the Python handler.
        py.allow_threads(|| {
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            loop {
                match self.server.take().map_err(py_err)? {
                    Some((req, reply)) => {
                        let kind = req.header().kind;
                        let payload = req.payload().to_vec();
                        let response = Python::with_gil(|py| -> PyResult<RawMsg> {
                            let ret = handler.bind(py).call1((kind, PyBytes::new(py, &payload)))?;
                            let (rkind, rdata): (u64, Vec<u8>) =
                                if let Ok(pair) = ret.extract::<(u64, Vec<u8>)>() {
                                    pair
                                } else {
                                    (0, ret.extract::<Vec<u8>>()?)
                                };
                            Ok(RawMsg::new(rkind, &rdata))
                        })?;
                        reply.respond(&response).map_err(py_err)?;
                        return Ok(true);
                    }
                    None => {
                        if Instant::now() >= deadline {
                            return Ok(false);
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
        })
    }
}

pub fn register_python_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyNode>()?;
    module.add_class::<PyPublisher>()?;
    module.add_class::<PySubscriber>()?;
    module.add_class::<PyReqClient>()?;
    module.add_class::<PyReqServer>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[pymodule]
fn quicbit(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_python_module(module)
}
