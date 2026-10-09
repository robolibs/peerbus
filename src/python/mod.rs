//! Python bindings for peerbus (pyo3).
//!
//! The binding surface intentionally transports opaque byte messages:
//! a `kind: int` tag plus `bytes`, carried by [`crate::RawMsg`]. Concrete
//! datapod Python classes from datapod 0.4 can sit above this by using
//! `kind = TYPE_HASH` and their own wire bytes.
//!
//! Module layout (submodules share these imports via `use super::*`):
//! * [`convert`] — Python <-> Rust value conversion helpers.
//! * [`types`] — `DeliveryPolicy`, `TopicQos`, and message wrappers.
//! * [`node`] — the `Node` class (endpoint + factory methods).
//! * [`pubsub`] / [`reqres`] / [`queans`] — per-mode raw + datapod classes.
//! * [`putack`] / [`datapod_putack`] — put/ack clients and servers.
//! * [`pip`] / [`datapod_pip`] — bidirectional pip clients and servers.

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyBufferError, PyIndexError, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyMemoryView, PyModule, PyTuple};

use crate::{
    AckServer, AnsReplyToken, AnsServer, DatapodMsg, DeliveryPolicy, LocalConfig, Node, NodeSample,
    PipClient, PipServer, PipServerToken, PipSessionToken, Publisher, PutAckToken, PutClient,
    PutUploadToken, QueClient, RawMsg, ReqClient, ReqReplyToken, ReqServer, Subscriber, TopicQos,
};

mod convert;
mod datapod_pip;
mod datapod_putack;
mod node;
mod pip;
mod pubsub;
mod putack;
mod queans;
mod reqres;
mod types;

pub(crate) use convert::*;
pub use datapod_pip::*;
pub use datapod_putack::*;
pub use node::*;
pub use pip::*;
pub use pubsub::*;
pub use putack::*;
pub use queans::*;
pub use reqres::*;
pub use types::*;

pub fn register_python_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDeliveryPolicy>()?;
    module.add_class::<PyTopicQos>()?;
    module.add_class::<PyMessage>()?;
    module.add_class::<PySampleView>()?;
    module.add_class::<PyDatapodMessage>()?;
    module.add_class::<PyNode>()?;
    module.add_class::<PyPublisher>()?;
    module.add_class::<PySubscriber>()?;
    module.add_class::<PyDatapodPublisher>()?;
    module.add_class::<PyDatapodSubscriber>()?;
    module.add_class::<PyDatapodSampleView>()?;
    module.add_class::<PyReqClient>()?;
    module.add_class::<PyReqServer>()?;
    module.add_class::<PyPendingReq>()?;
    module.add_class::<PyDatapodReqClient>()?;
    module.add_class::<PyDatapodReqServer>()?;
    module.add_class::<PyPendingDatapodReq>()?;
    module.add_class::<PyDatapodQueClient>()?;
    module.add_class::<PyDatapodAnsServer>()?;
    module.add_class::<PyPendingDatapodQue>()?;
    module.add_class::<PyPendingDatapodAnswers>()?;
    module.add(
        "PendingDatapodAns",
        module.getattr("PendingDatapodAnswers")?,
    )?;
    module.add_class::<PyDatapodPutClient>()?;
    module.add_class::<PyDatapodPutUpload>()?;
    module.add("DatapodPutSender", module.getattr("DatapodPutUpload")?)?;
    module.add_class::<PyDatapodAckServer>()?;
    module.add_class::<PyDatapodPutBatch>()?;
    module.add_class::<PyPendingDatapodPut>()?;
    module.add_class::<PyDatapodPipClient>()?;
    module.add_class::<PyDatapodPipSession>()?;
    module.add_class::<PyDatapodPipServer>()?;
    module.add_class::<PyPendingDatapodPip>()?;
    module.add_class::<PyQueClient>()?;
    module.add_class::<PyAnsServer>()?;
    module.add_class::<PyPendingQue>()?;
    module.add_class::<PyPendingAnswers>()?;
    module.add("PendingAns", module.getattr("PendingAnswers")?)?;
    module.add_class::<PyPutClient>()?;
    module.add_class::<PyPutUpload>()?;
    module.add("PutSender", module.getattr("PutUpload")?)?;
    module.add_class::<PyPutBatch>()?;
    module.add_class::<PyAckServer>()?;
    module.add_class::<PyPendingPut>()?;
    module.add_class::<PyPipClient>()?;
    module.add_class::<PyPipSession>()?;
    module.add_class::<PyPipServer>()?;
    module.add_class::<PyPendingPip>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[pymodule]
fn peerbus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_python_module(module)
}
