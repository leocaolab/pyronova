//! One request's before hooks, handler and after hooks, in order, in the request's own
//! `contextvars.Context`: the chain every synchronous path runs, on the main interpreter
//! and in a worker alike. The paths differ only in where an awaitable a hook or handler
//! returned runs ([`EventLoop`]) and in what a returned value may be ([`ToResponse`]: a
//! worker can't stream).
//!
//! The async engine runs the same order and semantics in Python (`_async_engine.py`),
//! because there a request is a task on the engine's own running loop.

use pyo3::call::PyCallArgs;
use pyo3::intern;
use pyo3::prelude::*;

use super::request_context::{Awaitable, RequestContext};
use crate::error::{HandlerError, ResponseError, Stage};
use crate::types::{PyronovaRequest, ResponseData};

/// Where an awaitable a hook or handler returned is driven to completion.
pub(crate) trait EventLoop {
    fn event_loop<'py>(&self, py: Python<'py>) -> Result<Bound<'py, PyAny>, HandlerError>;
}

/// A hook's or handler's return value as a response.
pub(crate) type ToResponse = for<'py> fn(Bound<'py, PyAny>) -> Result<ResponseData, ResponseError>;

/// The main interpreter's [`ToResponse`]: `response::extract_response_data`.
pub(crate) fn main_response(value: Bound<'_, PyAny>) -> Result<ResponseData, ResponseError> {
    crate::response::extract_response_data(value.py(), value)
}

/// A worker's [`ToResponse`]: the same mapping, except that a worker can't stream, so a
/// `Stream` is an error (streaming needs `gil=True, stream=True`).
pub(crate) fn worker_response(value: Bound<'_, PyAny>) -> Result<ResponseData, ResponseError> {
    if value.is_instance_of::<crate::python::stream::PyronovaStream>() {
        return Err(ResponseError::StreamInWorker);
    }
    crate::response::extract_response_data(value.py(), value)
}

/// The chain of one request: its context, its event loop, its response mapping.
pub(crate) struct Chain<'a, 'py, L: EventLoop> {
    pub(crate) rc: &'a RequestContext<'py>,
    pub(crate) event_loop: &'a L,
    pub(crate) to_response: ToResponse,
}

impl<'py, L: EventLoop> Chain<'_, 'py, L> {
    /// The before-request hooks, in order, until one returns something other than `None`:
    /// `Some(response)` short-circuits the request with it. A hook that raises fails the
    /// request (running the handler anyway would bypass an auth hook that denies by
    /// raising).
    pub(crate) fn before(
        &self,
        hooks: &[Py<PyAny>],
        req: &Bound<'py, PyronovaRequest>,
    ) -> Result<Option<ResponseData>, HandlerError> {
        for hook in hooks {
            let value = self.call(hook, (req,), Stage::BeforeHook)?;
            if !value.is_none() {
                return Ok(Some((self.to_response)(value)?));
            }
        }
        Ok(None)
    }

    /// What the handler returned, an awaitable already run.
    pub(crate) fn handler(
        &self,
        handler: &Py<PyAny>,
        req: &Bound<'py, PyronovaRequest>,
    ) -> Result<Bound<'py, PyAny>, HandlerError> {
        self.call(handler, (req,), Stage::Handler)
    }

    /// The after-request hooks, in order: each gets the response so far as a `Response`
    /// and may replace it by returning something other than `None`.
    pub(crate) fn after(
        &self,
        hooks: &[Py<PyAny>],
        req: &Bound<'py, PyronovaRequest>,
        response: ResponseData,
    ) -> Result<ResponseData, HandlerError> {
        hooks.iter().try_fold(response, |response, hook| {
            let current = response.to_py(self.rc.py())?;
            let value = self.call(hook, (req, current), Stage::AfterHook)?;
            if value.is_none() {
                Ok(response)
            } else {
                Ok((self.to_response)(value)?)
            }
        })
    }

    /// `f(*args)`, an awaitable it returns run to completion. An exception either raises is
    /// `stage`'s.
    fn call<A: PyCallArgs<'py>>(
        &self,
        f: &Py<PyAny>,
        args: A,
        stage: Stage,
    ) -> Result<Bound<'py, PyAny>, HandlerError> {
        let py = self.rc.py();
        let raised = |e: PyErr| HandlerError::python(py, stage, &e);
        let value = f.bind(py).call1(args).map_err(raised)?;
        match Awaitable::of(&value) {
            None => Ok(value),
            // A coroutine or any other non-`Future` awaitable (`_awaitable.drive` wraps
            // it): a task in the request's own context, so its `ContextVar` writes reach
            // the rest of the request.
            Some(Awaitable::InTask) => {
                let event_loop = self.event_loop.event_loop(py)?;
                self.rc.run_in_task(&event_loop, &value).map_err(raised)
            }
            // A `Future` is already scheduled in its own context: only waited for.
            Some(Awaitable::Future) => {
                let event_loop = self.event_loop.event_loop(py)?;
                event_loop
                    .call_method1(intern!(py, "run_until_complete"), (value,))
                    .map_err(raised)
            }
        }
    }
}
