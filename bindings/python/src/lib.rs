use pyo3::{create_exception, exceptions::PyException, prelude::*};
use std::sync::Arc;
use nextral::package::{CallOptions, RuntimeClient};

create_exception!(_nextral, NativeError, PyException);

fn map_package_error(error: nextral::package::PackageError) -> PyErr {
    NativeError::new_err((error.code, error.message))
}

fn map_core_error(error: nextral::CoreError) -> PyErr {
    map_package_error(error.into())
}

#[pyfunction]
fn lexical_score(text: &str, query: &str) -> PyResult<f32> {
    nextral::scoring::try_lexical_score(text, query).map_err(map_core_error)
}

#[pyfunction]
fn validate_config(config_json: &str) -> PyResult<String> {
    nextral::config::validate_config_json(config_json).map_err(map_core_error)
}

#[pyfunction]
fn e2e_smoke(py: Python<'_>) -> PyResult<String> {
    py.detach(nextral::package::e2e_smoke_json)
        .map_err(map_package_error)
}

#[pyfunction]
fn reembed_plan(request_json: &str) -> PyResult<String> {
    nextral::package::reembed_plan_json(request_json).map_err(map_package_error)
}

#[pyfunction]
fn ingest_request_schema() -> String {
    nextral::package::ingest_request_schema_json()
}

#[pyfunction]
fn adapter_smoke(py: Python<'_>, request_json: &str) -> PyResult<String> {
    py.detach(|| nextral::package::adapter_smoke_json(request_json))
        .map_err(map_package_error)
}

#[pyclass]
struct Client {
    client: Arc<RuntimeClient>,
    rt: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl Client {
    #[new]
    fn new(py: Python<'_>, config_json: &str) -> PyResult<Self> {
        let config = nextral::config::load_config(config_json).map_err(map_core_error)?;
        let client = py
            .detach(|| RuntimeClient::from_config(config))
            .map_err(map_package_error)?;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| NativeError::new_err(("io".to_string(), e.to_string())))?;
        Ok(Self {
            client: Arc::new(client),
            rt: Arc::new(rt),
        })
    }

    #[pyo3(signature = (tool, payload_json, timeout_ms=None))]
    fn call_json(
        &self,
        py: Python<'_>,
        tool: &str,
        payload_json: &str,
        timeout_ms: Option<u64>,
    ) -> PyResult<String> {
        let client = self.client.clone();
        let rt = self.rt.clone();
        let tool = tool.to_string();
        let payload = payload_json.to_string();
        py.detach(move || {
            rt.block_on(client.call_json(
                &tool,
                payload,
                CallOptions {
                    timeout_ms,
                    cancellation_token: None,
                },
            ))
        })
        .map_err(map_package_error)
    }

    #[pyo3(signature = (tool, payload_json, timeout_ms=None))]
    fn call_json_async<'py>(
        &self,
        py: Python<'py>,
        tool: String,
        payload_json: String,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .call_json(
                    &tool,
                    payload_json,
                    CallOptions {
                        timeout_ms,
                        cancellation_token: None,
                    },
                )
                .await
                .map_err(map_package_error)
        })
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn close(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<String> {
        let client = self.client.clone();
        let rt = self.rt.clone();
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000));
        let report = py
            .detach(move || rt.block_on(client.close(timeout)))
            .map_err(map_package_error)?;
        serde_json::to_string(&report)
            .map_err(|e| NativeError::new_err(("serialization".to_string(), e.to_string())))
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn close_async<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = client.close(timeout).await.map_err(map_package_error)?;
            serde_json::to_string(&report)
                .map_err(|e| NativeError::new_err(("serialization".to_string(), e.to_string())))
        })
    }

    fn serve(&self, py: Python<'_>) -> PyResult<PyServiceHandle> {
        let client = self.client.clone();
        let rt = self.rt.clone();
        let handle = py
            .detach(move || rt.block_on(client.serve()))
            .map_err(map_package_error)?;
        Ok(PyServiceHandle {
            handle: Arc::new(handle),
            rt: self.rt.clone(),
        })
    }

    fn serve_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let rt = self.rt.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = client.serve().await.map_err(map_package_error)?;
            Ok(PyServiceHandle {
                handle: Arc::new(handle),
                rt,
            })
        })
    }

    fn start_worker(&self, py: Python<'_>, config_json: &str) -> PyResult<PyWorkerHandle> {
        let config: nextral::runtime::prospective::WorkerConfig =
            serde_json::from_str(config_json)
                .map_err(|e| NativeError::new_err(("invalid_input".to_string(), e.to_string())))?;
        let client = self.client.clone();
        let rt = self.rt.clone();
        let handle = py
            .detach(move || rt.block_on(client.start_worker(config)))
            .map_err(map_package_error)?;
        Ok(PyWorkerHandle {
            handle: Arc::new(handle),
            rt: self.rt.clone(),
        })
    }

    fn start_worker_async<'py>(
        &self,
        py: Python<'py>,
        config_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config: nextral::runtime::prospective::WorkerConfig =
            serde_json::from_str(&config_json)
                .map_err(|e| NativeError::new_err(("invalid_input".to_string(), e.to_string())))?;
        let client = self.client.clone();
        let rt = self.rt.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = client.start_worker(config).await.map_err(map_package_error)?;
            Ok(PyWorkerHandle {
                handle: Arc::new(handle),
                rt,
            })
        })
    }
}

#[pyclass]
struct PyServiceHandle {
    handle: Arc<nextral::package::ServiceHandle>,
    rt: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyServiceHandle {
    #[pyo3(signature = (timeout_ms=None))]
    fn shutdown(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<()> {
        let handle = self.handle.clone();
        let rt = self.rt.clone();
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000));
        py.detach(move || rt.block_on(handle.shutdown(timeout)))
            .map_err(map_package_error)
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn shutdown_async<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle.shutdown(timeout).await.map_err(map_package_error)
        })
    }

    fn binds(&self) -> Vec<String> {
        self.handle.binds().to_vec()
    }
}
#[pyclass]
struct PyWorkerHandle {
    handle: Arc<nextral::package::WorkerHandle>,
    rt: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyWorkerHandle {
    fn tick(&self, py: Python<'_>) -> PyResult<String> {
        let handle = self.handle.clone();
        let rt = self.rt.clone();
        let report = py
            .detach(move || rt.block_on(handle.tick()))
            .map_err(map_package_error)?;
        serde_json::to_string(&report)
            .map_err(|e| NativeError::new_err(("serialization".to_string(), e.to_string())))
    }

    fn tick_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = handle.tick().await.map_err(map_package_error)?;
            serde_json::to_string(&report)
                .map_err(|e| NativeError::new_err(("serialization".to_string(), e.to_string())))
        })
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn shutdown(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<()> {
        let handle = self.handle.clone();
        let rt = self.rt.clone();
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000));
        py.detach(move || rt.block_on(handle.shutdown(timeout)))
            .map_err(map_package_error)
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn shutdown_async<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone();
        let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(30000));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle.shutdown(timeout).await.map_err(map_package_error)
        })
    }
}

#[pymodule]
fn _nextral(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("NativeError", py.get_type::<NativeError>())?;
    module.add_class::<Client>()?;
    module.add_class::<PyServiceHandle>()?;
    module.add_class::<PyWorkerHandle>()?;
    module.add_function(wrap_pyfunction!(lexical_score, module)?)?;
    module.add_function(wrap_pyfunction!(validate_config, module)?)?;
    module.add_function(wrap_pyfunction!(e2e_smoke, module)?)?;
    module.add_function(wrap_pyfunction!(reembed_plan, module)?)?;
    module.add_function(wrap_pyfunction!(ingest_request_schema, module)?)?;
    module.add_function(wrap_pyfunction!(adapter_smoke, module)?)?;
    Ok(())
}
