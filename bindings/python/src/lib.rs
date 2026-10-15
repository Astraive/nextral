use pyo3::{create_exception, exceptions::PyException, prelude::*};

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
fn configure_runtime(py: Python<'_>, config_json: &str) -> PyResult<String> {
    py.detach(|| nextral::package::configure_runtime_json(config_json))
        .map_err(map_package_error)
}

#[pyfunction]
fn serve(py: Python<'_>, config_json: &str, modes_json: &str) -> PyResult<String> {
    py.detach(|| nextral::package::run_service_hosts_json(config_json, modes_json))
        .map_err(map_package_error)
}

#[pyfunction]
fn stop_serving() {
    nextral::api::request_service_shutdown();
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

#[pyfunction]
fn mcp_call(py: Python<'_>, request_json: &str) -> PyResult<String> {
    py.detach(|| nextral::package::mcp_call_json(request_json))
        .map_err(map_package_error)
}

#[pymodule]
fn _nextral(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("NativeError", py.get_type::<NativeError>())?;
    module.add_function(wrap_pyfunction!(lexical_score, module)?)?;
    module.add_function(wrap_pyfunction!(validate_config, module)?)?;
    module.add_function(wrap_pyfunction!(configure_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(serve, module)?)?;
    module.add_function(wrap_pyfunction!(stop_serving, module)?)?;
    module.add_function(wrap_pyfunction!(e2e_smoke, module)?)?;
    module.add_function(wrap_pyfunction!(reembed_plan, module)?)?;
    module.add_function(wrap_pyfunction!(ingest_request_schema, module)?)?;
    module.add_function(wrap_pyfunction!(adapter_smoke, module)?)?;
    module.add_function(wrap_pyfunction!(mcp_call, module)?)?;
    Ok(())
}
