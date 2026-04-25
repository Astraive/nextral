use pyo3::{exceptions::PyRuntimeError, prelude::*};

fn map_core_error(error: nextral::CoreError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pyfunction]
fn lexical_score(text: String, query: String) -> PyResult<f32> {
    nextral::scoring::try_lexical_score(&text, &query).map_err(map_core_error)
}

#[pyfunction]
fn validate_config(config_json: String) -> PyResult<String> {
    nextral::config::validate_config_json(&config_json).map_err(map_core_error)
}

#[pyfunction]
fn e2e_smoke() -> PyResult<String> {
    nextral::package::e2e_smoke_json().map_err(|error| PyRuntimeError::new_err(error.message))
}

#[pyfunction]
fn reembed_plan(request_json: String) -> PyResult<String> {
    nextral::package::reembed_plan_json(&request_json)
        .map_err(|error| PyRuntimeError::new_err(error.message))
}

#[pyfunction]
fn ingest_request_schema() -> PyResult<String> {
    Ok(nextral::package::ingest_request_schema_json())
}

#[pymodule]
fn _nextral(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(lexical_score, module)?)?;
    module.add_function(wrap_pyfunction!(validate_config, module)?)?;
    module.add_function(wrap_pyfunction!(e2e_smoke, module)?)?;
    module.add_function(wrap_pyfunction!(reembed_plan, module)?)?;
    module.add_function(wrap_pyfunction!(ingest_request_schema, module)?)?;
    Ok(())
}
