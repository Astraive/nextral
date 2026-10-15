use super::{auth::Principal, http, ApiError, ServiceState, MAX_BODY, REQUEST_TIMEOUT};
use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, InputObject, InputValue, Object, Schema, TypeRef},
    Error, ErrorExtensions,
};
use async_graphql_parser::types::{BaseType, Type, TypeKind, TypeSystemDefinition};
use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use serde_json::{Map, Value};

pub const SCHEMA: &str = include_str!("../../contracts/graphql/schema.graphql");

fn type_ref(ty: &Type) -> TypeRef {
    let inner = match &ty.base {
        BaseType::Named(name) => TypeRef::named(name.to_string()),
        BaseType::List(ty) => TypeRef::List(Box::new(type_ref(ty))),
    };
    if ty.nullable {
        inner
    } else {
        TypeRef::NonNull(Box::new(inner))
    }
}

fn snake(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    for ch in name.chars() {
        if ch.is_ascii_uppercase() {
            result.push('_');
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

fn runtime_json(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (snake(&key), runtime_json(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(runtime_json).collect()),
        value => value,
    }
}

fn field_value(value: Value) -> async_graphql::Result<Option<FieldValue<'static>>> {
    Ok(match value {
        Value::Null => None,
        Value::Object(_) => Some(FieldValue::owned_any(value)),
        Value::Array(values) => Some(FieldValue::list(
            values
                .into_iter()
                .map(|v| {
                    field_value(v)
                        .map(|v| v.unwrap_or_else(|| FieldValue::value(async_graphql::Value::Null)))
                })
                .collect::<async_graphql::Result<Vec<_>>>()?,
        )),
        scalar => Some(FieldValue::value(async_graphql::Value::from_json(scalar)?)),
    })
}

fn graphql_error(error: ApiError) -> Error {
    Error::new(error.message).extend_with(|_, extensions| {
        extensions.set("code", error.code);
        extensions.set("status", error.status);
    })
}

/// Constructs the executor directly from the published SDL, including introspection.
pub fn build_schema() -> crate::contracts::CoreResult<Schema> {
    let document = async_graphql_parser::parse_schema(SCHEMA)
        .map_err(|e| crate::contracts::CoreError::InvalidInput(e.to_string()))?;
    let mut builder = Schema::build("Query", Some("Mutation"), None)
        .limit_depth(32)
        .limit_complexity(4096)
        .limit_recursive_depth(32)
        .limit_directives(16);
    for definition in document.definitions {
        let TypeSystemDefinition::Type(definition) = definition else {
            continue;
        };
        let object = match definition.node.kind {
            TypeKind::Object(object) => object,
            TypeKind::InputObject(input) => {
                let mut object = InputObject::new(definition.node.name.node.to_string());
                for field in input.fields {
                    object = object.field(InputValue::new(
                        field.node.name.node.to_string(),
                        type_ref(&field.node.ty.node),
                    ));
                }
                builder = builder.register(object);
                continue;
            }
            _ => {
                return Err(crate::contracts::CoreError::InvalidInput(
                    "unsupported type in GraphQL contract".into(),
                ))
            }
        };
        let name = definition.node.name.node.to_string();
        let root = name == "Query" || name == "Mutation";
        let mut output = Object::new(name);
        for definition in object.fields {
            let definition = definition.node;
            let name = definition.name.node.to_string();
            let resolver_name = name.clone();
            let mut field = Field::new(name, type_ref(&definition.ty.node), move |ctx| {
                let name = resolver_name.clone();
                FieldFuture::new(async move {
                    if !root {
                        let value = ctx.parent_value.try_downcast_ref::<Value>()?;
                        return field_value(
                            value.get(snake(&name)).cloned().unwrap_or(Value::Null),
                        );
                    }
                    let mut arguments = Map::new();
                    for (name, value) in ctx.args.iter() {
                        arguments.insert(
                            snake(name.as_str()),
                            runtime_json(value.deserialize::<Value>()?),
                        );
                    }
                    let state = ctx.data::<ServiceState>()?;
                    let principal = ctx.data::<Principal>()?;
                    let tool = match name.as_str() {
                        "memory" => {
                            if let Some(id) = arguments.remove("id") {
                                arguments.insert("memory_id".into(), id);
                            }
                            "nextral.memory.get"
                        }
                        "retrieve" => "nextral.memory.retrieve",
                        "reminders" => "nextral.reminders.list",
                        "ingestMemory" => "nextral.memory.ingest",
                        "forgetMemory" => "nextral.memory.forget",
                        "scheduleReminder" => "nextral.reminders.schedule",
                        _ => return Err(Error::new("GraphQL root has no runtime operation")),
                    };
                    let mut result = state
                        .call(principal, tool, Value::Object(arguments))
                        .await
                        .map_err(graphql_error)?;
                    if tool == "nextral.reminders.schedule" {
                        result = result["reminder"]["id"].clone();
                    }
                    field_value(result)
                })
            });
            for argument in definition.arguments {
                let arg = argument.node;
                let mut input = InputValue::new(arg.name.node.to_string(), type_ref(&arg.ty.node));
                if let Some(default) = arg.default_value {
                    input = input.default_value(default.node);
                }
                field = field.argument(input);
            }
            output = output.field(field);
        }
        builder = builder.register(output);
    }
    builder
        .finish()
        .map_err(|e| crate::contracts::CoreError::InvalidInput(e.to_string()))
}

#[derive(Clone)]
struct GraphqlState {
    runtime: ServiceState,
    schema: Schema,
}

pub(crate) fn router(runtime: ServiceState, schema: Schema) -> Router {
    let readiness = runtime.clone();
    Router::new()
        .route("/graphql", post(execute))
        .route("/healthz", get(http::health))
        .route(
            "/readyz",
            get(move || {
                let state = readiness.clone();
                async move { http::ready(State(state)).await }
            }),
        )
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(axum::middleware::from_fn_with_state(
            runtime.clone(),
            http::limit_requests,
        ))
        .with_state(GraphqlState { runtime, schema })
}

async fn execute(
    State(state): State<GraphqlState>,
    headers: HeaderMap,
    request: Result<Json<async_graphql::Request>, JsonRejection>,
) -> Result<Json<async_graphql::Response>, ApiError> {
    let principal = http::principal(&state.runtime, &headers)?;
    let Json(request) =
        request.map_err(|e| ApiError::new(e.status().as_u16(), "invalid_input", e.body_text()))?;
    let request = request.data(principal).data(state.runtime);
    let response = tokio::time::timeout(REQUEST_TIMEOUT, state.schema.execute(request))
        .await
        .map_err(|_| {
            ApiError::new(
                504,
                "deadline_exceeded",
                "GraphQL execution deadline exceeded",
            )
        })?;
    Ok(Json(response))
}
