# nextral

`nextral` is a package-first memory runtime with a canonical Rust core, Python
and Node.js bindings, CLI tools, and optional HTTP/gRPC/GraphQL/MCP service
surfaces.

## Architecture at a glance

```text
nextral/
├── src/                 # canonical Rust core and runtime modules
├── bindings/
│   ├── python/          # PyO3 bridge + Python wrappers
│   └── node/            # napi-rs bridge + TS wrappers
├── apps/                # native consumers (CLI, API, MCP, examples)
├── tests/
├── docs/
├── scripts/
├── Cargo.toml
├── package.json
└── pyproject.toml
```

## Build surfaces

```bash
cargo build --workspace
pip install -e bindings/python
npm --workspace bindings/node run build
nextral memory smoke
```

## Documentation

- Main index: `docs/README.md`
- Project structure: `docs/architecture/project-structure.md`
- Memory system docs: `docs/memory/README.md`
- Package production runtime: `docs/package-production.md`

