# Installation

## Released artifacts

```sh
python -m pip install nextral
npm install nextral
```

These commands require a published version with an artifact for your host; they do not establish that the current checkout has been published. Python requires CPython 3.10+; Node requires Node.js 22+. Supported release targets are Windows x64 MSVC, macOS x64/arm64, and glibc Linux x64/arm64. There is no musl/Alpine, PyPy, or free-threaded Python guarantee. See the [package matrix](../package-production.md#supported-capabilities-and-packages).

## Source checkout

From the repository root, install Rust, the platform linker, and the required language tooling, then:

```sh
cargo build --workspace
python -m pip install -e bindings/python
npm ci
npm --workspace bindings/node run build
```

Editable Python installation builds the PyO3 extension with maturin. Node uses the package's pinned napi tooling and writes the native module under `bindings/node/native/`, keeping `api.js` and `api.d.ts` authoritative. Cargo output respects the configured target directory; do not assume `target/` if the host overrides it.

Python installs a `nextral` console entry point; Rust also builds a binary named `nextral`. Use `python -m nextral --help` for Python and an explicit path to the Rust binary to avoid `PATH` ambiguity. npm installs a library, not the Rust CLI, API host, or MCP executable.

No package installs the external stores or a model provider. Continue with the [quickstart](quickstart.md) and [deployment configuration](../package-production.md).
