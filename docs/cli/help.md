# Help snapshots (0.0.1)

Historical snapshot only: these scaffold commands are not the current CLI. Use the [current CLI reference](README.md) for executable operations, Rust/Python parser differences, MCP JSON-RPC and real service hosting.

## `nextral --help`

```text
usage: nextral [-h] [--version] COMMAND ...

Nextral CLI - runtime-neutral scaffold for a memory-enabled agent runtime. Use subcommand help to inspect planned command surfaces.

positional arguments:
  COMMAND
    about       Show package status and documentation entry points.
    memory      Memory command group (placeholder surfaces).
    tools       Tool registry command group (placeholder surfaces).
    files       File memory command group (placeholder surfaces).

options:
  -h, --help    show this help message and exit
  --version     show program's version number and exit
```

## `nextral memory --help`

```text
usage: nextral memory [-h] SUBCOMMAND ...

positional arguments:
  SUBCOMMAND
    status      Show memory subsystem status (placeholder).
    add-file    Plan to add a file into memory ingestion flow (placeholder).

options:
  -h, --help    show this help message and exit
```

## `nextral memory add-file --help`

```text
usage: nextral memory add-file [-h] [path]

positional arguments:
  path          File path to register for future memory ingestion.

options:
  -h, --help    show this help message and exit
```

## `nextral tools --help`

```text
usage: nextral tools [-h] SUBCOMMAND ...

positional arguments:
  SUBCOMMAND
    list        List configured tools (placeholder).

options:
  -h, --help    show this help message and exit
```

## `nextral files --help`

```text
usage: nextral files [-h] SUBCOMMAND ...

positional arguments:
  SUBCOMMAND
    index       Prepare a file for memory indexing (placeholder).

options:
  -h, --help    show this help message and exit
```

