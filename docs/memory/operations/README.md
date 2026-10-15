# Memory operations

- [Consolidation and retention workers](consolidation-and-retention.md)
- [Incidents and recovery](incidents-and-recovery.md)
- [Scoped reembedding and backfill](reembed-and-backfill.md)
- [Deployment SLO targets, telemetry and measured replay research](slos-alerting-and-capacity.md)

Operate with a restricted no-DDL runtime database role after explicit owner migration. Run workers on a host-owned schedule; package import does not install services. Observe durable states and actual receipts rather than claiming cross-store atomicity or production readiness from an accepted request.

## Disposable integration stores

`docker-compose.integration.yml` binds all five real stores only to loopback. The MinIO service builds unmodified [upstream release `RELEASE.2025-10-15T17-29-55Z`](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z), commit [`9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`](https://github.com/minio/minio/commit/9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a). Upstream directs container users to build from source; its former public MinIO registries are not a provisioning prerequisite. The Compose build verifies the upstream archive SHA-256 and pins the official Go 1.24.8 and Alpine 3.22.1 image indexes by digest. Go dependencies remain governed by upstream `go.mod`/`go.sum` and the Go checksum database; no integrity checks are disabled.

The `mc` fixture client is an opt-in Compose `tooling` service, built from [official release `RELEASE.2025-08-13T08-35-41Z`](https://github.com/minio/mc/releases/tag/RELEASE.2025-08-13T08-35-41Z), commit [`7394ce0dd2a80935aded936b09fa12cbb3cb8096`](https://github.com/minio/mc/commit/7394ce0dd2a80935aded936b09fa12cbb3cb8096), with archive SHA-256 verification and the same pinned base images. It joins the actual Compose network for authenticated bucket creation and archive reads; it is not a separate storage implementation. Its AGPL license and exact source archive are retained under `/usr/share/mc/`.

MinIO is [AGPL-3.0-or-later](https://github.com/minio/minio/blob/9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a/LICENSE), independently of Nextral's license. The local image retains its license and exact upstream source archive under `/usr/share/minio/`, with source/revision/license OCI labels. Review corresponding-source obligations before redistributing this image or modifying MinIO.

Build both images before starting the integration runner (`docker compose -f docker-compose.integration.yml --profile tooling build minio mc`); CI does this separately from the runner's service-start deadline. A cold build needs access to Docker Hub's official Go/Alpine images, GitHub's source archive endpoint, and the Go module proxy/checksum services. It requires Docker Compose 2.17+ for `dockerfile_inline` and a working isolated Docker daemon. The server retains signed S3 access, conditional writes, and the real readiness endpoint; bucket fixtures and real-stack acceptance remain the runner's responsibility. The runner tears down stacks it starts unless explicitly asked to retain them. Building the images alone is not evidence of a successful five-store smoke or research run.
