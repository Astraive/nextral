#!/usr/bin/env python3
"""Portable release guards and installed-artifact consumer checks (stdlib only)."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile
from email.parser import BytesParser

ROOT = Path(__file__).resolve().parents[1]
NATIVE_TARGETS = {
    "win32-x64-msvc": ("pe", 0x8664),
    "darwin-x64": ("macho", 0x01000007),
    "darwin-arm64": ("macho", 0x0100000C),
    "linux-x64-gnu": ("elf", 62),
    "linux-arm64-gnu": ("elf", 183),
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def run(*args: str | Path, cwd: Path | None = None, env: dict | None = None) -> None:
    command = [str(arg) for arg in args]
    if os.name == "nt" and command[0] in {"npm", "npx"}:
        command[0] = shutil.which(command[0] + ".cmd") or command[0]
    subprocess.run(command, cwd=cwd, env=env, check=True)


def toml(path: Path) -> dict:
    import tomllib  # Release guards run with Python 3.12; consumers support 3.10.
    return tomllib.loads(path.read_text(encoding="utf-8"))


def published_packages() -> list[tuple[Path, dict]]:
    """Order publishable workspace crates after their local dependencies."""
    root = toml(ROOT / "Cargo.toml")
    packages = {}
    for directory in [ROOT, *(ROOT / member for member in root["workspace"]["members"])]:
        manifest = toml(directory / "Cargo.toml")
        if manifest["package"].get("publish", True) is not False:
            require(manifest["package"].get("publish", True) is True,
                    f"Release supports crates.io only: {directory}")
            packages[manifest["package"]["name"]] = (directory, manifest)
    ordered = []
    visiting = set()
    visited = set()

    def visit(name: str) -> None:
        require(name not in visiting, f"Cyclic publish dependencies: {name}")
        if name in visited:
            return
        visiting.add(name)
        directory, manifest = packages[name]
        tables = [manifest]
        tables.extend(manifest.get("target", {}).values())
        for table in tables:
            for section in ("dependencies", "build-dependencies", "dev-dependencies"):
                for alias, dependency in table.get(section, {}).items():
                    if not isinstance(dependency, dict) or "path" not in dependency:
                        continue
                    dependency_name = dependency.get("package", alias)
                    require(dependency_name in packages,
                            f"{name} depends on unpublished local crate {dependency_name}")
                    expected = packages[dependency_name][1]["package"]["version"]
                    require(dependency.get("version") == expected,
                            f"{name}: local dependency {dependency_name} must declare version {expected}")
                    visit(dependency_name)
        visiting.remove(name)
        visited.add(name)
        ordered.append((directory, manifest))

    for name in packages:
        visit(name)
    return ordered



def version_guard(tag: str | None = None) -> str:
    root_manifest = toml(ROOT / "Cargo.toml")
    version = root_manifest["package"]["version"]
    declarations = {}
    packages = [root_manifest["package"]["name"]]
    for member in root_manifest["workspace"]["members"]:
        package = toml(ROOT / member / "Cargo.toml")["package"]
        packages.append(package["name"])
        declarations[f"{member}/Cargo.toml"] = package["version"]
    locked = toml(ROOT / "Cargo.lock")["package"]
    for name in packages:
        matches = [package for package in locked if package["name"] == name and "source" not in package]
        require(len(matches) == 1, f"Missing or duplicate workspace package in Cargo.lock: {name}")
        declarations[f"Cargo.lock:{name}"] = matches[0]["version"]
    for name in ("package.json", "bindings/node/package.json"):
        declarations[name] = json.loads((ROOT / name).read_text(encoding="utf-8"))["version"]
    for name in ("package-lock.json", "bindings/node/package-lock.json"):
        lock = json.loads((ROOT / name).read_text(encoding="utf-8"))
        declarations[name] = lock["version"]
        declarations[name + ":root"] = lock["packages"][""]["version"]
        if "bindings/node" in lock["packages"]:
            declarations[name + ":bindings/node"] = lock["packages"]["bindings/node"]["version"]
    declarations["bindings/python/pyproject.toml"] = toml(ROOT / "bindings/python/pyproject.toml")["project"]["version"]
    tree = ast.parse((ROOT / "bindings/python/nextral/_version.py").read_text(encoding="utf-8"))
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)]
    declarations["bindings/python/nextral/_version.py"] = next(
        ast.literal_eval(node.value) for node in assignments
        if any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)
    )
    for name, declared in declarations.items():
        require(declared == version, f"Version mismatch: {name}={declared!r}, Rust={version!r}")
    published_packages()
    if tag is not None:
        require(tag == f"v{version}", f"Tag {tag!r} must match v{version}")
    print(f"Release version: {version}")
    return version


def only_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.resolve().glob(pattern))
    require(len(files) == 1, f"Expected one {pattern} in {directory}, found {len(files)}")
    return files[0]


def native_guard(data: bytes, suffix: str) -> None:
    kind, machine = NATIVE_TARGETS[suffix]
    require(len(data) >= 64, f"Truncated native artifact: {suffix}")
    if kind == "elf":
        require(data[:5] == b"\x7fELF\x02" and data[5] == 1, f"Expected little-endian ELF64: {suffix}")
        actual = struct.unpack_from("<H", data, 18)[0]
    elif kind == "pe":
        require(data[:2] == b"MZ", f"Expected PE artifact: {suffix}")
        offset = struct.unpack_from("<I", data, 0x3C)[0]
        require(offset + 6 <= len(data) and data[offset:offset + 4] == b"PE\0\0", f"Invalid PE artifact: {suffix}")
        actual = struct.unpack_from("<H", data, offset + 4)[0]
    else:
        require(data[:4] == b"\xcf\xfa\xed\xfe", f"Expected little-endian Mach-O64: {suffix}")
        actual = struct.unpack_from("<I", data, 4)[0]
    require(actual == machine, f"Wrong architecture in native artifact: {suffix}")


def archive_members(archive: tarfile.TarFile) -> dict[str, tarfile.TarInfo]:
    members = {}
    for item in archive.getmembers():
        path = PurePosixPath(item.name)
        require(not path.is_absolute() and ".." not in path.parts and "\\" not in item.name,
                f"Unsafe archive path: {item.name}")
        require(item.isfile() or item.isdir(), f"Unexpected archive member: {item.name}")
        require(item.name not in members, f"Duplicate archive member: {item.name}")
        members[item.name] = item
    return members


def npm_guard(path: Path, version: str) -> None:
    with tarfile.open(path, "r:gz") as archive:
        members = archive_members(archive)
        required = {"package/package.json", "package/api.js", "package/api.d.ts", "package/native-loader.js", "package/LICENSE", "package/README.md"}
        required.update(f"package/native/nextral.{suffix}.node" for suffix in NATIVE_TARGETS)
        require(required <= members.keys(), f"npm artifact missing: {sorted(required - members.keys())}")
        files = {name for name, item in members.items() if item.isfile()}
        require(files == required, f"Unexpected npm files: {sorted(files - required)}")
        metadata = json.load(archive.extractfile(members["package/package.json"]))
        require(metadata["name"] == "nextral" and metadata["version"] == version, "Wrong npm name/version")
        require(not any(metadata.get("scripts", {}).get(name) for name in ("preinstall", "install", "postinstall")),
                "npm artifact must not execute install hooks")
        for suffix in NATIVE_TARGETS:
            native_guard(archive.extractfile(members[f"package/native/nextral.{suffix}.node"]).read(), suffix)


def wheel_guard(path: Path, version: str) -> str:
    require("-cp310-abi3-" in path.name, f"Expected CPython 3.10 stable ABI wheel: {path.name}")
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)), f"Duplicate wheel member: {path}")
        require(all(not PurePosixPath(name).is_absolute() and ".." not in PurePosixPath(name).parts
                    and "\\" not in name for name in names), f"Unsafe wheel path: {path}")
        metadata_paths = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        require(len(metadata_paths) == 1, f"Invalid wheel metadata: {path}")
        metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
        require(metadata["Name"] == "nextral" and metadata["Version"] == version, f"Wrong wheel name/version: {path}")
        require("nextral/py.typed" in archive.namelist(), f"Missing typing marker: {path}")
        require("nextral/_nextral.pyi" in names, f"Missing native type declarations: {path}")
        require(metadata["Requires-Python"] == ">=3.10", f"Unexpected supported Python range: {path}")
        require(path.name.startswith(f"nextral-{version}-"), f"Wrong wheel filename version: {path}")
        wheel_metadata = BytesParser().parsebytes(archive.read(metadata_paths[0].replace("METADATA", "WHEEL")))
        require(wheel_metadata["Root-Is-Purelib"] == "false", f"Expected native wheel: {path}")
        require(all(tag.startswith("cp310-abi3-") for tag in wheel_metadata.get_all("Tag", []))
                and wheel_metadata.get_all("Tag"), f"Expected cp310 abi3 wheel tags: {path}")
        natives = [name for name in archive.namelist() if name.startswith("nextral/_nextral") and name.endswith((".so", ".pyd"))]
        require(len(natives) == 1, f"Expected exactly one native Python module: {path}")
        if "manylinux_2_28_x86_64" in path.name:
            suffix = "linux-x64-gnu"
        elif "manylinux_2_28_aarch64" in path.name:
            suffix = "linux-arm64-gnu"
        elif "macosx_" in path.name and "_x86_64.whl" in path.name:
            suffix = "darwin-x64"
        elif "macosx_" in path.name and "_arm64.whl" in path.name:
            suffix = "darwin-arm64"
        elif path.name.endswith("win_amd64.whl"):
            suffix = "win32-x64-msvc"
        else:
            raise ValueError(f"Unsupported wheel platform: {path.name}")
        native_guard(archive.read(natives[0]), suffix)
        return suffix


def crate_guard(path: Path, version: str, name: str = "nextral") -> None:
    with tarfile.open(path, "r:gz") as archive:
        prefix = f"{name}-{version}/"
        members = archive_members(archive)
        names = {entry for entry, item in members.items() if item.isfile()}
        required = {prefix + entry for entry in ("Cargo.toml", "Cargo.lock")}
        if name == "nextral":
            required.update(prefix + entry for entry in (
                "build.rs", "src/lib.rs", "LICENSE", "README.md",
                "fixtures/evaluation/representative.json",
                "examples/research_host.rs", "examples/research_evaluation.rs",
            ))
            require(any(entry.startswith(prefix + "contracts/") and entry.endswith(".proto") for entry in names), "Crate missing gRPC schema")
            require(any(entry.startswith(prefix + "migrations/") and entry.endswith(".sql") for entry in names), "Crate missing SQL migrations")
        else:
            required.add(prefix + "src/main.rs")
        require(required <= names, f"Crate missing required files: {sorted(required - names)}")
        for entry in names:
            require(entry.startswith(prefix), f"Unexpected crate root: {entry}")
            relative = entry.removeprefix(prefix)
            if relative.startswith("examples/"):
                require(len(PurePosixPath(relative).parts) == 2 and relative.endswith(".rs"),
                        f"Unsafe packaged example: {entry}")
            if relative.startswith("fixtures/"):
                require(relative == "fixtures/evaluation/representative.json", f"Unexpected packaged fixture: {entry}")
            require(not any(part.startswith(".env") or part in {".cargo", ".git", "node_modules", "__pycache__"}
                            for part in PurePosixPath(relative).parts), f"Unsafe packaged file: {entry}")
        import tomllib
        metadata = tomllib.loads(archive.extractfile(prefix + "Cargo.toml").read().decode())
        require(metadata["package"]["name"] == name and metadata["package"]["version"] == version, "Wrong crate name/version")
        for table in [metadata, *metadata.get("target", {}).values()]:
            for section in ("dependencies", "build-dependencies", "dev-dependencies"):
                for alias, dependency in table.get(section, {}).items():
                    if isinstance(dependency, dict):
                        dependency_name = dependency.get("package", alias)
                        if dependency_name in {manifest["package"]["name"] for _, manifest in published_packages()}:
                            require(dependency.get("version", "").removeprefix("^") == version,
                                    f"Wrong packaged workspace dependency version: {name} -> {dependency_name}")


def artifact_guard(directory: Path, tag: str | None, require_checksums: bool = False,
                   require_provenance: bool = False) -> None:
    version = version_guard(tag)
    npm = only_file(directory, "*.tgz")
    npm_guard(npm, version)
    require(npm.name == f"nextral-{version}.tgz", "Wrong npm artifact filename")
    expected = {npm.name}
    for _, manifest in published_packages():
        name = manifest["package"]["name"]
        crate = directory / f"{name}-{version}.crate"
        crate_guard(crate, version, name)
        expected.add(crate.name)
    wheels = sorted(directory.glob("*.whl"))
    platforms = [wheel_guard(path, version) for path in wheels]
    require(len(platforms) == len(NATIVE_TARGETS) and set(platforms) == set(NATIVE_TARGETS), "Release requires exactly one wheel for each supported OS/architecture")
    expected.update(path.name for path in wheels)
    with tarfile.open(npm, "r:gz") as archive:
        for suffix in NATIVE_TARGETS:
            name = f"nextral.{suffix}.node"
            data = (directory / name).read_bytes()
            native_guard(data, suffix)
            require(data == archive.extractfile(f"package/native/{name}").read(),
                    f"Standalone native artifact differs from npm package: {name}")
            expected.add(name)
    require(all(path.is_file() and not path.is_symlink() for path in directory.iterdir()),
            "Release bundle must contain only regular files")
    actual = {path.name for path in directory.iterdir()}
    require(actual - {"SHA256SUMS", "provenance.jsonl"} == expected,
            f"Unexpected release artifacts: {sorted(actual - expected - {'SHA256SUMS', 'provenance.jsonl'})}")
    if require_provenance:
        provenance = directory / "provenance.jsonl"
        require(provenance.is_file() and provenance.stat().st_size > 0, "Missing GitHub provenance bundle")
        for line in provenance.read_text(encoding="utf-8").splitlines():
            require(isinstance(json.loads(line), dict), "Invalid GitHub provenance bundle")
    checksums = directory / "SHA256SUMS"
    contents = "".join(f"{hashlib.sha256((directory / name).read_bytes()).hexdigest()}  {name}\n" for name in sorted(expected))
    if checksums.exists():
        require(checksums.read_text(encoding="utf-8") == contents, "Release checksum mismatch")
    else:
        require(not require_checksums, "Missing release checksums")
        checksums.write_text(contents, encoding="utf-8")
    print(f"Validated {len(expected)} release files and {checksums}")


def provenance_guard(directory: Path, tag: str | None) -> None:
    artifact_guard(directory, tag, require_checksums=True, require_provenance=True)
    repository = os.environ.get("GITHUB_REPOSITORY")
    require(bool(repository), "Set GITHUB_REPOSITORY to verify release provenance")
    for path in sorted(directory.iterdir()):
        if path.name != "provenance.jsonl":
            run("gh", "attestation", "verify", path, "--bundle", directory / "provenance.jsonl",
                "--repo", repository, "--signer-workflow", f"{repository}/.github/workflows/release.yml")


def python_install(directory: Path, interpreter: str | Path = sys.executable) -> None:
    wheel = only_file(directory, "*.whl")
    run(interpreter, "-m", "pip", "install", "--no-deps", "--force-reinstall", wheel)


def python_smoke(directory: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="nextral-wheel-") as temporary:
        consumer = Path(temporary)
        venv.EnvBuilder(with_pip=True).create(consumer / "venv")
        interpreter = consumer / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        python_install(directory, interpreter)
        run(interpreter, "-I", ROOT / "bindings/python/tests/test_consumer.py", cwd=consumer)


def node_smoke(directory: Path) -> None:
    package = only_file(directory, "*.tgz")
    with tempfile.TemporaryDirectory(prefix="nextral-npm-") as temporary:
        consumer = Path(temporary)
        (consumer / "package.json").write_text('{"name":"nextral-consumer","private":true}', encoding="utf-8")
        run("npm", "install", "--ignore-scripts", "--no-audit", "--no-fund", package, cwd=consumer)
        environment = os.environ.copy()
        environment["NEXTRAL_PACKAGE"] = str(consumer / "node_modules/nextral")
        run("node", "--test", ROOT / "bindings/node/test/consumer.test.cjs", cwd=consumer, env=environment)


def extract_crate(package: Path, directory: Path) -> Path:
    with tarfile.open(package, "r:gz") as archive:
        archive_members(archive)
        for item in archive.getmembers():
            require((directory / item.name).resolve().is_relative_to(directory), f"Unsafe crate archive path: {item.name}")
        archive.extractall(directory, filter="data")
    return directory / package.name.removesuffix(".crate")


def cargo_context_env(context: str) -> dict:
    environment = os.environ.copy()
    metadata = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT, env=environment, check=True, capture_output=True, text=True,
    )
    target = Path(json.loads(metadata.stdout)["target_directory"])
    # Archive verification must not replace workspace native artifacts.
    environment["CARGO_TARGET_DIR"] = str(target / context)
    return environment


def crate_package() -> None:
    version_guard()
    arguments = ["cargo", "package", "--locked"]
    for _, manifest in published_packages():
        arguments.extend(["--package", manifest["package"]["name"]])
    # One invocation lets Cargo verify dependent crates using its temporary registry.
    run(*arguments, cwd=ROOT, env=cargo_context_env("crate-package"))


def crate_smoke(directory: Path) -> None:
    version = version_guard()
    environment = cargo_context_env("crate-consumer")
    with tempfile.TemporaryDirectory(prefix="nextral-crate-") as temporary:
        consumer = Path(temporary)
        sources = {}
        for _, manifest in published_packages():
            name = manifest["package"]["name"]
            package = directory / f"{name}-{version}.crate"
            crate_guard(package, version, name)
            sources[name] = extract_crate(package, consumer)
        source = sources["nextral"]
        (consumer / "src").mkdir()
        shutil.copy2(ROOT / "bindings/python/tests/fixtures/test-config.json", consumer / "src/config.json")
        (consumer / "src/main.rs").write_text(
            'fn main() {\n'
            '    nextral::package::configure_runtime_json(include_str!("config.json")).expect("configure");\n'
            '    let result = nextral::package::e2e_smoke_json().expect("run memory scenario");\n'
            '    let smoke: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");\n'
            '    assert_eq!(smoke["ingested"]["status"], "accepted");\n'
            '    let id = &smoke["ingested"]["record_id"];\n'
            '    assert!(smoke["retrieved"]["items"].as_array().expect("retrieval items")\n'
            '        .iter().any(|item| &item["memory_id"] == id));\n'
            '}\n',
            encoding="utf-8",
        )
        members = ", ".join(json.dumps(path.name) for path in sources.values())
        patches = "".join(f"{name} = {{ path = {json.dumps(path.name)} }}\n" for name, path in sources.items())
        (consumer / "Cargo.toml").write_text(
            '[package]\nname = "nextral-release-consumer"\nversion = "0.0.0"\nedition = "2021"\n'
            + f'[workspace]\nmembers = [{members}]\nresolver = "2"\n'
            + '[dependencies]\nserde_json = "1"\nnextral = { path = ' + json.dumps(source.as_posix()) + ', features = ["full"] }\n'
            + '[patch.crates-io]\n' + patches,
            encoding="utf-8",
        )
        run("cargo", "generate-lockfile", "--manifest-path", consumer / "Cargo.toml", cwd=consumer, env=environment)
        run("cargo", "build", "--workspace", "--all-targets", "--all-features", "--locked",
            "--manifest-path", consumer / "Cargo.toml", cwd=consumer, env=environment)
        run("cargo", "run", "--locked", "--package", "nextral-release-consumer",
            "--manifest-path", consumer / "Cargo.toml", cwd=consumer, env=environment)


def crate_publish(directory: Path) -> None:
    reference = os.environ.get("GITHUB_REF", "")
    require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and reference.startswith("refs/tags/v")
            and os.environ.get("PUBLISH_RELEASE") == "true",
            "Registry publishing requires explicit PUBLISH_RELEASE=true in a version-tag workflow_dispatch")
    require(bool(os.environ.get("CARGO_REGISTRY_TOKEN")), "Configure CARGO_REGISTRY_TOKEN in the crates-io environment")
    tag = reference.removeprefix("refs/tags/")
    artifact_guard(directory, tag, require_checksums=True, require_provenance=True)
    version = version_guard(tag)
    environment = cargo_context_env("crate-publish")
    with tempfile.TemporaryDirectory(prefix="nextral-publish-") as temporary:
        for _, manifest in published_packages():
            name = manifest["package"]["name"]
            source = extract_crate(directory / f"{name}-{version}.crate", Path(temporary))
            # Cargo waits for index availability before the next dependent publication.
            run("cargo", "publish", "--locked", "--registry", "crates-io", "--manifest-path", source / "Cargo.toml", cwd=source, env=environment)


def collect(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    require(not any(destination.iterdir()), f"Release output must be empty: {destination}")
    files = sorted(path for path in source.rglob("*") if path.is_file() and path.suffix in {".whl", ".tgz", ".crate", ".node"})
    require(bool(files), f"No release artifacts found in {source}")
    for path in files:
        target = destination / path.name
        require(not target.exists(), f"Duplicate release artifact: {path.name}")
        shutil.copy2(path, target)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    versions = subparsers.add_parser("versions")
    versions.add_argument("--tag")
    artifacts = subparsers.add_parser("artifacts")
    artifacts.add_argument("directory", type=Path)
    artifacts.add_argument("--tag")
    artifacts.add_argument("--require-checksums", action="store_true")
    artifacts.add_argument("--require-provenance", action="store_true")
    provenance = subparsers.add_parser("provenance")
    provenance.add_argument("directory", type=Path)
    provenance.add_argument("--tag")
    subparsers.add_parser("crate-package")
    for command in ("python-install", "python-smoke", "node-smoke", "crate-smoke", "crate-publish"):
        subparsers.add_parser(command).add_argument("directory", type=Path)
    collection = subparsers.add_parser("collect")
    collection.add_argument("source", type=Path)
    collection.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.command == "versions":
        version_guard(args.tag)
    elif args.command == "artifacts":
        artifact_guard(args.directory.resolve(), args.tag, args.require_checksums, args.require_provenance)
    elif args.command == "crate-package":
        crate_package()
    elif args.command == "provenance":
        provenance_guard(args.directory.resolve(), args.tag)
    elif args.command == "collect":
        collect(args.source.resolve(), args.destination.resolve())
    else:
        {"python-install": python_install, "python-smoke": python_smoke, "node-smoke": node_smoke, "crate-smoke": crate_smoke, "crate-publish": crate_publish}[args.command](args.directory.resolve())


if __name__ == "__main__":
    main()
