"""Verify MPCQ release artifacts and plan idempotent PyPI publication."""

from __future__ import annotations

import argparse
import email.policy
import hashlib
import json
import re
import shutil
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from email.message import Message
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import Any, cast

PROJECT = "mpcq"
RELEASE_VERSION = "0.5.0"
RELEASE_SHA_RE = re.compile(r"[0-9a-f]{40}")
EXPECTED_REQUIREMENTS = {"adam-core<0.9.0,>=0.8.0", "adam-core>=0.8.0,<0.9.0"}


class ReleaseError(ValueError):
    """A release artifact or registry state violates the release contract."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _metadata(payload: bytes, source: str) -> Message:
    message = BytesParser(policy=email.policy.default).parsebytes(payload)
    if message["Name"] != PROJECT or message["Version"] != RELEASE_VERSION:
        raise ReleaseError(
            f"{source}: expected {PROJECT} {RELEASE_VERSION}, got "
            f"{message['Name']} {message['Version']}"
        )
    requires_python = {
        item.strip() for item in (message["Requires-Python"] or "").split(",") if item.strip()
    }
    if requires_python != {">=3.11", "<3.15"}:
        raise ReleaseError(f"{source}: unexpected Requires-Python {requires_python}")
    requirements = {
        value.replace(" ", "").lower() for value in message.get_all("Requires-Dist", [])
    }
    if not (requirements & EXPECTED_REQUIREMENTS):
        raise ReleaseError(f"{source}: missing adam-core>=0.8.0,<0.9.0")
    if any("adam-assist" in requirement for requirement in requirements):
        raise ReleaseError(f"{source}: adam-assist must not be a dependency")
    return message


def _verify_version_file(payload: bytes, source: str) -> None:
    text = payload.decode("utf-8")
    if not re.fullmatch(r"__version__ = ['\"]0\.5\.0['\"]\n?", text):
        raise ReleaseError(f"{source}: generated runtime version is not 0.5.0")


def _verify_wheel(path: Path) -> Message:
    expected = f"{PROJECT}-{RELEASE_VERSION}-py3-none-any.whl"
    if path.name != expected:
        raise ReleaseError(f"unexpected wheel filename: {path.name} != {expected}")
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        wheel_names = [name for name in names if name.endswith(".dist-info/WHEEL")]
        version_names = [name for name in names if name == "mpcq/_version.py"]
        if len(metadata_names) != 1 or len(wheel_names) != 1 or len(version_names) != 1:
            raise ReleaseError(f"{path.name}: expected one METADATA, WHEEL, and mpcq/_version.py")
        metadata = _metadata(archive.read(metadata_names[0]), path.name)
        wheel_metadata = BytesParser(policy=email.policy.default).parsebytes(
            archive.read(wheel_names[0])
        )
        if wheel_metadata["Root-Is-Purelib"] != "true":
            raise ReleaseError(f"{path.name}: wheel is not pure Python")
        if wheel_metadata.get_all("Tag", []) != ["py3-none-any"]:
            raise ReleaseError(f"{path.name}: unexpected wheel tags")
        _verify_version_file(archive.read(version_names[0]), f"{path.name}:mpcq/_version.py")
        return metadata


def _verify_sdist(path: Path) -> Message:
    expected = f"{PROJECT}-{RELEASE_VERSION}.tar.gz"
    if path.name != expected:
        raise ReleaseError(f"unexpected sdist filename: {path.name} != {expected}")
    root = f"{PROJECT}-{RELEASE_VERSION}"
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            pure = PurePosixPath(member.name)
            if pure.is_absolute() or ".." in pure.parts or not pure.parts or pure.parts[0] != root:
                raise ReleaseError(f"{path.name}: unsafe or unexpected member {member.name}")
            if member.issym() or member.islnk():
                raise ReleaseError(f"{path.name}: links are forbidden ({member.name})")
        metadata_members = [member for member in members if member.name == f"{root}/PKG-INFO"]
        version_members = [
            member for member in members if member.name == f"{root}/src/mpcq/_version.py"
        ]
        if len(metadata_members) != 1 or len(version_members) != 1:
            raise ReleaseError(f"{path.name}: expected PKG-INFO and generated _version.py")
        metadata_stream = archive.extractfile(metadata_members[0])
        version_stream = archive.extractfile(version_members[0])
        if metadata_stream is None or version_stream is None:
            raise ReleaseError(f"{path.name}: release metadata is not a regular file")
        metadata = _metadata(metadata_stream.read(), path.name)
        _verify_version_file(version_stream.read(), f"{path.name}:src/mpcq/_version.py")
        return metadata


def artifact_records(dist: Path) -> list[dict[str, Any]]:
    if not dist.is_dir():
        raise ReleaseError(f"distribution directory does not exist: {dist}")
    entries = sorted(path for path in dist.iterdir() if path.is_file())
    expected_names = {
        f"{PROJECT}-{RELEASE_VERSION}-py3-none-any.whl",
        f"{PROJECT}-{RELEASE_VERSION}.tar.gz",
    }
    names = {path.name for path in entries}
    if names != expected_names:
        raise ReleaseError(f"expected exactly {sorted(expected_names)}, got {sorted(names)}")
    wheel = next(path for path in entries if path.suffix == ".whl")
    sdist = next(path for path in entries if path.name.endswith(".tar.gz"))
    wheel_metadata = _verify_wheel(wheel)
    sdist_metadata = _verify_sdist(sdist)
    for field in ("Name", "Version", "Requires-Python"):
        if wheel_metadata[field] != sdist_metadata[field]:
            raise ReleaseError(f"wheel/sdist metadata mismatch for {field}")
    return [
        {"filename": path.name, "sha256": sha256(path), "size": path.stat().st_size}
        for path in entries
    ]


def expected_manifest(dist: Path, release_sha: str) -> dict[str, Any]:
    if not RELEASE_SHA_RE.fullmatch(release_sha):
        raise ReleaseError("release SHA must be a full lowercase 40-character Git SHA")
    return {
        "schema_version": 1,
        "project": PROJECT,
        "version": RELEASE_VERSION,
        "release_sha": release_sha,
        "artifacts": artifact_records(dist),
    }


def expected_checksums(manifest: dict[str, Any]) -> str:
    return "".join(f"{item['sha256']}  dist/{item['filename']}\n" for item in manifest["artifacts"])


def create_bundle(dist: Path, release_sha: str, manifest_path: Path, checksums_path: Path) -> None:
    manifest = expected_manifest(dist, release_sha)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    checksums_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    checksums_path.write_text(expected_checksums(manifest), encoding="utf-8")


def verify_bundle(
    dist: Path, release_sha: str, manifest_path: Path, checksums_path: Path
) -> dict[str, Any]:
    expected = expected_manifest(dist, release_sha)
    try:
        actual = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseError(f"cannot read candidate manifest: {error}") from error
    if actual != expected:
        raise ReleaseError("candidate manifest does not match the exact artifact bytes")
    try:
        checksums = checksums_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ReleaseError(f"cannot read candidate checksums: {error}") from error
    if checksums != expected_checksums(expected):
        raise ReleaseError("SHA256SUMS does not match the exact artifact bytes")
    return expected


def _fetch_release(registry: str, project: str, version: str) -> dict[str, Any] | None:
    if not registry.startswith(("https://", "http://")):
        raise ReleaseError("registry must use HTTP(S)")
    url = (
        f"{registry.rstrip('/')}/pypi/{urllib.parse.quote(project)}/"
        f"{urllib.parse.quote(version)}/json"
    )
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "mpcq-release-verifier/1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
            if not isinstance(payload, dict):
                raise ReleaseError("registry response is not a JSON object")
            return cast(dict[str, Any], payload)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise ReleaseError(f"registry request failed: HTTP {error.code}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise ReleaseError(f"registry request failed: {error}") from error


def plan_publication(manifest: dict[str, Any], release: dict[str, Any] | None) -> dict[str, Any]:
    candidates = {item["filename"]: item for item in manifest["artifacts"]}
    remote: dict[str, dict[str, Any]] = {}
    if release is not None:
        info = release.get("info", {})
        if info.get("name", "").lower().replace("_", "-") != PROJECT:
            raise ReleaseError("registry response is for the wrong project")
        if info.get("version") != RELEASE_VERSION:
            raise ReleaseError("registry response is for the wrong version")
        urls = release.get("urls")
        if not isinstance(urls, list):
            raise ReleaseError("registry response has no release file list")
        for item in urls:
            filename = item.get("filename")
            digest = item.get("digests", {}).get("sha256")
            if not isinstance(filename, str) or not isinstance(digest, str):
                raise ReleaseError("registry release file is missing filename or SHA-256")
            if filename in remote:
                raise ReleaseError(f"registry contains duplicate filename: {filename}")
            if item.get("yanked", False):
                raise ReleaseError(f"registry file is yanked: {filename}")
            remote[filename] = {"filename": filename, "sha256": digest}
    unexpected = sorted(set(remote) - set(candidates))
    if unexpected:
        raise ReleaseError(f"registry contains unexpected release files: {unexpected}")
    for filename, item in remote.items():
        if item["sha256"] != candidates[filename]["sha256"]:
            raise ReleaseError(f"registry digest mismatch for {filename}")
    missing = sorted(set(candidates) - set(remote))
    state = "complete" if not missing else ("partial" if remote else "absent")
    return {
        "schema_version": 1,
        "project": PROJECT,
        "version": RELEASE_VERSION,
        "release_sha": manifest["release_sha"],
        "state": state,
        "candidate": [candidates[name] for name in sorted(candidates)],
        "remote": [remote[name] for name in sorted(remote)],
        "missing": missing,
        "missing_count": len(missing),
    }


def registry_state(
    manifest: dict[str, Any], registry: str, *, require_complete: bool, retries: int, delay: float
) -> dict[str, Any]:
    if retries < 1:
        raise ReleaseError("retries must be at least one")
    for attempt in range(retries):
        state = plan_publication(manifest, _fetch_release(registry, PROJECT, RELEASE_VERSION))
        if not require_complete or state["state"] == "complete":
            return state
        if attempt + 1 < retries:
            time.sleep(delay)
    raise ReleaseError(
        f"registry release remained {state['state']} after {retries} verification attempts"
    )


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("create", "verify", "prepare-pypi", "verify-pypi"):
        child = subparsers.add_parser(command)
        child.add_argument("--dist", type=Path, required=True)
        child.add_argument("--release-sha", required=True)
        child.add_argument("--version", required=True)
        child.add_argument("--manifest", type=Path, required=True)
        child.add_argument("--checksums", type=Path, required=True)
        if command in {"prepare-pypi", "verify-pypi"}:
            child.add_argument("--registry", default="https://pypi.org")
            child.add_argument("--state-file", type=Path, required=True)
            child.add_argument("--retries", type=int, default=1)
            child.add_argument("--retry-delay", type=float, default=5.0)
        if command == "prepare-pypi":
            child.add_argument("--upload-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    if args.version != RELEASE_VERSION:
        raise SystemExit(f"expected release version {RELEASE_VERSION}, got {args.version}")
    try:
        if args.command == "create":
            create_bundle(args.dist, args.release_sha, args.manifest, args.checksums)
            return
        manifest = verify_bundle(args.dist, args.release_sha, args.manifest, args.checksums)
        if args.command == "verify":
            return
        require_complete = args.command == "verify-pypi"
        state = registry_state(
            manifest,
            args.registry,
            require_complete=require_complete,
            retries=args.retries,
            delay=args.retry_delay,
        )
        if args.command == "prepare-pypi":
            if args.upload_dir.exists():
                shutil.rmtree(args.upload_dir)
            args.upload_dir.mkdir(parents=True)
            for filename in state["missing"]:
                shutil.copy2(args.dist / filename, args.upload_dir / filename)
        _write_state(args.state_file, state)
    except ReleaseError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
