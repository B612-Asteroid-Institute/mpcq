# ruff: noqa: F401
import re
import subprocess
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _package_version
from pathlib import Path

from .client import BigQueryMPCClient, MPCClient
from .observations import MPCObservations
from .orbits import MPCOrbits
from .submissions import (
    MPCSubmissionHistory,
    MPCSubmissionResults,
    SubmissionDetails,
    TrksubMapping,
)


def _version_from_scm() -> str | None:
    repo_root = Path(__file__).resolve().parents[2]
    if not (repo_root / ".git").exists():
        return None

    try:
        raw = subprocess.check_output(
            ["git", "-C", str(repo_root), "describe", "--tags", "--long", "--dirty", "--always"],
            text=True,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return None

    match = re.fullmatch(
        r"v?(?P<tag>\d+\.\d+\.\d+)-(?P<count>\d+)-g(?P<sha>[0-9a-f]+)(?P<dirty>-dirty)?",
        raw,
    )
    if not match:
        return None

    tag = match.group("tag")
    count = int(match.group("count"))
    sha = match.group("sha")
    dirty = match.group("dirty") is not None

    if count == 0 and not dirty:
        return tag

    suffix = f".dev{count}+g{sha}"
    if dirty:
        suffix = f"{suffix}.dirty"
    return f"{tag}{suffix}"


try:
    from ._version import __version__
except ImportError:
    scm_version = _version_from_scm()
    if scm_version is not None:
        __version__ = scm_version
    else:
        try:
            __version__ = _package_version("mpcq")
        except PackageNotFoundError:
            __version__ = "0+unknown"
