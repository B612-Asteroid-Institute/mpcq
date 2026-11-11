"""Run MPCQ's complete suite against the installed wheel from a clean directory."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from importlib import metadata
from pathlib import Path

RUNTIME_MODULES = {
    "src/mpcq/__init__.py",
    "src/mpcq/client.py",
    "src/mpcq/observations.py",
    "src/mpcq/orbits.py",
    "src/mpcq/submissions.py",
}


def _explicit_repository_root() -> Path:
    configured = os.environ.get("MPCQ_REPOSITORY_ROOT")
    if not configured:
        raise SystemExit("MPCQ_REPOSITORY_ROOT must name the immutable CI checkout")
    root = Path(configured).resolve()
    if not (root / ".git").exists() or not (root / "pyproject.toml").is_file():
        raise SystemExit(f"MPCQ_REPOSITORY_ROOT is not a Git checkout: {root}")
    return root


def _validate_coverage_xml(path: Path) -> None:
    root = ET.parse(path).getroot()
    filenames = {
        item.attrib["filename"] for item in root.findall("./packages/package/classes/class")
    }
    absolute = sorted(filename for filename in filenames if Path(filename).is_absolute())
    installed = sorted(filename for filename in filenames if "site-packages" in filename)
    shipped_tests = sorted(
        filename
        for filename in filenames
        if filename.startswith("src/mpcq/tests/") or "/mpcq/tests/" in filename
    )
    missing_runtime = sorted(RUNTIME_MODULES - filenames)
    line_rate = float(root.attrib["line-rate"])
    if absolute or installed or shipped_tests or missing_runtime or line_rate < 0.55:
        raise SystemExit(
            "invalid installed-wheel coverage XML: "
            f"{absolute=} {installed=} {shipped_tests=} {missing_runtime=} {line_rate=}"
        )
    print(f"Coverage XML provenance: {sorted(filenames)}; line-rate={line_rate:.4f}")


def main() -> None:
    repository = _explicit_repository_root()

    import mpcq

    module = Path(mpcq.__file__).resolve()
    distribution_root = Path(metadata.distribution("mpcq").locate_file("")).resolve()
    if module.is_relative_to(repository / "src"):
        raise SystemExit(f"mpcq resolved from the source checkout, not the wheel: {module}")
    if not module.is_relative_to(distribution_root):
        raise SystemExit(
            f"mpcq module {module} is outside installed distribution {distribution_root}"
        )
    print(f"Installed MPCQ provenance: {module}")

    coverage_data = repository / ".coverage"
    coverage_xml = repository / "coverage.xml"
    coverage_data.unlink(missing_ok=True)
    coverage_xml.unlink(missing_ok=True)

    with tempfile.TemporaryDirectory(prefix="mpcq-installed-wheel-tests-") as temporary:
        test_root = Path(temporary)
        shutil.copytree(repository / "src/mpcq/tests", test_root / "mpcq_package_tests")
        shutil.copytree(repository / "tests", test_root / "repository_tests")
        (test_root / "test_installed_wheel_provenance.py").write_text(
            """from importlib import metadata
import os
from pathlib import Path


def test_mpcq_is_loaded_from_the_installed_wheel():
    import mpcq

    repository = Path(os.environ["MPCQ_REPOSITORY_ROOT"]).resolve()
    module = Path(mpcq.__file__).resolve()
    distribution = Path(metadata.distribution("mpcq").locate_file("")).resolve()
    assert not module.is_relative_to(repository / "src"), module
    assert module.is_relative_to(distribution), (module, distribution)
    print(f"pytest MPCQ provenance: {module}")
""",
            encoding="utf-8",
        )
        environment = os.environ.copy()
        environment["MPCQ_REPOSITORY_ROOT"] = str(repository)
        environment["PYTHONNOUSERSITE"] = "1"
        environment.pop("PYTHONPATH", None)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "coverage",
                "run",
                f"--rcfile={repository / 'pyproject.toml'}",
                "--parallel-mode",
                "-m",
                "pytest",
                "-c",
                str(repository / "pyproject.toml"),
                "--rootdir",
                str(test_root),
                "--benchmark-disable",
                "mpcq_package_tests",
                "repository_tests",
                "test_installed_wheel_provenance.py",
            ],
            cwd=test_root,
            env=environment,
            check=True,
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "coverage",
                "combine",
                f"--rcfile={repository / 'pyproject.toml'}",
                str(test_root),
            ],
            cwd=repository,
            env=environment,
            check=True,
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "coverage",
                "xml",
                f"--rcfile={repository / 'pyproject.toml'}",
                "-o",
                str(coverage_xml),
            ],
            cwd=repository,
            env=environment,
            check=True,
        )

    _validate_coverage_xml(coverage_xml)
    coverage_data.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
