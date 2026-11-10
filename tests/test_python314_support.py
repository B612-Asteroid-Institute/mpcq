from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path

import pytest

CORE_CANDIDATE = "cd4b24f923d048e8538af88dd8c24b1c10b5e770"
PUBLIC_CORE_REQUIREMENT = "adam-core>=0.7.0,<0.8.0"


def _project_root() -> Path:
    if "MPCQ_REPOSITORY_ROOT" in os.environ:
        configured = os.environ["MPCQ_REPOSITORY_ROOT"]
        assert configured, "explicit repository root must not be empty"
        root = Path(configured).resolve()
    else:
        root = Path(__file__).resolve().parents[1]
    assert (root / "pyproject.toml").is_file(), "project root is incomplete"
    return root


def _repository_root() -> Path:
    root = _project_root()
    if "MPCQ_REPOSITORY_ROOT" in os.environ:
        assert (root / ".git").exists(), "explicit repository root is not a Git checkout"
    elif not (root / ".git").exists():
        pytest.skip("repository-only Python support governance is not shipped in the sdist")
    return root


def _project(root: Path) -> dict[str, object]:
    return tomllib.loads((root / "pyproject.toml").read_text())


def _lock(root: Path) -> dict[str, object]:
    return tomllib.loads((root / "pdm.lock").read_text())


def _verify_python_metadata(project: dict[str, object]) -> None:
    metadata = project["project"]
    assert metadata["requires-python"] == ">=3.11,<3.15"
    assert {
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    } <= set(metadata["classifiers"])
    dependencies = metadata["dependencies"]
    assert PUBLIC_CORE_REQUIREMENT in dependencies
    assert not any("git+" in dependency.lower() for dependency in dependencies)


def _verify_coverage_config(project: dict[str, object]) -> None:
    coverage = project["tool"]["coverage"]
    assert coverage["run"] == {
        "relative_files": True,
        "source": ["mpcq"],
        "omit": ["*/mpcq/tests/*", "mpcq/tests/*"],
    }
    assert coverage["paths"]["source"] == ["src/mpcq", "*/site-packages/mpcq"]
    assert coverage["report"]["omit"] == ["*/mpcq/tests/*", "mpcq/tests/*"]


def _verify_registry_lock(document: dict[str, object]) -> None:
    assert document["metadata"]["targets"] == [{"requires_python": ">=3.11,<3.14"}]
    core = [package for package in document["package"] if package["name"] == "adam-core"]
    assert len(core) == 1
    assert core[0]["version"] == "0.7.0"
    assert set(core[0]["requires_python"].replace(" ", "").split(",")) == {
        ">=3.11",
        "<3.14",
    }
    files = core[0]["files"]
    assert len(files) == 12
    assert all("cp314" not in item["file"] for item in files)
    assert {f"cp{minor}" for minor in (311, 312, 313)} == {
        tag for item in files for tag in ("cp311", "cp312", "cp313") if tag in item["file"]
    }
    assert not ({"git", "ref", "revision", "url", "path"} & core[0].keys())


def _verify_installed_runner(runner: str) -> None:
    assert '"coverage",\n                "run"' in runner
    assert '"--parallel-mode"' in runner
    assert '"coverage",\n                "combine"' in runner
    assert '"coverage",\n                "xml"' in runner
    assert '"--rootdir"' in runner
    assert "_validate_coverage_xml(coverage_xml)" in runner
    assert "src/mpcq/tests/" in runner
    assert "line_rate < 0.55" in runner
    assert "--cov=mpcq" not in runner


def _verify_ci(workflow: str) -> None:
    assert 'python-version: ["3.11", "3.12", "3.13"]' in workflow
    assert workflow.count('python-version: "3.14"') == 1
    assert f'ADAM_CORE_PYTHON314_REF: "{CORE_CANDIDATE}"' in workflow
    assert "repository: B612-Asteroid-Institute/adam_core" in workflow
    assert 'git -C "$core" rev-parse HEAD' in workflow
    assert "build_release_wheel.py" in workflow
    assert "_rust_native.build_identity()" in workflow
    assert "python -m pip check" in workflow
    assert workflow.count("pdm run lint") == 2
    assert workflow.count("pdm run typecheck") == 2
    assert workflow.count("python tests/run_installed_wheel_suite.py") == 2
    assert workflow.count("MPCQ_REPOSITORY_ROOT: ${{ github.workspace }}") == 2
    assert workflow.count("pdm run docs-build") == 2
    assert "pdm run coverage" not in workflow
    assert "codex/nongrav-support" not in workflow
    assert "adam-assist" not in workflow.lower()
    assert re.fullmatch(r"[0-9a-f]{40}", CORE_CANDIDATE)


def _verify_publication_gate(workflow: str) -> None:
    assert "if: ${{ vars.MPCQ_RELEASES_ENABLED == 'true' }}" in workflow
    assert "pypa/gh-action-pypi-publish@release/v1" in workflow


def test_python_metadata_and_core_dependency_are_bounded() -> None:
    _verify_python_metadata(_project(_project_root()))


def test_installed_wheel_coverage_is_repository_relative() -> None:
    root = _repository_root()
    _verify_coverage_config(_project(root))
    _verify_installed_runner((root / "tests/run_installed_wheel_suite.py").read_text())


def test_registry_lock_stays_on_public_core_and_excludes_python314() -> None:
    root = _repository_root()
    _verify_registry_lock(_lock(root))


def test_ci_has_public_and_exact_python314_installed_wheel_lanes() -> None:
    root = _repository_root()
    workflow = (root / ".github/workflows/pip-build-lint-test-coverage.yml").read_text()
    _verify_ci(workflow)


def test_publication_is_fail_closed_until_release_alignment() -> None:
    root = _repository_root()
    workflow = (root / ".github/workflows/publish.yaml").read_text()
    _verify_publication_gate(workflow)


def test_governance_rejects_contract_mutations() -> None:
    root = _repository_root()
    project_text = (root / "pyproject.toml").read_text()
    project = tomllib.loads(project_text)
    lock = (root / "pdm.lock").read_text()
    runner = (root / "tests/run_installed_wheel_suite.py").read_text()
    ci = (root / ".github/workflows/pip-build-lint-test-coverage.yml").read_text()
    publish = (root / ".github/workflows/publish.yaml").read_text()

    with pytest.raises(AssertionError):
        _verify_python_metadata(tomllib.loads(project_text.replace("<3.15", "<3.14", 1)))
    with pytest.raises(AssertionError):
        _verify_coverage_config(
            tomllib.loads(project_text.replace("*/site-packages/mpcq", "*/source/mpcq", 1))
        )
    with pytest.raises(AssertionError):
        _verify_registry_lock(tomllib.loads(lock.replace("cp313", "cp314", 1)))
    with pytest.raises(AssertionError):
        _verify_installed_runner(
            runner.replace(
                '"coverage",\n                "combine"', '"coverage",\n                "xml"', 1
            )
        )
    with pytest.raises(AssertionError):
        _verify_ci(ci.replace(CORE_CANDIDATE, "0" * 40, 1))
    with pytest.raises(AssertionError):
        _verify_ci(ci.replace("python tests/run_installed_wheel_suite.py", "pdm run coverage", 1))
    with pytest.raises(AssertionError):
        _verify_publication_gate(publish.replace("MPCQ_RELEASES_ENABLED", "BYPASS", 1))
    _verify_coverage_config(project)


def test_explicit_repository_root_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MPCQ_REPOSITORY_ROOT", str(tmp_path))
    with pytest.raises(AssertionError, match="project root is incomplete|not a Git checkout"):
        _repository_root()
