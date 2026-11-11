from __future__ import annotations

import hashlib
import os
import tomllib
from collections.abc import Mapping
from pathlib import Path

import pytest

PUBLIC_CORE_VERSION = "0.8.0"
PUBLIC_CORE_REQUIREMENT = "adam-core>=0.8.0,<0.9.0"
PYTHON_MATRIX = ("3.11", "3.12", "3.13", "3.14")
PYTHON_SUPPORT_WORKFLOW_SHA256 = "15bea65b21b68dfcb644de7bfe1af8c7fce52959a7ec442ccfe991eac0ca5c9d"


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
    assert {f"Programming Language :: Python :: {version}" for version in PYTHON_MATRIX} <= set(
        metadata["classifiers"]
    )
    dependencies = metadata["dependencies"]
    assert PUBLIC_CORE_REQUIREMENT in dependencies
    assert not any("git+" in dependency.lower() for dependency in dependencies)


def _verify_coverage_config(project: dict[str, object]) -> None:
    coverage = project["tool"]["coverage"]
    expected_omit = [
        "*/mpcq/tests/*",
        "mpcq/tests/*",
        "*/mpcq/_version.py",
        "mpcq/_version.py",
    ]
    assert coverage["run"] == {
        "relative_files": True,
        "source": ["mpcq"],
        "omit": expected_omit,
    }
    assert coverage["paths"]["source"] == ["src/mpcq", "*/site-packages/mpcq"]
    assert coverage["report"]["omit"] == expected_omit


def _verify_registry_lock(document: dict[str, object]) -> None:
    assert document["metadata"]["targets"] == [{"requires_python": ">=3.11,<3.15"}]
    assert document["metadata"]["strategy"] == ["inherit_metadata"]
    packages = document["package"]
    versions = {package["name"]: package["version"] for package in packages}
    assert {
        "numpy": "2.4.6",
        "pandas": "3.1.1",
        "pyarrow": "26.0.1",
        "astropy": "8.0.2",
        "grpcio": "1.85.1",
    }.items() <= versions.items()
    assert all(
        not ({"git", "ref", "revision", "url", "path", "editable"} & package.keys())
        for package in packages
    )
    core = [package for package in packages if package["name"] == "adam-core"]
    assert len(core) == 1
    assert core[0]["version"] == PUBLIC_CORE_VERSION
    assert set(core[0]["requires_python"].replace(" ", "").split(",")) == {
        ">=3.11",
        "<3.15",
    }
    files = core[0]["files"]
    assert len(files) == 16
    assert {f"cp{minor}" for minor in (311, 312, 313, 314)} == {
        tag for item in files for tag in ("cp311", "cp312", "cp313", "cp314") if tag in item["file"]
    }
    assert not ({"git", "ref", "revision", "url", "path", "editable"} & core[0].keys())


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


def _load_unique_yaml(workflow: str) -> Mapping[str, object]:
    import yaml
    from yaml.resolver import BaseResolver

    class UniqueKeyLoader(yaml.SafeLoader):
        pass

    def construct_unique_mapping(
        loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False
    ) -> dict[object, object]:
        loader.flatten_mapping(node)
        mapping: dict[object, object] = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            assert key not in mapping, f"duplicate workflow key: {key!r}"
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    UniqueKeyLoader.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG, construct_unique_mapping)
    document = yaml.load(workflow, Loader=UniqueKeyLoader)
    assert isinstance(document, Mapping)
    return document


def _exact_keys(mapping: Mapping[str, object], expected: set[str]) -> None:
    assert set(mapping) == expected


def _verify_workflow_digest(workflow: bytes) -> None:
    actual = hashlib.sha256(workflow).hexdigest()
    assert actual == PYTHON_SUPPORT_WORKFLOW_SHA256, (
        f"Python support workflow SHA-256 drift: {actual}"
    )


def _verify_ci(workflow: bytes) -> None:
    _verify_workflow_digest(workflow)
    workflow_text = workflow.decode("utf-8")
    document = _load_unique_yaml(workflow_text)
    assert document["env"] == {"ADAM_CORE_PUBLIC_VERSION": "0.8.0"}

    jobs = document["jobs"]
    assert isinstance(jobs, Mapping)
    assert set(jobs) == {"python-support"}
    job = jobs["python-support"]
    assert isinstance(job, Mapping)
    _exact_keys(job, {"name", "runs-on", "strategy", "defaults", "steps"})
    assert job["name"] == "Python ${{ matrix.python-version }} support"
    assert job["runs-on"] == "ubuntu-latest"
    assert job["strategy"] == {
        "fail-fast": False,
        "matrix": {"python-version": list(PYTHON_MATRIX)},
    }
    assert job["defaults"] == {"run": {"shell": "bash -l {0}"}}

    steps = job["steps"]
    assert isinstance(steps, list)
    assert len(steps) == 10
    assert all(isinstance(step, Mapping) for step in steps)

    checkout, setup, build, verify, frozen, lint, typing, tests, docs, coveralls = steps
    _exact_keys(checkout, {"name", "uses", "with"})
    assert checkout == {
        "name": "Checkout MPCQ",
        "uses": "actions/checkout@v4",
        "with": {"fetch-depth": 0},
    }
    _exact_keys(setup, {"uses", "with"})
    assert setup == {
        "uses": "actions/setup-python@v5",
        "with": {"python-version": "${{ matrix.python-version }}", "cache": "pip"},
    }

    expected_run_steps = {
        "Build and install public wheel pair": build,
        "Verify public binary Core, installed MPCQ, lock, and clean tree": verify,
        "Verify frozen lock from public binary artifacts": frozen,
        "Lint": lint,
        "Type check": typing,
        "Test the installed MPCQ wheel with coverage": tests,
        "Build documentation": docs,
    }
    for name, step in expected_run_steps.items():
        assert step["name"] == name
        assert "if" not in step and "continue-on-error" not in step
        command = step["run"]
        assert isinstance(command, str)
        lines = command.splitlines()
        if len(lines) > 1:
            assert lines[0] == "set -euo pipefail"

    _exact_keys(build, {"name", "run"})
    build_run = build["run"]
    assert 'python -m pip install "build==1.3.0"' in build_run
    assert "python -m build --wheel" in build_run
    assert '"adam-core==$ADAM_CORE_PUBLIC_VERSION"' in build_run

    _exact_keys(verify, {"name", "env", "run"})
    assert verify["env"] == {"MATRIX_PYTHON_VERSION": "${{ matrix.python-version }}"}
    verify_run = verify["run"]
    assert "python -m pip check" in verify_run
    assert "_rust_native.build_identity()" in verify_run
    assert "pdm lock --check" in verify_run
    assert "git diff --exit-code" in verify_run

    _exact_keys(frozen, {"name", "env", "run"})
    assert frozen["env"] == {"MATRIX_PYTHON_VERSION": "${{ matrix.python-version }}"}
    frozen_run = frozen["run"]
    assert 'locked_venv="$RUNNER_TEMP/locked-dev-venv"' in frozen_run
    assert 'pdm export --group dev --output "$requirements"' in frozen_run
    assert 'python -m virtualenv "$locked_venv"' in frozen_run
    assert 'locked_python="$locked_venv/bin/python"' in frozen_run
    assert "--only-binary=:all: --require-hashes --requirement" in frozen_run
    assert frozen_run.count('"$locked_python" -I') == 4
    assert frozen_run.count("env -u PYTHONPATH") == 4
    assert 'find_spec("mpcq") is None' in frozen_run

    assert lint == {"name": "Lint", "run": "pdm run lint"}
    assert typing == {"name": "Type check", "run": "pdm run typecheck"}
    assert tests == {
        "name": "Test the installed MPCQ wheel with coverage",
        "env": {"MPCQ_REPOSITORY_ROOT": "${{ github.workspace }}"},
        "run": "python tests/run_installed_wheel_suite.py",
    }
    assert docs == {"name": "Build documentation", "run": "pdm run docs-build"}
    _exact_keys(coveralls, {"name", "uses", "with"})
    assert coveralls == {
        "name": "Coverage report to Coveralls",
        "uses": "coverallsapp/github-action@v2.0.0",
        "with": {"path-to-lcov": "coverage.xml"},
    }

    assert "build_release_wheel.py" not in workflow_text
    assert "rust-toolchain" not in workflow_text
    assert "maturin" not in workflow_text
    assert "B612-Asteroid-Institute/adam_core" not in workflow_text
    assert "candidate" not in workflow_text.lower()
    assert "python314" not in workflow_text.lower()
    assert "codex/nongrav-support" not in workflow_text
    assert "adam-assist" not in workflow_text.lower()


def _stored_adversarial_workflow_mutations(workflow: str) -> dict[str, str]:
    mutations: dict[str, str] = {}

    def add(name: str, old: str, new: str) -> None:
        assert old in workflow, (name, old)
        mutation = workflow.replace(old, new, 1)
        assert mutation != workflow
        assert mutation not in mutations.values()
        mutations[name] = mutation

    add("include plain", "      matrix:\n", "      matrix:\n        include: []\n")
    add("include double quoted", "      matrix:\n", '      matrix:\n        "include" : []\n')
    add("include single quoted", "      matrix:\n", "      matrix:\n        'include': []\n")
    add("exclude spaced", "      matrix:\n", "      matrix:\n        exclude : []\n")
    add("exclude double quoted", "      matrix:\n", '      matrix:\n        "exclude": []\n')
    add("exclude single quoted", "      matrix:\n", "      matrix:\n        'exclude' : []\n")
    add(
        "extra job normal",
        "  python-support:\n",
        "  escape:\n    runs-on: windows-latest\n    steps: []\n  python-support:\n",
    )
    add("extra job empty", "  python-support:\n", "  escape: {}\n  python-support:\n")
    add("extra job quoted", "  python-support:\n", "  'escape' : {}\n  python-support:\n")
    add(
        "duplicate runner",
        "    runs-on: ubuntu-latest\n",
        "    runs-on: ubuntu-latest\n    runs-on : ubuntu-latest\n",
    )
    add(
        "job if spaced",
        "    name: Python ${{ matrix.python-version }} support\n",
        "    if : true\n    name: Python ${{ matrix.python-version }} support\n",
    )
    add(
        "job if double quoted",
        "    name: Python ${{ matrix.python-version }} support\n",
        '    "if": true\n    name: Python ${{ matrix.python-version }} support\n',
    )
    add(
        "job if single quoted",
        "    name: Python ${{ matrix.python-version }} support\n",
        "    'if' : true\n    name: Python ${{ matrix.python-version }} support\n",
    )
    add("step if plain", "      - name: Lint\n", "      - name: Lint\n        if: true\n")
    add(
        "step if quoted",
        "      - name: Lint\n",
        '      - name: Lint\n        "if" : true\n',
    )
    add(
        "job continue plain",
        "    runs-on: ubuntu-latest\n",
        "    continue-on-error : true\n    runs-on: ubuntu-latest\n",
    )
    add(
        "job continue quoted",
        "    runs-on: ubuntu-latest\n",
        "    'continue-on-error': true\n    runs-on: ubuntu-latest\n",
    )
    add(
        "step continue plain",
        "      - name: Lint\n",
        "      - name: Lint\n        continue-on-error: true\n",
    )
    add(
        "step continue quoted",
        "      - name: Lint\n",
        '      - name: Lint\n        "continue-on-error" : true\n',
    )
    add("mask true", "          python -m pip check\n", "          python -m pip check || true\n")
    add("mask colon", "          python -m pip check\n", "          python -m pip check || :\n")
    add(
        "mask exit zero",
        "          python -m pip check\n",
        "          python -m pip check || exit 0\n",
    )
    add(
        "mask echo",
        "          python -m pip check\n",
        "          python -m pip check || echo ignored\n",
    )
    add(
        "mask split line",
        "          python -m pip check\n",
        "          python -m pip check ||\n            true\n",
    )
    add("strict removed", "          set -euo pipefail\n", "          echo no-strict-mode\n")
    add("strict weakened", "          set -euo pipefail\n", "          set -uo pipefail\n")
    add(
        "strict delayed by blank",
        "        run: |\n          set -euo pipefail\n",
        "        run: |\n\n          set -euo pipefail\n",
    )
    add(
        "errexit disabled short",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          set +e\n",
    )
    add(
        "errexit disabled long",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          set +o errexit\n",
    )
    add(
        "shell version if",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n"
        '          if [ "$MATRIX_PYTHON_VERSION" = "3.14" ]; then exit 0; fi\n',
    )
    add(
        "shell version test",
        "          set -euo pipefail\n",
        '          set -euo pipefail\n          test "$MATRIX_PYTHON_VERSION" = "3.14" && exit 0\n',
    )
    add(
        "shell version bracket",
        "          set -euo pipefail\n",
        '          set -euo pipefail\n          [ "$MATRIX_PYTHON_VERSION" = "3.14" ] && exit 0\n',
    )
    add(
        "shell version case",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n"
        '          case "$MATRIX_PYTHON_VERSION" in 3.14) exit 0;; esac\n',
    )
    add(
        "matrix expression in run",
        "          python -m pip check\n",
        '          echo "${{ matrix.python-version }}"\n          python -m pip check\n',
    )
    add(
        "Python heredoc version branch",
        "          import adam_core\n",
        '          if os.environ["MATRIX_PYTHON_VERSION"] == "3.14": raise SystemExit(0)\n'
        "          import adam_core\n",
    )
    add(
        "setup Python hardcoded",
        "python-version: ${{ matrix.python-version }}",
        'python-version: "3.14"',
    )
    add(
        "Core acceptance conditional",
        "      - name: Verify public binary Core, installed MPCQ, lock, and clean tree\n",
        "      - name: Verify public binary Core, installed MPCQ, lock, and clean tree\n"
        "        if: matrix.python-version != '3.14'\n",
    )
    add(
        "frozen acceptance conditional",
        "      - name: Verify frozen lock from public binary artifacts\n",
        "      - name: Verify frozen lock from public binary artifacts\n"
        "        if: matrix.python-version != '3.14'\n",
    )
    add(
        "tests conditional",
        "      - name: Test the installed MPCQ wheel with coverage\n",
        "      - name: Test the installed MPCQ wheel with coverage\n"
        "        if: matrix.python-version != '3.14'\n",
    )
    add(
        "no-binary override",
        "--only-binary=:all: --require-hashes",
        "--no-binary=:all: --require-hashes",
    )
    add("only-binary wrong", "--only-binary=:all:", "--only-binary=:none:")
    add(
        "only-binary removed",
        "--only-binary=:all: --require-hashes",
        "--require-hashes",
    )
    add(
        "require-hashes removed",
        "--only-binary=:all: --require-hashes",
        "--only-binary=:all:",
    )
    add(
        "one isolated flag removed",
        '"$locked_python" -I -m pip check',
        '"$locked_python" -m pip check',
    )
    add("all isolated flags removed", '"$locked_python" -I', '"$locked_python"')
    assert len(mutations) == 45
    return mutations


def _representative_semantic_workflow_mutations(workflow: str) -> dict[str, str]:
    mutations: dict[str, str] = {}

    def add(name: str, old: str, new: str) -> None:
        assert old in workflow, (name, old)
        mutation = workflow.replace(old, new, 1)
        assert mutation != workflow
        assert mutation not in mutations.values()
        mutations[name] = mutation

    add("negated command", "          python -m pip check\n", "          ! python -m pip check\n")
    add(
        "and-colon masking",
        "          python -m pip check\n",
        "          python -m pip check && :\n",
    )
    add(
        "and-true masking",
        "          python -m pip check\n",
        "          python -m pip check && true\n",
    )
    add(
        "ERR trap masking",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          trap 'exit 0' ERR\n",
    )
    add(
        "EXIT trap masking",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          trap 'exit 0' EXIT\n",
    )
    add(
        "combined option disablement",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          set +ue\n",
    )
    add(
        "mixed option disablement",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          set -u +e\n",
    )
    add(
        "pipefail disablement",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          set +o pipefail\n",
    )
    add(
        "shopt errexit disablement",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n          shopt -u -o errexit\n",
    )
    add(
        "indirect sys.version_info branch",
        "          import adam_core\n",
        "          if sys.version_info >= (3, 14): raise SystemExit(0)\n"
        "          import adam_core\n",
    )
    add(
        "pythonLocation branch",
        "          set -euo pipefail\n",
        "          set -euo pipefail\n"
        '          if [[ "$pythonLocation" == *3.14* ]]; then exit 0; fi\n',
    )
    add(
        "toJSON matrix branch",
        "          python -m pip check\n",
        '          echo "${{ toJSON(matrix) }}"\n          python -m pip check\n',
    )
    add(
        "trailing no-binary override",
        '--only-binary=:all: --require-hashes --requirement "$requirements"',
        '--only-binary=:all: --require-hashes --requirement "$requirements" --only-binary=:none:',
    )
    add(
        "isolated-flag comment spoof",
        '          env -u PYTHONPATH "$locked_python" -I -m pip check\n',
        '          # "$locked_python" -I\n'
        '          env -u PYTHONPATH "$locked_python" -m pip check\n',
    )
    assert len(mutations) == 14
    return mutations


def _verify_publication_gate(workflow: str) -> None:
    assert "workflow_dispatch:" in workflow
    assert "push:" not in workflow
    assert "if: ${{ vars.MPCQ_RELEASES_ENABLED == 'true' }}" in workflow
    assert "environment:" in workflow and "name: pypi" in workflow
    assert "actions/download-artifact@v4" in workflow
    assert "release_artifacts.py prepare-pypi" in workflow
    assert "pypa/gh-action-pypi-publish@release/v1" in workflow
    assert "release_artifacts.py verify-pypi" in workflow
    assert "python -m build" not in workflow and "pdm build" not in workflow


def test_python_metadata_and_core_dependency_are_bounded() -> None:
    _verify_python_metadata(_project(_project_root()))


def test_installed_wheel_coverage_is_repository_relative() -> None:
    root = _repository_root()
    _verify_coverage_config(_project(root))
    _verify_installed_runner((root / "tests/run_installed_wheel_suite.py").read_text())


def test_registry_lock_covers_the_public_python_matrix() -> None:
    root = _repository_root()
    _verify_registry_lock(_lock(root))


def test_ci_has_one_uniform_public_core_python_matrix() -> None:
    root = _repository_root()
    workflow = (root / ".github/workflows/pip-build-lint-test-coverage.yml").read_bytes()
    assert hashlib.sha256(workflow).hexdigest() == PYTHON_SUPPORT_WORKFLOW_SHA256
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
    ci_bytes = (root / ".github/workflows/pip-build-lint-test-coverage.yml").read_bytes()
    ci = ci_bytes.decode("utf-8")
    publish = (root / ".github/workflows/publish.yaml").read_text()

    stored_mutations = _stored_adversarial_workflow_mutations(ci)
    semantic_mutations = _representative_semantic_workflow_mutations(ci)
    digest_mutations = {
        "ordinary byte drift": ci_bytes + b"# drift\n",
        "CRLF line endings": ci_bytes.replace(b"\n", b"\r\n"),
        "CR-only line endings": ci_bytes.replace(b"\n", b"\r"),
        **{name: mutation.encode() for name, mutation in stored_mutations.items()},
        **{name: mutation.encode() for name, mutation in semantic_mutations.items()},
    }
    assert len(digest_mutations) == 62
    for mutation in digest_mutations.values():
        with pytest.raises(AssertionError, match="workflow SHA-256 drift"):
            _verify_workflow_digest(mutation)

    with pytest.raises(AssertionError):
        _verify_python_metadata(tomllib.loads(project_text.replace("<3.15", "<3.14", 1)))
    with pytest.raises(AssertionError):
        _verify_coverage_config(
            tomllib.loads(project_text.replace("*/site-packages/mpcq", "*/source/mpcq", 1))
        )
    with pytest.raises(AssertionError):
        _verify_registry_lock(tomllib.loads(lock.replace("cp314", "cp313")))
    with pytest.raises(AssertionError):
        _verify_installed_runner(
            runner.replace(
                '"coverage",\n                "combine"', '"coverage",\n                "xml"', 1
            )
        )

    def mutate_ci(old: str, new: str) -> str:
        assert old in ci
        return ci.replace(old, new, 1)

    ci_mutations = [
        mutate_ci('"3.13", "3.14"', '"3.13"'),
        mutate_ci("      matrix:\n", '      matrix:\n        "include" : []\n'),
        mutate_ci(
            '        python-version: ["3.11", "3.12", "3.13", "3.14"]\n',
            '        python-version: ["3.11", "3.12", "3.13", "3.14"]\n'
            '        exclude :\n          - python-version: "3.14"\n',
        ),
        mutate_ci("  python-support:\n", "  unexpected-job: {}\n  python-support:\n"),
        mutate_ci(
            "    name: Python ${{ matrix.python-version }} support\n",
            '    "if" : true\n    name: Python ${{ matrix.python-version }} support\n',
        ),
        mutate_ci(
            "    runs-on: ubuntu-latest\n",
            "    'continue-on-error' : true\n    runs-on: ubuntu-latest\n",
        ),
        mutate_ci(
            "      - name: Lint\n",
            '      - name: Lint\n        "continue-on-error" : true\n',
        ),
        mutate_ci(
            "      - name: Verify public binary Core, installed MPCQ, lock, and clean tree\n",
            "      - name: Verify public binary Core, installed MPCQ, lock, and clean tree\n"
            "        'if' : matrix.python-version != '3.14'\n",
        ),
        mutate_ci("          python -m pip check\n", "          python -m pip check || true\n"),
        mutate_ci("          python -m pip check\n", "          python -m pip check || :\n"),
        mutate_ci("          python -m pip check\n", "          python -m pip check || exit 0\n"),
        mutate_ci(
            "          set -euo pipefail\n",
            "          set -euo pipefail\n          set +e\n",
        ),
        mutate_ci(
            "          set -euo pipefail\n",
            "          set -euo pipefail\n          set +o errexit\n",
        ),
        mutate_ci("          set -euo pipefail\n", "          echo no-strict-mode\n"),
        mutate_ci("          set -euo pipefail\n", "          set -uo pipefail\n"),
        mutate_ci(
            "          set -euo pipefail\n",
            "          set -euo pipefail\n"
            '          if [ "$MATRIX_PYTHON_VERSION" = "3.14" ]; then exit 0; fi\n',
        ),
        mutate_ci(
            "          python -m pip check\n",
            "          echo ${{ matrix.python-version }}\n          python -m pip check\n",
        ),
        mutate_ci(
            "--only-binary=:all: --require-hashes",
            "--no-binary=:none: --require-hashes",
        ),
        mutate_ci("--only-binary=:all: --require-hashes", "--require-hashes"),
        mutate_ci("--only-binary=:all: --require-hashes", "--only-binary=:all:"),
        mutate_ci('"$locked_python" -I -m pip check', '"$locked_python" -m pip check'),
        mutate_ci(
            'locked_venv="$RUNNER_TEMP/locked-dev-venv"',
            'locked_venv="$VIRTUAL_ENV"',
        ),
        mutate_ci(
            "      - name: Lint\n",
            "      - name: Escape\n        run: echo bypass\n      - name: Lint\n",
        ),
        mutate_ci("python-version: ${{ matrix.python-version }}", 'python-version: "3.14"'),
        mutate_ci('ADAM_CORE_PUBLIC_VERSION: "0.8.0"', 'ADAM_CORE_PUBLIC_VERSION: "0.8.1"'),
        mutate_ci("python tests/run_installed_wheel_suite.py", "pdm run coverage"),
        mutate_ci(
            "MPCQ_REPOSITORY_ROOT: ${{ github.workspace }}",
            "MPCQ_REPOSITORY_ROOT: ''",
        ),
        mutate_ci(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n    runs-on: ubuntu-latest\n",
        ),
    ]
    assert len(set(ci_mutations)) == len(ci_mutations)
    for mutation in ci_mutations:
        with pytest.raises(AssertionError):
            _verify_ci(mutation.encode())
    with pytest.raises(AssertionError):
        _verify_publication_gate(publish.replace("MPCQ_RELEASES_ENABLED", "BYPASS", 1))
    _verify_coverage_config(project)


def test_explicit_repository_root_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MPCQ_REPOSITORY_ROOT", str(tmp_path))
    with pytest.raises(AssertionError, match="project root is incomplete|not a Git checkout"):
        _repository_root()
