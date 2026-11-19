from __future__ import annotations

import importlib.util
import json
import os
import tomllib
from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml

RELEASE_VERSION = "0.5.0"
RELEASE_SHA = "a" * 40


def _root() -> Path:
    configured = os.environ.get("MPCQ_REPOSITORY_ROOT")
    if configured is not None:
        assert configured, "explicit repository root must not be empty"
        root = Path(configured).resolve()
        assert (root / ".git").exists(), "explicit repository root is not a Git checkout"
    else:
        root = Path(__file__).resolve().parents[1]
        if not (root / ".git").exists():
            pytest.skip("repository-only release governance is not shipped in the sdist")
    assert (root / "pyproject.toml").is_file()
    return root


def _release_module():
    path = _root() / "scripts/release_artifacts.py"
    spec = importlib.util.spec_from_file_location("mpcq_release_artifacts", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _workflow(name: str) -> tuple[str, Mapping[str, object]]:
    text = (_root() / ".github/workflows" / name).read_text(encoding="utf-8")
    document = yaml.safe_load(text)
    assert isinstance(document, Mapping)
    return text, document


def _manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "project": "mpcq",
        "version": RELEASE_VERSION,
        "release_sha": RELEASE_SHA,
        "artifacts": [
            {
                "filename": "mpcq-0.5.0-py3-none-any.whl",
                "sha256": "1" * 64,
                "size": 101,
            },
            {
                "filename": "mpcq-0.5.0.tar.gz",
                "sha256": "2" * 64,
                "size": 202,
            },
        ],
    }


def _remote(*names: str, digest_override: str | None = None) -> dict[str, object]:
    candidates = {item["filename"]: item for item in _manifest()["artifacts"]}
    return {
        "info": {"name": "mpcq", "version": RELEASE_VERSION},
        "urls": [
            {
                "filename": name,
                "digests": {
                    "sha256": digest_override or candidates[name]["sha256"],
                },
                "yanked": False,
            }
            for name in names
        ],
    }


def test_dynamic_version_and_release_build_contract_are_exact() -> None:
    project = tomllib.loads((_root() / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["dynamic"] == ["version"]
    assert "version" not in project["project"]
    assert project["tool"]["pdm"]["version"] == {
        "source": "scm",
        "write_to": "mpcq/_version.py",
        "write_template": "__version__ = '{}'",
    }
    assert project["build-system"]["requires"] == ["pdm-backend==2.5.0"]


def test_candidate_workflow_builds_and_tests_one_exact_publication_set() -> None:
    text, workflow = _workflow("release-candidate.yaml")
    assert workflow["name"] == "Release candidate artifacts"
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["env"] == {"MPCQ_RELEASE_VERSION": RELEASE_VERSION}
    assert set(workflow["jobs"]) == {"candidate"}
    job = workflow["jobs"]["candidate"]
    assert job["if"] == (
        "${{ vars.MPCQ_RELEASES_ENABLED == 'true' && github.ref == 'refs/heads/main' }}"
    )
    assert job["runs-on"] == "ubuntu-latest"
    assert 'PDM_BUILD_SCM_VERSION="$MPCQ_RELEASE_VERSION"' in text
    assert 'git archive --format=tar "$GITHUB_SHA"' in text
    assert "python -m build --sdist --wheel" in text
    assert "release_artifacts.py create" in text
    assert "release_artifacts.py verify" in text
    assert "tests/run_installed_wheel_suite.py" in text
    assert "pypa/gh-action-pypi-publish" not in text
    assert "actions/upload-artifact@v4" in text


def test_publish_workflow_promotes_accepted_bytes_under_protection() -> None:
    text, workflow = _workflow("publish.yaml")
    assert workflow["name"] == "Publish accepted MPCQ artifacts"
    assert workflow["permissions"] == {
        "contents": "read",
        "actions": "read",
        "id-token": "write",
    }
    assert workflow["env"] == {"MPCQ_RELEASE_VERSION": RELEASE_VERSION}
    assert "push:" not in text
    assert set(workflow["jobs"]) == {"publish-pypi"}
    job = workflow["jobs"]["publish-pypi"]
    assert job["if"] == "${{ vars.MPCQ_RELEASES_ENABLED == 'true' }}"
    assert job["environment"] == {"name": "pypi", "url": "https://pypi.org/p/mpcq"}
    assert '/environments/pypi")' in text
    assert "/environments/pypi/deployment-branch-policies?per_page=100" in text
    assert 'select(.type == "required_reviewers")' in text
    assert "prevent_self_review" not in text
    assert 'test "$(jq -r .total_count <<<"$policies_json")" = 1' in text
    assert '"v$MPCQ_RELEASE_VERSION"' in text
    assert 'git rev-parse "v$EXPECTED_VERSION^{commit}"' in text
    assert "candidate_run_id" in text
    assert "Release candidate artifacts" in text
    assert "actions/download-artifact@v4" in text
    assert "release_artifacts.py prepare-pypi" in text
    assert "steps.publication-state.outputs.missing_count != '0'" in text
    assert "pypa/gh-action-pypi-publish@release/v1" in text
    assert "release_artifacts.py verify-pypi" in text
    assert "python -m build" not in text
    assert "pdm build" not in text


def test_publication_planner_is_absent_partial_complete_and_idempotent() -> None:
    release = _release_module()
    manifest = _manifest()
    wheel = "mpcq-0.5.0-py3-none-any.whl"
    sdist = "mpcq-0.5.0.tar.gz"

    absent = release.plan_publication(manifest, None)
    assert absent["state"] == "absent"
    assert absent["missing"] == [wheel, sdist]

    partial = release.plan_publication(manifest, _remote(wheel))
    assert partial["state"] == "partial"
    assert partial["missing"] == [sdist]

    complete = release.plan_publication(manifest, _remote(wheel, sdist))
    assert complete["state"] == "complete"
    assert complete["missing_count"] == 0
    assert release.plan_publication(manifest, _remote(wheel, sdist)) == complete


def test_publication_planner_fails_on_conflicts_or_unexpected_files() -> None:
    release = _release_module()
    manifest = _manifest()
    wheel = "mpcq-0.5.0-py3-none-any.whl"

    with pytest.raises(release.ReleaseError, match="digest mismatch"):
        release.plan_publication(manifest, _remote(wheel, digest_override="f" * 64))

    unexpected = {
        "info": {"name": "mpcq", "version": RELEASE_VERSION},
        "urls": [
            {
                "filename": "mpcq-0.5.0.zip",
                "digests": {"sha256": "3" * 64},
                "yanked": False,
            }
        ],
    }
    with pytest.raises(release.ReleaseError, match="unexpected release files"):
        release.plan_publication(manifest, unexpected)

    yanked = _remote(wheel)
    yanked["urls"][0]["yanked"] = True
    with pytest.raises(release.ReleaseError, match="yanked"):
        release.plan_publication(manifest, yanked)


def test_manifest_format_is_stable_and_contains_exact_source_identity(tmp_path: Path) -> None:
    release = _release_module()
    manifest = _manifest()
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    assert json.loads(path.read_text(encoding="utf-8"))["release_sha"] == RELEASE_SHA
    assert release.expected_checksums(manifest) == (
        "1" * 64 + "  dist/mpcq-0.5.0-py3-none-any.whl\n" + "2" * 64 + "  dist/mpcq-0.5.0.tar.gz\n"
    )
