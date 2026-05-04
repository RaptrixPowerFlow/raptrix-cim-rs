#!/usr/bin/env python3
"""Emit GitHub Actions outputs for automatic release tagging.

Reads the root ``Cargo.toml`` ``[package]`` version (``raptrix-cim-rs`` crate).
Used by ``.github/workflows/auto-release-tag.yml``.

**Policy (reliable on GitHub merge/squash/rebase):** On every ``push`` to ``main``,
if ``refs/tags/v{version}`` is **absent** on ``origin`` for the **current** root
package version, create that tag at the pushed commit. This matches the rule
“bump ``[package]`` version → merge to ``main`` → tag appears → ``release.yml`` runs”.

``workflow_dispatch`` does the same existence check (backfill). ``DRY_RUN=true``
skips the git tag step (handled in the workflow, not here).
"""

from __future__ import annotations

import os
import subprocess
import tomllib


def _out(key: str, value: str) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{key}={value}\n")


def _root_package_version(cargo_toml: bytes) -> str:
    data = tomllib.loads(cargo_toml.decode())
    try:
        return str(data["package"]["version"])
    except KeyError as exc:
        raise SystemExit("root Cargo.toml must have [package].version") from exc


def _remote_tag_exists(tag: str) -> bool:
    proc = subprocess.run(
        ["git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(proc.stdout.strip())


def main() -> int:
    event = os.environ.get("GITHUB_EVENT_NAME", "push")

    with open("Cargo.toml", "rb") as f:
        current = _root_package_version(f.read())
    tag = f"v{current}"

    if _remote_tag_exists(tag):
        skip = True
        reason = f"tag {tag} already exists on origin"
    else:
        skip = False
        if event == "workflow_dispatch":
            reason = "workflow_dispatch: tag missing for current Cargo.toml version"
        else:
            reason = (
                "push to main: tag missing for current root [package] version "
                f"({current}); will tag this commit"
            )

    _out("skip", "true" if skip else "false")
    _out("tag", tag)
    _out("version", current)
    _out("reason", reason.replace("\n", " "))

    print(f"skip={skip} tag={tag} version={current} reason={reason}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
