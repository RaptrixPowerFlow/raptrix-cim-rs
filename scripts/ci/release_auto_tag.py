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


def _remote_tag_sha(tag: str) -> str | None:
    proc = subprocess.run(
        ["git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}^{{}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    line = proc.stdout.strip().splitlines()
    if not line:
        proc = subprocess.run(
            ["git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}"],
            capture_output=True,
            text=True,
            check=False,
        )
        line = proc.stdout.strip().splitlines()
    if not line:
        return None
    return line[0].split()[0]


def _head_sha() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _github_release_exists(tag: str) -> bool:
    repo = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        return False
    import urllib.error
    import urllib.request

    url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def main() -> int:
    event = os.environ.get("GITHUB_EVENT_NAME", "push")

    with open("Cargo.toml", "rb") as f:
        current = _root_package_version(f.read())
    tag = f"v{current}"
    head = _head_sha()

    if _remote_tag_exists(tag):
        skip = True
        tag_sha = _remote_tag_sha(tag)
        if tag_sha == head and not _github_release_exists(tag):
            dispatch_release = True
            reason = (
                f"tag {tag} already exists at HEAD but GitHub Release is missing; "
                "will dispatch release.yml"
            )
        else:
            dispatch_release = False
            if tag_sha == head:
                reason = f"tag {tag} already exists at HEAD with a GitHub Release"
            else:
                reason = f"tag {tag} already exists on origin (not at HEAD)"
    else:
        skip = False
        dispatch_release = False
        if event == "workflow_dispatch":
            reason = "workflow_dispatch: tag missing for current Cargo.toml version"
        else:
            reason = (
                "push to main: tag missing for current root [package] version "
                f"({current}); will tag this commit"
            )

    _out("skip", "true" if skip else "false")
    _out("dispatch_release", "true" if dispatch_release else "false")
    _out("tag", tag)
    _out("version", current)
    _out("reason", reason.replace("\n", " "))

    print(
        f"skip={skip} dispatch_release={dispatch_release} tag={tag} "
        f"version={current} reason={reason}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
