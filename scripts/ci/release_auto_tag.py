#!/usr/bin/env python3
"""Emit GitHub Actions outputs for automatic release tagging.

Reads the root ``Cargo.toml`` ``[package]`` version (``raptrix-cim-rs`` crate).
Used by ``.github/workflows/auto-release-tag.yml``.

- On ``push`` to ``main``: create ``v{version}`` only if that version changed
  versus ``github.event.before`` and the tag does not exist on ``origin``.
- On ``workflow_dispatch``: create ``v{version}`` if the tag is missing (backfill).
"""

from __future__ import annotations

import os
import subprocess
import sys
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


def _git_show(ref: str) -> bytes | None:
    try:
        return subprocess.check_output(["git", "show", ref], stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return None


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
    dry = os.environ.get("DRY_RUN", "false").lower() == "true"

    with open("Cargo.toml", "rb") as f:
        current = _root_package_version(f.read())
    tag = f"v{current}"

    skip = True
    reason = "unknown"

    if event == "workflow_dispatch":
        if _remote_tag_exists(tag):
            reason = f"tag {tag} already exists on origin"
        else:
            skip = False
            reason = "workflow_dispatch: tag missing for current Cargo.toml version"
    elif event == "push":
        before = os.environ.get("BEFORE_SHA", "")
        if not before or before == "0" * 40:
            reason = "push without usable github.event.before (initial push or force-push)"
        else:
            old_blob = _git_show(f"{before}:Cargo.toml")
            if old_blob is None:
                reason = f"Cargo.toml not found at {before}"
            else:
                old_ver = _root_package_version(old_blob)
                if old_ver == current:
                    reason = f"root [package] version unchanged ({current})"
                elif _remote_tag_exists(tag):
                    reason = f"tag {tag} already exists on origin"
                else:
                    skip = False
                    reason = f"version bump {old_ver} -> {current}"
    else:
        reason = f"unsupported event {event!r}"

    _out("skip", "true" if skip else "false")
    _out("tag", tag)
    _out("version", current)
    _out("reason", reason.replace("\n", " "))

    print(f"skip={skip} tag={tag} version={current} reason={reason}", flush=True)
    if dry:
        print("(dry_run: no git tag created)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
