# Cross-Repo Release Sync Workflow

This repository is the canonical source of truth for the Raptrix `.rpf` schema contract and CIM-to-RPF mapping behavior.

## Scope

- Master contract owner: `raptrix-cim-rs`
- Downstream consumers:
  - `raptrix-psse-rs`
  - `raptrix-core`
  - `raptrix-studio`

## Automatic release tags (CI)

Pushing to ``main`` runs **Auto release tag** (``.github/workflows/auto-release-tag.yml``):

- Reads the root ``Cargo.toml`` ``[package]`` version for ``raptrix-cim-rs``.
- If ``refs/tags/vX.Y.Z`` is **not** on ``origin`` for that version, it creates and pushes an annotated tag at the **current** ``main`` commit (works with merge, squash, and rebase — no ``github.event.before`` comparison).
- After the tag push, **Auto release tag** calls ``workflow_dispatch`` on **Release** (``.github/workflows/release.yml``) for that tag ref. This is required because **pushes performed with the default ``GITHUB_TOKEN`` do not start new workflow runs**, so a tag-only push from Actions would otherwise never build binaries.

**Backfill:** GitHub → **Actions** → **Auto release tag** → **Run workflow**. Set **dry_run** to ``true`` to log only.

Version bumps must remain the single source of truth in root ``Cargo.toml`` (keep ``raptrix-cim-arrow`` dependency version in sync, e.g. via ``./scripts/sync-versions.ps1``).

### PAT / permissions (org repositories)

Fine-grained PATs must be **explicitly allowed for the RaptrixPowerFlow organization** and granted at least:

- **Contents:** read/write (push tags, create releases)
- **Actions:** read/write (optional: dispatch ``release.yml`` manually via API)

If ``git push`` or ``POST .../actions/workflows/.../dispatches`` returns **403 Resource not accessible**, the token is authenticated but **not authorized for this repo** — update the PAT’s repository access or use an org role with push rights.

**Manual release dispatch** (same inputs as the Actions UI), once ``GH_TOKEN`` is set:

```powershell
./scripts/trigger-release-dispatch.ps1 -Version 0.3.4
```

Use ``-Draft`` for a draft GitHub Release; ``-SkipPublish`` to build artifacts only (rare).

## Release Triggers

Run this workflow on any of the following changes:

- `raptrix-cim-arrow/src/schema.rs`
- `docs/schema-contract.md`
- `src/rpf_writer.rs`
- `src/parser.rs`
- Any CLI behavior that affects profile detection or metadata emission

## Versioning Rules

- PATCH: non-structural fixes (bug fixes, docs, metadata text fixes)
- MINOR: additive format changes (new optional fields/tables/metadata keys)
- MAJOR: breaking wire-shape changes (required field or table rename/removal/reorder/type change)
- **Exception — v0.13.1:** Ashburn-class trailing optional CLP columns use a compatibility-extension stamp (`0.13.1`) with dual-read of `0.13.0` instead of jumping to `0.14.0`. Do not repeat without updating `schema-contract.md`.

## Canonical Release Steps

1. Validate this repo on main:
   - `cargo fmt --all -- --check`
   - `cargo check --workspace --all-targets`
   - `cargo test --workspace --all-targets`
2. Tag release:
   - `vX.Y.Z` for crate release
   - optional `schema-vX.Y.Z` for explicit contract milestones
3. Ensure GitHub action `Master Contract CI` publishes the contract artifact.
4. Publish release notes with:
   - schema/contract impact summary
   - **Interoperability posture**: IEC 61970 CIM 17+ baseline across supported profile exchanges
   - **CGMES ingest compatibility**: v3.0+ only (breaking change in v0.8.0)
   - **Public dataset note**: ENTSO-E CAS is the canonical public validation corpus
   - migration notes for downstream repos

## Downstream Sync Checklist

### raptrix-core

1. PR-D first on existing `protection_contingencies.tripped_elements` + study-JSON pairs (works on v0.13.1).
2. After this 0.14.0 crate: honor `reserved=true` and ingest `contingency_sequences` when the feature flag is on.
3. Update embedded or vendored schema references to current contract.
4. Re-run CMake configure/build and import validation for local `.rpf` samples (generate `v014_funnel_demo.rpf` with `cargo test -p raptrix-cim-arrow --test v014_funnel_demo`; do not expect `.rpf` files in the clone).

### raptrix-studio

1. Bump to crate 0.7.0 when writing `tpl_category`, `reserved`, or `contingency_sequences`.
2. Validate `.rpf` loading against current release artifact (dual-read of 0.13.x does not require a writer bump).
3. Confirm optional table handling remains non-breaking.
4. Re-run typecheck/test/build validation.

### raptrix-psse-rs (writer bump)

1. Update dependency to latest `raptrix-cim-arrow` 0.7.0 when emitting the new fields.
2. Re-run parser and output tests.
3. Confirm no local schema fork or duplicate contract files remain.

## CI crash notes (keep this current)

These are the failure modes that have burned time on `main`. Fix the gate here before the next crate bump.

| Gate | Typical crash | What to do first |
| --- | --- | --- |
| **Public Safety** | `*.rpf` is blocked | Never commit interchange binaries. Tests may emit gitignored `.rpf` under `tests/data/fixtures/` from the XML/JSON snippets. |
| **Public Safety** | `*.xml` / `*.rdf` outside `tests/data/fixtures/` | Put CIM exchange snippets only in fixtures. |
| **Master Contract** | `cargo fmt --all -- --check` | Run `cargo fmt --all` before push. Clippy is **not** a Master Contract gate. |
| **Markdown Lint** | blanks-around-lists (`MD032`) | Blank line before/after lists in `CHANGELOG.md` and ADRs. |
| **Version Consistency** | root crate version ≠ `raptrix-cim-arrow` dep / crate | `./scripts/sync-versions.ps1 -Check` |
| **Auto release tag** | 403 / tag exists / `GITHUB_TOKEN` does not chain workflows | PAT needs Contents + Actions; this workflow dispatches **Release** after tagging. |
| **Release** | `scripts/pre-release-check.ps1` (fmt / check / test) on Linux | Same commands as Master Contract; matrix is Windows / Linux / macOS. |

Do **not** hard-code machine-local paths (OneDrive, usernames, `External_Share`) in tests. Optional local datasets belong behind `RAPTRIX_TEST_DATA_ROOT` or `RAPTRIX_EXTERNAL_RPF_DIR`.

## Compatibility Guardrails

- Readers must tolerate unknown trailing root columns and unknown metadata keys.
- Writers in this repo must preserve canonical required root table ordering.
- Any planned breaking contract change must include:
  - MAJOR version bump
  - migration guide section in release notes
  - downstream update tasks in all three consumer repos
