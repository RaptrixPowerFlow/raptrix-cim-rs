# Local Git hooks for Raptrix repositories

This repository includes a recommended local hooks directory at `.githooks/`.

Enable the hooks for your local clone with:

```bash
git config core.hooksPath .githooks
```

What this does:

- `pre-commit` runs `scripts/public-safety-check.sh --mode staged` to block sensitive/proprietary files, large payloads, and common secret patterns.
- `pre-push` blocks push unless `RAPTRIX_ALLOW_PUSH=1` is explicitly set in the shell (review/approval gate).
- `pre-push` also runs the RPF regression matrix (`cargo run --bin regression_matrix -- --profiles both --include-ssh-dy`) when test data is configured through `RAPTRIX_TEST_DATA_ROOT` or `.raptrix-test-data-root`.

Notes:

- Hooks set via `core.hooksPath` are local to your clone and are not pushed to remotes.
- To bypass for one commit (advanced): `git commit --no-verify`.
- To allow push after review in your current shell: `RAPTRIX_ALLOW_PUSH=1 git push ...`.
- To bypass the local regression gate in exceptional cases: `RAPTRIX_SKIP_REGRESSION=1 RAPTRIX_ALLOW_PUSH=1 git push ...`.
- CI runs the same safety script in tracked-file mode for pull requests and pushes.
