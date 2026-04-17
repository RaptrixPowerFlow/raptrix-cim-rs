#!/usr/bin/env bash
set -euo pipefail

MODE="tracked"
if [[ "${1:-}" == "--mode" && -n "${2:-}" ]]; then
  MODE="$2"
fi

if [[ "$MODE" != "tracked" && "$MODE" != "staged" ]]; then
  echo "Usage: $0 [--mode tracked|staged]" >&2
  exit 2
fi

bad=0
max_bytes=$((10 * 1024 * 1024))

pattern='BEGIN RSA PRIVATE KEY|BEGIN PRIVATE KEY|BEGIN DSA PRIVATE KEY|AWS_SECRET_ACCESS_KEY|AWS_ACCESS_KEY_ID|AKIA[0-9A-Z]{16}|password=|api_key|API_KEY|token=|-----BEGIN OPENSSH PRIVATE KEY-----'

# Build file list based on mode
if [[ "$MODE" == "staged" ]]; then
  mapfile -d '' files < <(git diff --cached --name-only --diff-filter=ACMR -z)
else
  mapfile -d '' files < <(git ls-files -z)
fi

for file in "${files[@]}"; do
  case "$file" in
    INTERNAL-MARKETING-GUIDE.md|*.raw|*.RAW|*.dyr|*.DYR|*.sav|*.SAV|*.epc|*.EPC|*.pss|*.PSS|*.pdf)
      echo "[public-safety] blocked filename: $file"
      bad=1
      ;;
  esac

  case "$file" in
    tests/data/external/*|tests/data/large/*|data/*)
      echo "[public-safety] blocked sensitive data path: $file"
      bad=1
      ;;
  esac

  case "$file" in
    *.xml|*.XML|*.rdf|*.RDF|*.cim|*.CIM)
      case "$file" in
        tests/data/fixtures/*)
          ;;
        *)
          echo "[public-safety] blocked CIM exchange file outside fixtures: $file"
          bad=1
          ;;
      esac
      ;;
  esac

  if [[ "$MODE" == "staged" ]]; then
    size=$(git cat-file -s ":$file" 2>/dev/null || echo 0)
  else
    if [[ -f "$file" ]]; then
      size=$(wc -c < "$file")
    else
      size=0
    fi
  fi

  if [[ "$size" -gt "$max_bytes" ]]; then
    echo "[public-safety] large tracked file (>10MB): $file ($size bytes)"
    bad=1
  fi

  if [[ "$MODE" == "staged" ]]; then
    if git show ":$file" 2>/dev/null | grep -I -n -E --quiet "$pattern"; then
      echo "[public-safety] potential secret in staged file: $file"
      git show ":$file" 2>/dev/null | grep -I -n -E "$pattern" | sed -n '1,5p'
      bad=1
    fi
  else
    if [[ -f "$file" ]] && grep -I -n -E --quiet "$pattern" "$file"; then
      echo "[public-safety] potential secret in tracked file: $file"
      grep -I -n -E "$pattern" "$file" | sed -n '1,5p'
      bad=1
    fi
  fi
done

if [[ "$bad" -ne 0 ]]; then
  echo
  echo "Public safety checks failed. Remove sensitive files/content and retry."
  exit 1
fi

echo "Public safety checks passed ($MODE mode)."
