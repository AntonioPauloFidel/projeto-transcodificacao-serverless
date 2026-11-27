#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "$ROOT/.localstack/env" ]]; then
  echo "[regressao] Execute scripts/init-localstack.sh antes deste teste." >&2
  exit 1
fi

exec python3 "$ROOT/tests/test_regressao.py" "$@"
