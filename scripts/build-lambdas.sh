#!/usr/bin/env bash
# Empacota as três Lambdas. Tenta instalar dependências para Python 3.11
# (runtime da Lambda); se não houver rede, segue sem deps: o runtime do
# LocalStack já traz boto3 e o worker degrada para transcodificação mock.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="$ROOT/build"
DEPS="$BUILD/.deps"

# Mantém o diretório (ele é montado no container); remove apenas o conteúdo.
find "$BUILD" -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
mkdir -p "$DEPS"

echo "[build] Instalando dependências (boto3, Pillow) para python3.11..."
if python3 -m pip install --quiet --disable-pip-version-check \
    --python-version 3.11 --only-binary=:all: --target "$DEPS" \
    boto3 Pillow 2>"$BUILD/pip-error.log"; then
  echo "[build] Dependências instaladas em $DEPS"
else
  echo "[build] AVISO: pip falhou (sem rede?). Usando boto3 do runtime e modo mock para imagens."
  echo "[build] Detalhes em $BUILD/pip-error.log"
  rm -rf "$DEPS"
  mkdir -p "$DEPS"
fi

for fn in upload-handler transcode-worker status-handler; do
  pkg="$BUILD/$fn"
  mkdir -p "$pkg"
  cp "$ROOT/lambdas/$fn/index.py" "$pkg/index.py"
  cp -R "$DEPS"/. "$pkg"/

  python3 - "$pkg" "$BUILD/$fn.zip" <<'PY'
import os
import sys
import zipfile

source, output = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for root, dirs, files in os.walk(source):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for name in files:
            path = os.path.join(root, name)
            archive.write(path, os.path.relpath(path, source))
print(f"[build] {os.path.basename(output)} pronto")
PY
done

echo "[build] Pacotes disponíveis em $BUILD"
