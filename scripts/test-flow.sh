#!/usr/bin/env bash
# Teste ponta a ponta: foto, vídeo, trigger S3, SQS/SNS, DynamoDB e S3 final.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="$ROOT/.localstack"
BUILD="$ROOT/build"
COMPOSE_FILE="$ROOT/docker-compose.yml"

if [[ ! -f "$STATE/env" ]]; then
  echo "[test] Execute scripts/init-localstack.sh antes deste teste." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$STATE/env"
API_KEY="$(tr -d '\r\n' < "$STATE/api-key.txt")"
API_BASE="${API_ENDPOINT:-${PUBLIC_S3_ENDPOINT}}/restapis/${REST_API_ID}/dev/_user_request_"
DIRECT_ID="s3-direct-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:8])')"

alc() {
  docker compose -f "$COMPOSE_FILE" exec -T \
    -e AWS_DEFAULT_REGION="$AWS_REGION" localstack awslocal "$@"
}

field() {
  python3 -c 'import json,sys; print(json.load(sys.stdin).get(sys.argv[1], ""))' "$1"
}

echo "[test] API: $API_BASE"

echo "[test] Limpando filas de execuções anteriores..."
alc sqs purge-queue --queue-url "$QUEUE_URL" >/dev/null
alc sqs purge-queue --queue-url "$DLQ_URL" >/dev/null

echo "[test] 1/8 Verificando GET /health (sem API key)..."
HEALTH_CODE="$(curl -sS -o /dev/null -w '%{http_code}' "$API_BASE/health")"
[[ "$HEALTH_CODE" == "200" ]] || { echo "[test] health falhou: $HEALTH_CODE" >&2; exit 1; }

echo "[test] 2/8 Validando autenticação e payload..."
NO_KEY_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$API_BASE/media" \
  -H 'Content-Type: application/json' -d '{}')"
[[ "$NO_KEY_CODE" == "401" || "$NO_KEY_CODE" == "403" ]] || {
  echo "[test] API key não foi exigida (HTTP $NO_KEY_CODE)" >&2; exit 1; }
BAD_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$API_BASE/media" \
  -H 'Content-Type: application/json' -H "x-api-key: $API_KEY" \
  -d '{"sender":"teste","user_id":"user-001","file_name":"x.txt"}')"
[[ "$BAD_CODE" == "400" ]] || { echo "[test] payload inválido retornou $BAD_CODE" >&2; exit 1; }

echo "[test] 3/8 Criando fixtures (PNG válido e vídeo mock)..."
python3 - "$BUILD" "$DIRECT_ID" <<'PY'
import base64
import json
import struct
import sys
import zlib
from pathlib import Path

build, direct_id = Path(sys.argv[1]), sys.argv[2]

def chunk(kind: bytes, data: bytes) -> bytes:
    payload = kind + data
    return struct.pack(">I", len(data)) + payload + struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF)

width = height = 8
raw = b"".join(b"\x00" + bytes([255, 120, 40] * width) for _ in range(height))
png = (
    b"\x89PNG\r\n\x1a\n"
    + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    + chunk(b"IDAT", zlib.compress(raw))
    + chunk(b"IEND", b"")
)
foto = build / "media-test-foto.png"
foto.write_bytes(png)

video = build / "media-test-video.mp4"
video.write_bytes(b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso2mp41" + b"\x00" * 512)

for name, path, ctype in [("foto", foto, "image/png"), ("video", video, "video/mp4")]:
    payload = {
        "file_name": path.name,
        "sender": "test-flow",
        "user_id": "user-001",
        "content_type": ctype,
        "content_base64": base64.b64encode(path.read_bytes()).decode("ascii"),
    }
    (build / f"payload-{name}.json").write_text(json.dumps(payload))

metadata = {
    "media-id": direct_id,
    "sender": "s3-event",
    "user-id": "user-002",
    "timestamp": "2026-01-01T00:00:00+00:00",
    "original-name": "direct.png",
}
(build / "s3-direct-metadata.json").write_text(json.dumps(metadata))
print("[test] Fixtures prontas")
PY

echo "[test] 4/8 Inscrevendo fila de teste no tópico SNS..."
STATUS_QUEUE_NAME="media-status-test-queue"
alc sqs create-queue --queue-name "$STATUS_QUEUE_NAME" >/dev/null
STATUS_QUEUE_URL="$(alc sqs get-queue-url --queue-name "$STATUS_QUEUE_NAME" \
  --query QueueUrl --output text | tr -d '\r')"
alc sqs purge-queue --queue-url "$STATUS_QUEUE_URL" >/dev/null
STATUS_QUEUE_ARN="$(alc sqs get-queue-attributes --queue-url "$STATUS_QUEUE_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text | tr -d '\r')"

python3 - "$BUILD/sns-queue-policy.json" "$STATUS_QUEUE_ARN" "$TOPIC_ARN" <<'PY'
import json
import sys

output, queue_arn, topic_arn = sys.argv[1], sys.argv[2], sys.argv[3]
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSNS",
            "Effect": "Allow",
            "Principal": {"Service": "sns.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
        }
    ],
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump({"Policy": json.dumps(policy)}, handle)
PY
alc sqs set-queue-attributes --queue-url "$STATUS_QUEUE_URL" \
  --attributes "file:///opt/lambda-packages/sns-queue-policy.json" >/dev/null
alc sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs \
  --notification-endpoint "$STATUS_QUEUE_ARN" >/dev/null

post_media() {
  curl -sS -X POST "$API_BASE/media" \
    -H 'Content-Type: application/json' \
    -H "x-api-key: $API_KEY" \
    --data-binary "@$1"
}

wait_status() {
  local media_id="$1" elapsed=0 status body
  while [[ "$elapsed" -lt 120 ]]; do
    body="$(curl -sS "$API_BASE/media/$media_id" -H "x-api-key: $API_KEY")"
    status="$(printf '%s' "$body" | field status)"
    echo "  $media_id -> ${status:-SEM_STATUS}"
    if [[ "$status" == "COMPLETED" ]]; then
      LAST_BODY="$body"
      return 0
    fi
    if [[ "$status" == "FAILED" ]]; then
      echo "$body" >&2
      return 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "[test] Timeout aguardando $media_id" >&2
  return 1
}

echo "[test] 5/8 Enviando foto e vídeo via API Gateway..."
FOTO_BODY="$(post_media "$BUILD/payload-foto.json")"
echo "  upload foto: $FOTO_BODY"
FOTO_ID="$(printf '%s' "$FOTO_BODY" | field media_id)"
VIDEO_BODY="$(post_media "$BUILD/payload-video.json")"
echo "  upload vídeo: $VIDEO_BODY"
VIDEO_ID="$(printf '%s' "$VIDEO_BODY" | field media_id)"
[[ -n "$FOTO_ID" && -n "$VIDEO_ID" ]] || { echo "[test] upload falhou" >&2; exit 1; }

echo "[test] 6/8 Acompanhando processamento..."
wait_status "$FOTO_ID"
FOTO_FINAL="$LAST_BODY"
wait_status "$VIDEO_ID"
VIDEO_FINAL="$LAST_BODY"

echo "[test] 7/8 Validando trigger direto do S3 e objetos processados..."
alc s3api put-object --bucket "$RAW_BUCKET" --key "raw/direct/s3-trigger-sample.png" \
  --body /opt/lambda-packages/media-test-foto.png --content-type image/png \
  --metadata "file:///opt/lambda-packages/s3-direct-metadata.json" >/dev/null
wait_status "$DIRECT_ID"
DIRECT_FINAL="$LAST_BODY"

for final_body in "$FOTO_FINAL" "$VIDEO_FINAL" "$DIRECT_FINAL"; do
  OUTPUT_KEY="$(printf '%s' "$final_body" | field output_key)"
  [[ -n "$OUTPUT_KEY" ]] || { echo "[test] output_key ausente: $final_body" >&2; exit 1; }
  SIZE="$(alc s3api head-object --bucket "$PROCESSED_BUCKET" --key "$OUTPUT_KEY" \
    --query ContentLength --output text | tr -d '\r')"
  echo "  $OUTPUT_KEY -> ${SIZE} bytes"
done

echo "  Foto   : $(printf '%s' "$FOTO_FINAL" | field s3_url)"
echo "  Vídeo  : $(printf '%s' "$VIDEO_FINAL" | field s3_url)"
echo "  Direct : $(printf '%s' "$DIRECT_FINAL" | field s3_url)"

echo "[test] 8/8 Verificando notificações SNS e DLQ..."
sleep 3
alc sqs receive-message --queue-url "$STATUS_QUEUE_URL" \
  --max-number-of-messages 10 --wait-time-seconds 3 \
  --query 'Messages[*].Body' --output json > "$STATE/sns-messages.json"

python3 - "$STATE/sns-messages.json" "$FOTO_ID" "$VIDEO_ID" "$DIRECT_ID" <<'PY'
import json
import sys

path, foto_id, video_id, direct_id = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
with open(path, encoding="utf-8") as handle:
    envelopes = json.load(handle)

events = []
for envelope in envelopes:
    message = json.loads(envelope) if isinstance(envelope, str) else envelope
    if "Message" in message:
        message = json.loads(message["Message"])
    events.append((message.get("media_id"), message.get("status")))

print("  Eventos SNS recebidos:", events)
required = {
    (foto_id, "PROCESSING"), (foto_id, "COMPLETED"),
    (video_id, "PROCESSING"), (video_id, "COMPLETED"),
    (direct_id, "PROCESSING"), (direct_id, "COMPLETED"),
}
missing = required - set(events)
if missing:
    print(f"[test] Notificações ausentes: {missing}", file=sys.stderr)
    sys.exit(1)
PY

DLQ_COUNT="$(alc sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names ApproximateNumberOfMessages \
  --query 'Attributes.ApproximateNumberOfMessages' --output text | tr -d '\r')"
[[ "$DLQ_COUNT" == "0" ]] || { echo "[test] DLQ contém $DLQ_COUNT mensagem(ns)" >&2; exit 1; }

NOT_FOUND_CODE="$(curl -sS -o /dev/null -w '%{http_code}' \
  "$API_BASE/media/inexistente" -H "x-api-key: $API_KEY")"
[[ "$NOT_FOUND_CODE" == "404" ]] || { echo "[test] esperado 404, veio $NOT_FOUND_CODE" >&2; exit 1; }

cat <<EOF

[test] SUCESSO ✅
  - API key, validação e 404: OK
  - Foto, vídeo e upload direto no S3: COMPLETED
  - Notificações SNS PROCESSING/COMPLETED: OK
  - Objetos finais no bucket $PROCESSED_BUCKET: OK
  - DLQ vazia: OK
EOF
