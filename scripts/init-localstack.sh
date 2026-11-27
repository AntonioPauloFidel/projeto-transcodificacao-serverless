#!/usr/bin/env bash
# Cria toda a infraestrutura local (idempotente) usando awslocal dentro do LocalStack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
REGION="${AWS_REGION:-us-east-1}"
PUBLIC_S3_ENDPOINT="${PUBLIC_S3_ENDPOINT:-http://localhost:4566}"
BUILD="$ROOT/build"
STATE="$ROOT/.localstack"

RAW_BUCKET="raw-media-bucket"
PROCESSED_BUCKET="processed-media-bucket"
TABLE="MediaMetadata"
QUEUE_NAME="media-transcode-queue"
DLQ_NAME="media-transcode-dlq"
TOPIC_NAME="media-status-notifications"
API_NAME="media-transcode-api"

for zip in upload-handler.zip transcode-worker.zip status-handler.zip; do
  if [[ ! -f "$BUILD/$zip" ]]; then
    echo "[init] Falta $zip. Execute: scripts/build-lambdas.sh" >&2
    exit 1
  fi
done

if ! docker compose version >/dev/null 2>&1; then
  echo "[init] docker compose não encontrado." >&2
  exit 1
fi

alc() {
  "${COMPOSE[@]}" exec -T -e AWS_DEFAULT_REGION="$REGION" localstack awslocal "$@"
}

echo "[init] Subindo o LocalStack..."
"${COMPOSE[@]}" up -d

echo "[init] Aguardando o endpoint 4566..."
ready=0
for _ in $(seq 1 90); do
  if alc s3 ls >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" -ne 1 ]]; then
  echo "[init] LocalStack não ficou pronto; veja: docker compose -f '$COMPOSE_FILE' logs localstack" >&2
  exit 1
fi

mkdir -p "$STATE"

# ---------------------------------------------------------------- S3
for bucket in "$RAW_BUCKET" "$PROCESSED_BUCKET"; do
  if alc s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    echo "[init] Bucket $bucket já existe"
  else
    alc s3api create-bucket --bucket "$bucket" >/dev/null
    echo "[init] Bucket $bucket criado"
  fi
done

# ----------------------------------------------------------- DynamoDB
if ! alc dynamodb describe-table --table-name "$TABLE" >/dev/null 2>&1; then
  alc dynamodb create-table \
    --table-name "$TABLE" \
    --attribute-definitions AttributeName=media_id,AttributeType=S \
    --key-schema AttributeName=media_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST >/dev/null
fi
alc dynamodb wait table-exists --table-name "$TABLE"
echo "[init] Tabela $TABLE pronta"

# ---------------------------------------------------------- SQS / SNS
alc sqs create-queue --queue-name "$DLQ_NAME" >/dev/null
DLQ_URL="$(alc sqs get-queue-url --queue-name "$DLQ_NAME" --query QueueUrl --output text | tr -d '\r')"
DLQ_ARN="$(alc sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text | tr -d '\r')"

python3 - "$BUILD/sqs-queue-attributes.json" "$DLQ_ARN" <<'PY'
import json
import sys

output, dlq_arn = sys.argv[1], sys.argv[2]
attributes = {
    "VisibilityTimeout": "120",
    "RedrivePolicy": json.dumps(
        {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
    ),
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump(attributes, handle)
PY

alc sqs create-queue --queue-name "$QUEUE_NAME" \
  --attributes "file:///opt/lambda-packages/sqs-queue-attributes.json" >/dev/null
QUEUE_URL="$(alc sqs get-queue-url --queue-name "$QUEUE_NAME" --query QueueUrl --output text | tr -d '\r')"
alc sqs set-queue-attributes --queue-url "$QUEUE_URL" \
  --attributes "file:///opt/lambda-packages/sqs-queue-attributes.json" >/dev/null
QUEUE_ARN="$(alc sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text | tr -d '\r')"

python3 - "$BUILD/sqs-s3-policy.json" "$QUEUE_ARN" "$RAW_BUCKET" <<'PY'
import json
import sys

output, queue_arn, bucket = sys.argv[1], sys.argv[2], sys.argv[3]
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowS3ObjectCreatedEvents",
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": f"arn:aws:s3:::{bucket}"}},
        }
    ],
}
with open(output, "w", encoding="utf-8") as handle:
    json.dump({"Policy": json.dumps(policy)}, handle)
PY
alc sqs set-queue-attributes --queue-url "$QUEUE_URL" \
  --attributes "file:///opt/lambda-packages/sqs-s3-policy.json" >/dev/null

python3 - "$BUILD/s3-notification.json" "$QUEUE_ARN" <<'PY'
import json
import sys

output, queue_arn = sys.argv[1], sys.argv[2]
config = {"QueueConfigurations": [{"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}]}
with open(output, "w", encoding="utf-8") as handle:
    json.dump(config, handle)
PY
alc s3api put-bucket-notification-configuration --bucket "$RAW_BUCKET" \
  --notification-configuration "file:///opt/lambda-packages/s3-notification.json"
echo "[init] SQS ($QUEUE_NAME + DLQ) e trigger S3 configurados"

TOPIC_ARN="$(alc sns create-topic --name "$TOPIC_NAME" --query TopicArn --output text | tr -d '\r')"
echo "[init] Tópico SNS: $TOPIC_ARN"

# ---------------------------------------------------------------- IAM
if ! alc iam get-role --role-name media-lambda-role >/dev/null 2>&1; then
  alc iam create-role --role-name media-lambda-role \
    --assume-role-policy-document "file:///opt/config/lambda-trust-policy.json" >/dev/null
fi
alc iam put-role-policy --role-name media-lambda-role \
  --policy-name media-lambda-policy \
  --policy-document "file:///opt/config/lambda-policy.json" >/dev/null
ROLE_ARN="$(alc iam get-role --role-name media-lambda-role --query 'Role.Arn' --output text | tr -d '\r')"

# ------------------------------------------------------------- Lambdas
BASE_VARS="AWS_DEFAULT_REGION=$REGION,RAW_BUCKET=$RAW_BUCKET,PROCESSED_BUCKET=$PROCESSED_BUCKET,MEDIA_TABLE=$TABLE,TOPIC_ARN=$TOPIC_ARN,PUBLIC_S3_ENDPOINT=$PUBLIC_S3_ENDPOINT"

deploy_function() {
  local name="$1" environment="$2"
  # Recriar é mais confiável no LocalStack do que atualizar $LATEST.
  if alc lambda get-function --function-name "$name" >/dev/null 2>&1; then
    alc lambda delete-function --function-name "$name" >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
      alc lambda get-function --function-name "$name" >/dev/null 2>&1 || break
      sleep 1
    done
  fi
  alc lambda create-function \
    --function-name "$name" \
    --runtime python3.11 \
    --role "$ROLE_ARN" \
    --handler index.handler \
    --zip-file "fileb:///opt/lambda-packages/$name.zip" \
    --timeout 120 \
    --memory-size 512 \
    --environment "$environment" >/dev/null
  echo "[init] Lambda $name pronta"
}

deploy_function upload-handler "Variables={$BASE_VARS,QUEUE_URL=$QUEUE_URL,MAX_UPLOAD_BYTES=10485760}"
deploy_function transcode-worker "Variables={$BASE_VARS,MAX_IMAGE_SIZE=1280}"
deploy_function status-handler "Variables={$BASE_VARS}"

MAPPING="$(alc lambda list-event-source-mappings --function-name transcode-worker \
  --query "EventSourceMappings[?EventSourceArn=='$QUEUE_ARN'].UUID" --output text \
  | tr -d '\r' | grep -v '^None$' || true)"
if [[ -z "$MAPPING" ]]; then
  alc lambda create-event-source-mapping \
    --function-name transcode-worker \
    --event-source-arn "$QUEUE_ARN" \
    --batch-size 10 \
    --enabled >/dev/null
  echo "[init] Event source mapping SQS -> transcode-worker criado"
else
  echo "[init] Event source mapping já existe ($MAPPING)"
fi

# ------------------------------------------------------- API Gateway
API_ID="$(alc apigateway get-rest-apis --query "items[?name=='$API_NAME'].id" \
  --output text | head -n1 | tr -d '\r')"
if [[ -z "$API_ID" ]]; then
  API_ID="$(alc apigateway create-rest-api --name "$API_NAME" \
    --description 'Serviço de transcodificação de mídia (LocalStack)' \
    --query id --output text | tr -d '\r')"
fi
ROOT_ID="$(alc apigateway get-resources --rest-api-id "$API_ID" \
  --query "items[?path=='/'].id" --output text | head -n1 | tr -d '\r')"

ensure_resource() {
  local parent="$1" part="$2" path="$3" id
  id="$(alc apigateway get-resources --rest-api-id "$API_ID" \
    --query "items[?path=='$path'].id" --output text | head -n1 | tr -d '\r')"
  if [[ -z "$id" ]]; then
    id="$(alc apigateway create-resource --rest-api-id "$API_ID" \
      --parent-id "$parent" --path-part "$part" --query id --output text | tr -d '\r')"
  fi
  printf '%s' "$id"
}

MEDIA_RES="$(ensure_resource "$ROOT_ID" media "/media")"
MEDIA_ID_RES="$(ensure_resource "$MEDIA_RES" '{id}' "/media/{id}")"
HEALTH_RES="$(ensure_resource "$ROOT_ID" health "/health")"

put_lambda_method() {
  local resource="$1" method="$2" fn="$3" route="$4"
  local fn_arn uri sid

  alc apigateway delete-method --rest-api-id "$API_ID" \
    --resource-id "$resource" --http-method "$method" >/dev/null 2>&1 || true
  alc apigateway put-method --rest-api-id "$API_ID" \
    --resource-id "$resource" --http-method "$method" \
    --authorization-type NONE --api-key-required >/dev/null

  fn_arn="$(alc lambda get-function --function-name "$fn" \
    --query 'Configuration.FunctionArn' --output text | tr -d '\r')"
  uri="arn:aws:apigateway:$REGION:lambda:path/2015-03-31/functions/$fn_arn/invocations"
  alc apigateway put-integration --rest-api-id "$API_ID" \
    --resource-id "$resource" --http-method "$method" \
    --type AWS_PROXY --integration-http-method POST --uri "$uri" >/dev/null

  sid="apigw-$(echo "$fn" | tr -d '{}')-$method-$(echo "$resource" | tr -d '{}')"
  alc lambda remove-permission --function-name "$fn" \
    --statement-id "$sid" >/dev/null 2>&1 || true
  alc lambda add-permission --function-name "$fn" \
    --statement-id "$sid" --action lambda:InvokeFunction \
    --principal apigateway.amazonaws.com \
    --source-arn "arn:aws:execute-api:$REGION:000000000000:$API_ID/*/$method$route" >/dev/null
}

put_lambda_method "$MEDIA_RES" POST upload-handler "/media"
put_lambda_method "$MEDIA_ID_RES" GET status-handler "/media/*"

# GET /health sem API key (usado pelo teste para checar prontidão).
alc apigateway delete-method --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_RES" --http-method GET >/dev/null 2>&1 || true
alc apigateway put-method --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_RES" --http-method GET --authorization-type NONE >/dev/null
alc apigateway put-integration --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_RES" --http-method GET --type MOCK \
  --request-templates '{"application/json":"{\"statusCode\":200}"}' >/dev/null
alc apigateway put-method-response --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_RES" --http-method GET --status-code 200 >/dev/null
alc apigateway put-integration-response --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_RES" --http-method GET --status-code 200 \
  --response-template '{"application/json":"{\"ok\":true,\"service\":\"media-transcode\"}"}' >/dev/null

alc apigateway create-deployment --rest-api-id "$API_ID" --stage-name dev >/dev/null

KEY_ID="$(alc apigateway get-api-keys --query "items[?name=='media-local-key'].id" \
  --output text | head -n1 | tr -d '\r')"
if [[ -z "$KEY_ID" ]]; then
  KEY_ID="$(alc apigateway create-api-key --name media-local-key \
    --description 'Chave para testes locais' --enabled --query id --output text | tr -d '\r')"
fi
API_KEY="$(alc apigateway get-api-key --api-key "$KEY_ID" --include-value \
  --query value --output text | tr -d '\r')"
printf '%s\n' "$API_KEY" > "$STATE/api-key.txt"

USAGE_ID="$(alc apigateway get-usage-plans --query "items[?name=='media-local-plan'].id" \
  --output text | head -n1 | tr -d '\r')"
if [[ -z "$USAGE_ID" ]]; then
  USAGE_ID="$(alc apigateway create-usage-plan --name media-local-plan \
    --description 'Plano local com API key' \
    --api-stages "apiId=$API_ID,stage=dev" --query id --output text | tr -d '\r')"
fi
if ! alc apigateway get-usage-plan-keys --usage-plan-id "$USAGE_ID" \
  --query "items[?keyId=='$KEY_ID'].keyId" --output text | grep -q "$KEY_ID"; then
  alc apigateway create-usage-plan-key --usage-plan-id "$USAGE_ID" \
    --key-id "$KEY_ID" --key-type API_KEY >/dev/null
fi
echo "[init] API Gateway REST: $API_ID (stage dev)"

cat > "$STATE/env" <<EOF
export AWS_REGION="$REGION"
export REST_API_ID="$API_ID"
export TOPIC_ARN="$TOPIC_ARN"
export QUEUE_URL="$QUEUE_URL"
export DLQ_URL="$DLQ_URL"
export RAW_BUCKET="$RAW_BUCKET"
export PROCESSED_BUCKET="$PROCESSED_BUCKET"
export MEDIA_TABLE="$TABLE"
export PUBLIC_S3_ENDPOINT="$PUBLIC_S3_ENDPOINT"
EOF

cat <<EOF

[init] Ambiente pronto.
  API base : http://localhost:4566/restapis/$API_ID/dev/_user_request_
  API key  : $STATE/api-key.txt
  Fila     : $QUEUE_URL
  DLQ      : $DLQ_URL
  Próximo  : scripts/test-flow.sh
EOF
