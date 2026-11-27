# Serviço de Transcodificação de Mídia — AWS + LocalStack

Serviço serverless orientado a eventos que recebe fotos e vídeos, persiste metadados, processa os arquivos de forma assíncrona e notifica o cliente sobre o progresso. Tudo roda localmente com [LocalStack](https://localstack.cloud/).

## Arquitetura

```
                    ┌──────────────┐
   POST /media ───► │ API Gateway  │ (API Key obrigatória)
   GET /media/{id}  │  (REST/dev)  │
                    └──────┬───────┘
                           │
                ┌──────────┴──────────┐
                ▼                     ▼
      ┌──────────────────┐   ┌────────────────────┐
      │ upload-handler   │   │ status-handler     │
      │ (Lambda)         │   │ (Lambda)           │
      └───┬──────┬───────┘   └─────────┬──────────┘
          │      │                     │
          │      │  PENDING            │ GetItem
          ▼      ▼                     ▼
   ┌──────────┐  ┌──────────────┐  ┌──────────────┐
   │ S3 raw   │  │ DynamoDB     │  │ MediaMetadata│
   │ bucket   │  │ MediaMetadata│  └──────────────┘
   └────┬─────┘  └──────┬───────┘
        │ s3:ObjectCreated:*   ▲ update status
        ▼                      │
   ┌──────────────────┐        │
   │ SQS queue + DLQ  │────────┘
   └────────┬─────────┘
            ▼
   ┌────────────────────┐   ┌──────────────────────────┐
   │ transcode-worker   │──►│ S3 processed bucket      │
   │ PROCESSING → DONE  │   │ + SNS status-notifications│
   └────────────────────┘   └──────────────────────────┘
```

### Componentes

| Recurso | Nome | Função |
|---|---|---|
| API Gateway REST | `media-transcode-api` (stage `dev`) | `POST /media`, `GET /media/{id}`, `GET /health`. API Key obrigatória nos endpoints de mídia. |
| S3 | `raw-media-bucket` | Arquivos originais; dispara notificação `s3:ObjectCreated:*`. |
| S3 | `processed-media-bucket` | Resultado da transcodificação/backup. |
| DynamoDB | `MediaMetadata` | `media_id` (PK), `remetente`, `usuario`, `timestamp`, `status`, `s3_url`, chaves e metadados de saída. |
| SQS | `media-transcode-queue` | Esteira assíncrona, com DLQ `media-transcode-dlq` (`maxReceiveCount=3`). |
| SNS | `media-status-notifications` | Eventos `PROCESSING`, `COMPLETED` e `FAILED`. |
| Lambda | `upload-handler` | Valida, salva no S3, grava `PENDING` e envia mensagem SQS. |
| Lambda | `transcode-worker` | Consome SQS, transcodifica, atualiza status e publica no SNS. |
| Lambda | `status-handler` | Consulta `GET /media/{id}` no DynamoDB. |

**Deduplicação:** o upload pelo API e o trigger do S3 geram duas mensagens para o mesmo arquivo. O worker usa uma condição no DynamoDB (`status IN (PENDING, FAILED) → PROCESSING`) para processar apenas uma vez. Falhas reais voltam para a fila e, após 3 tentativas, vão para a DLQ.

**Transcodificação:** usa Pillow para imagens e FFmpeg para vídeos quando disponíveis. Se as bibliotecas/binários não estiverem no container de execução, o worker aplica um mock leve e registra o modo em `transcode_mode` (`pillow`, `ffmpeg`, `mock(no-pillow)`, `mock(no-ffmpeg-or-invalid-input)`).

## Pré-requisitos

- Docker + Docker Compose v2 (com o socket `/var/run/docker.sock` disponível — o LocalStack usa-o para iniciar os runtimes das Lambdas)
- Python 3.11+ com `pip`
- `curl`
- Acesso à internet na primeira execução (para `pip install`/pull da imagem). Sem rede, o build segue em modo mock.

## Como executar

```bash
cd /projetos/projeto-transcodificacao-serverless
chmod +x scripts/*.sh

# 1. Empacota as Lambdas (zips em build/)
./scripts/build-lambdas.sh

# 2. Sobe o LocalStack e cria toda a infraestrutura
./scripts/init-localstack.sh

# 3. Teste ponta a ponta
./scripts/test-flow.sh
```

Para parar e apagar o estado local:

```bash
docker compose down -v
rm -rf build .localstack
```

> Não são executados `git commit`, `git push` nem qualquer alteração no histórico do Git.

## Usando a API

A chave gerada fica em `.localstack/api-key.txt`. A URL base do REST API é impressa pelo `init-localstack.sh` e segue o formato:

```
http://localhost:4566/restapis/<REST_API_ID>/dev/_user_request_
```

### Enviar mídia

```bash
API=http://localhost:4566/restapis/<REST_API_ID>/dev/_user_request_
KEY=$(cat .localstack/api-key.txt)

BASE64=$(base64 -w0 foto.png)   # macOS: base64 -i foto.png | tr -d '\n'

curl -sS -X POST "$API/media" \
  -H "Content-Type: application/json" \
  -H "x-api-key: $KEY" \
  -d "{\"file_name\":\"foto.png\",\"sender\":\"joao\",\"user_id\":\"user-001\",\"content_type\":\"image/png\",\"content_base64\":\"$BASE64\"}"
```

Resposta `202`:

```json
{
  "media_id": "6f0b...",
  "status": "PENDING",
  "s3_url": "http://localhost:4566/raw-media-bucket/raw/user-001/6f0b.../foto.png",
  "bucket": "raw-media-bucket",
  "key": "raw/user-001/6f0b.../foto.png"
}
```

Campos obrigatórios: `file_name`, `sender`, `user_id` e `content_base64` (ou `content`). Opcionais: `media_id`, `content_type`. Limite padrão: 10 MB (`MAX_UPLOAD_BYTES`).

### Consultar status

```bash
curl -sS "$API/media/<media_id>" -H "x-api-key: $KEY"
```

Exemplo de resposta final:

```json
{
  "media_id": "6f0b...",
  "remetente": "joao",
  "usuario": "user-001",
  "timestamp": "2026-11-27T12:00:00+00:00",
  "status": "COMPLETED",
  "s3_url": "http://localhost:4566/processed-media-bucket/processed/6f0b.../foto-transcoded.jpg",
  "raw_key": "raw/user-001/6f0b.../foto.png",
  "output_key": "processed/6f0b.../foto-transcoded.jpg",
  "transcode_mode": "pillow",
  "output_size": 1234
}
```

Estados: `PENDING` → `PROCESSING` → `COMPLETED` ou `FAILED`.

### Upload direto no S3 (trigger alternativo)

```bash
docker compose exec -T localstack awslocal s3api put-object \
  --bucket raw-media-bucket \
  --key raw/direct/minha-foto.png \
  --body minha-foto.png \
  --content-type image/png \
  --metadata '{"media-id":"s3-direct-001","sender":"joao","user-id":"user-001"}'
```

O worker cria o registro no DynamoDB quando o objeto ainda não tem metadados persistidos.

## Comandos úteis

```bash
# Logs do LocalStack (inclui logs das Lambdas)
docker compose logs -f localstack

# Estado dos serviços
curl -s http://localhost:4566/_localstack/health | python3 -m json.tool

# Mensagens na DLQ
docker compose exec -T localstack awslocal sqs get-queue-attributes \
  --queue-url "$(grep DLQ_URL .localstack/env | cut -d= -f2-)" \
  --attribute-names ApproximateNumberOfMessages

# Notificações SNS capturadas pelo teste
python3 -m json.tool .localstack/sns-messages.json
```

## Estrutura de arquivos

```
.
├── docker-compose.yml               # LocalStack: s3,lambda,dynamodb,sqs,sns,apigateway,iam
├── config/
│   ├── lambda-trust-policy.json     # assume role para Lambda
│   └── lambda-policy.json           # S3, DynamoDB, SQS, SNS, CloudWatch Logs
├── lambdas/
│   ├── upload-handler/index.py
│   ├── transcode-worker/index.py
│   └── status-handler/index.py
├── scripts/
│   ├── build-lambdas.sh             # empacotamento dos zips
│   ├── init-localstack.sh           # provisionamento idempotente com awslocal
│   └── test-flow.sh                 # teste ponta a ponta
├── build/                           # gerado (zips, configs temporárias)
└── .localstack/                     # gerado (estado, API key, env)
```


