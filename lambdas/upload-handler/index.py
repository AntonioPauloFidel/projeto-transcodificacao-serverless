"""API Gateway handler: valida o upload, grava o original no S3 e enfileira o job."""

import base64
import binascii
import json
import logging
import mimetypes
import os
import re
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "raw-media-bucket")
MEDIA_TABLE = os.environ.get("MEDIA_TABLE", "MediaMetadata")
QUEUE_URL = os.environ.get("QUEUE_URL", "")
PUBLIC_ENDPOINT = os.environ.get("PUBLIC_S3_ENDPOINT", "http://localhost:4566").rstrip("/")
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(10 * 1024 * 1024)))

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.client("dynamodb", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, ensure_ascii=False),
    }


def _safe_name(file_name: str) -> str:
    name = os.path.basename(file_name)
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name).strip("._") or "upload.bin"
    return name[:120]


def _parse_event(event: dict) -> dict:
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8")
    if isinstance(body, str):
        return json.loads(body)
    if isinstance(body, dict):
        return body
    raise ValueError("Corpo da requisição inválido: esperado JSON.")


def handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return _response(200, {"ok": True})

    try:
        payload = _parse_event(event)

        sender = str(payload.get("sender") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        file_name = str(payload.get("file_name") or "").strip()
        if not sender or not user_id or not file_name:
            raise ValueError("Campos obrigatórios ausentes: sender, user_id e file_name.")

        encoded = payload.get("content_base64")
        if encoded is not None:
            try:
                content = base64.b64decode(encoded, validate=True)
            except (binascii.Error, TypeError, ValueError) as exc:
                raise ValueError("content_base64 não é um base64 válido.") from exc
        elif payload.get("content") is not None:
            content = str(payload["content"]).encode("utf-8")
        else:
            raise ValueError("Informe content_base64 (ou content).")

        if not content:
            raise ValueError("O arquivo está vazio.")
        if len(content) > MAX_UPLOAD_BYTES:
            raise ValueError(f"Arquivo excede o limite de {MAX_UPLOAD_BYTES} bytes.")

        media_id = str(payload.get("media_id") or uuid.uuid4())
        timestamp = _now()
        safe_name = _safe_name(file_name)
        key = f"raw/{user_id}/{media_id}/{safe_name}"
        content_type = (
            payload.get("content_type")
            or mimetypes.guess_type(safe_name)[0]
            or "application/octet-stream"
        )

        metadata = {
            "media-id": media_id,
            "sender": sender,
            "user-id": user_id,
            "timestamp": timestamp,
            "original-name": safe_name,
        }
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=content,
            ContentType=content_type,
            Metadata=metadata,
        )

        raw_url = f"{PUBLIC_ENDPOINT}/{RAW_BUCKET}/{key}"
        item = {
            "media_id": {"S": media_id},
            "remetente": {"S": sender},
            "usuario": {"S": user_id},
            "timestamp": {"S": timestamp},
            "status": {"S": "PENDING"},
            "s3_url": {"S": raw_url},
            "raw_key": {"S": key},
            "file_name": {"S": safe_name},
            "content_type": {"S": content_type},
            "size_bytes": {"N": str(len(content))},
        }
        dynamodb.put_item(TableName=MEDIA_TABLE, Item=item)

        if not QUEUE_URL:
            raise ValueError("QUEUE_URL não configurada na Lambda de entrada.")
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(
                {
                    "media_id": media_id,
                    "bucket": RAW_BUCKET,
                    "key": key,
                    "source": "upload-handler",
                    "request_id": getattr(context, "aws_request_id", None),
                }
            ),
            MessageAttributes={
                "source": {"DataType": "String", "StringValue": "upload-handler"},
                "media_id": {"DataType": "String", "StringValue": media_id},
            },
        )

        logger.info("Upload aceito: media_id=%s key=%s", media_id, key)
        return _response(
            202,
            {
                "media_id": media_id,
                "status": "PENDING",
                "s3_url": raw_url,
                "bucket": RAW_BUCKET,
                "key": key,
            },
        )

    except ValueError as exc:
        logger.warning("Requisição inválida: %s", exc)
        return _response(400, {"error": str(exc)})
    except json.JSONDecodeError as exc:
        logger.warning("JSON inválido: %s", exc)
        return _response(400, {"error": "JSON inválido no corpo da requisição."})
    except (BotoCoreError, ClientError) as exc:
        logger.exception("Falha ao persistir/enfileirar upload")
        return _response(500, {"error": "Falha interna ao registrar a mídia.", "detail": str(exc)})
