"""API Gateway handler: consulta os metadados e o status atual no DynamoDB."""

import json
import logging
import os
from urllib.parse import unquote

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
MEDIA_TABLE = os.environ.get("MEDIA_TABLE", "MediaMetadata")

dynamodb = boto3.client("dynamodb", region_name=REGION)


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, ensure_ascii=False),
    }


def _unpack(item: dict) -> dict:
    result = {}
    for key, value in item.items():
        if "S" in value:
            result[key] = value["S"]
        elif "N" in value:
            try:
                result[key] = int(value["N"])
            except ValueError:
                result[key] = value["N"]
        else:
            result[key] = value
    return result


def handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return _response(200, {"ok": True})

    path = event.get("path") or ""
    if path.rstrip("/").endswith("/health") or event.get("httpMethod") == "HEALTH":
        return _response(200, {"ok": True, "service": "media-transcode"})

    media_id = (event.get("pathParameters") or {}).get("id")
    if not media_id:
        media_id = unquote(path.rstrip("/").split("/")[-1]) or None
    if not media_id or media_id in {"media", "health"}:
        return _response(400, {"error": "Informe o media_id no caminho: /media/{id}."})

    try:
        result = dynamodb.get_item(
            TableName=MEDIA_TABLE,
            Key={"media_id": {"S": media_id}},
            ConsistentRead=True,
        )
    except (BotoCoreError, ClientError) as exc:
        logger.exception("Falha ao consultar media_id=%s", media_id)
        return _response(500, {"error": "Falha ao consultar metadados.", "detail": str(exc)})

    if "Item" not in result:
        return _response(404, {"error": "Mídia não encontrada.", "media_id": media_id})

    return _response(200, _unpack(result["Item"]))
