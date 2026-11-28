"""Consumidor SQS: transcodifica a mídia, atualiza o DynamoDB e publica no SNS."""

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, unquote_plus

import boto3
from botocore.exceptions import ClientError

try:  # dependência opcional: sem Pillow o worker usa um mock de imagem.
    from PIL import Image
except ImportError:  # pragma: no cover - depende do pacote empacotado
    Image = None

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "raw-media-bucket")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "processed-media-bucket")
MEDIA_TABLE = os.environ.get("MEDIA_TABLE", "MediaMetadata")
TOPIC_ARN = os.environ.get("TOPIC_ARN", "")
PUBLIC_ENDPOINT = os.environ.get("PUBLIC_S3_ENDPOINT", "http://localhost:4566").rstrip("/")
MAX_IMAGE_SIZE = int(os.environ.get("MAX_IMAGE_SIZE", "1280"))
PROCESSING_TIMEOUT_SECONDS = int(os.environ.get("PROCESSING_TIMEOUT_SECONDS", "120"))

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.client("dynamodb", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)

VIDEO_SUFFIXES = {".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v"}


class ProcessingInProgress(Exception):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _url(bucket: str, key: str) -> str:
    return f"{PUBLIC_ENDPOINT}/{bucket}/{quote(key, safe='/')}"


def _notify(media_id: str, status: str, **extra) -> None:
    if not TOPIC_ARN:
        logger.warning("TOPIC_ARN não configurado; notificação %s ignorada", status)
        return
    message = {"media_id": media_id, "status": status, "timestamp": _now(), **extra}
    try:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"Media {status}",
            Message=json.dumps(message, ensure_ascii=False),
            MessageAttributes={
                "status": {"DataType": "String", "StringValue": status},
                "media_id": {"DataType": "String", "StringValue": media_id},
            },
        )
    except ClientError:
        logger.exception("Falha ao publicar notificação SNS (%s)", status)


def _ensure_record(media_id: str, bucket: str, key: str) -> None:
    """Cria o registro quando o evento veio diretamente do bucket S3."""
    existing = dynamodb.get_item(
        TableName=MEDIA_TABLE,
        Key={"media_id": {"S": media_id}},
        ConsistentRead=True,
    )
    if "Item" in existing:
        return

    head = s3.head_object(Bucket=bucket, Key=key)
    meta = head.get("Metadata", {})
    item = {
        "media_id": {"S": media_id},
        "remetente": {"S": meta.get("sender", "s3-event")},
        "usuario": {"S": meta.get("user-id", "unknown")},
        "timestamp": {"S": meta.get("timestamp") or _now()},
        "status": {"S": "PENDING"},
        "s3_url": {"S": _url(bucket, key)},
        "raw_key": {"S": key},
        "file_name": {"S": meta.get("original-name") or Path(key).name},
        "content_type": {"S": head.get("ContentType") or "application/octet-stream"},
        "size_bytes": {"N": str(head.get("ContentLength", 0))},
        "source": {"S": "s3-event"},
    }
    try:
        dynamodb.put_item(
            TableName=MEDIA_TABLE,
            Item=item,
            ConditionExpression="attribute_not_exists(media_id)",
        )
        logger.info("Registro criado a partir do evento S3: %s", media_id)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise


def _claim(media_id: str) -> bool:
    """Deduplica eventos: PENDING/FAILED ou PROCESSING abandonado podem virar PROCESSING."""
    stale_before = (
        datetime.now(timezone.utc) - timedelta(seconds=PROCESSING_TIMEOUT_SECONDS)
    ).isoformat(timespec="seconds")
    try:
        dynamodb.update_item(
            TableName=MEDIA_TABLE,
            Key={"media_id": {"S": media_id}},
            UpdateExpression="SET #s = :processing, processing_started_at = :ts",
            ConditionExpression=(
                "attribute_exists(media_id) AND (#s IN (:pending, :failed) "
                "OR (#s = :processing AND processing_started_at < :stale_before))"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":processing": {"S": "PROCESSING"},
                ":pending": {"S": "PENDING"},
                ":failed": {"S": "FAILED"},
                ":stale_before": {"S": stale_before},
                ":ts": {"S": _now()},
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
            raise

    current = dynamodb.get_item(
        TableName=MEDIA_TABLE,
        Key={"media_id": {"S": media_id}},
        ConsistentRead=True,
    ).get("Item", {})
    if current.get("status", {}).get("S") == "PROCESSING":
        started_at = current.get("processing_started_at", {}).get("S", "?")
        raise ProcessingInProgress(f"{media_id} em PROCESSING desde {started_at}")
    logger.info("Evento duplicado/ignorado para %s", media_id)
    return False


def _download(bucket: str, key: str) -> Path:
    target = Path(tempfile.gettempdir()) / f"raw-{uuid.uuid4().hex}-{Path(key).name}"
    s3.download_file(bucket, key, str(target))
    return target


def _transcode_image(src: Path) -> tuple[Path, str, str]:
    if Image is not None:
        try:
            with Image.open(src) as image:
                image = image.convert("RGB")
                image.thumbnail((MAX_IMAGE_SIZE, MAX_IMAGE_SIZE))
                out = src.with_name(f"{src.stem}-transcoded.jpg")
                image.save(out, "JPEG", quality=85, optimize=True)
                return out, "image/jpeg", "pillow"
        except Exception as exc:  # imagem corrompida cai no mock controlado
            logger.warning("Pillow não conseguiu processar %s: %s", src.name, exc)

    out = src.with_name(f"{src.stem}-mock.jpg")
    out.write_bytes(b"MOCK-JPEG\n" + src.read_bytes())
    return out, "image/jpeg", "mock(no-pillow)"


def _transcode_video(src: Path) -> tuple[Path, str, str]:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        out = src.with_name(f"{src.stem}-transcoded.mp4")
        scale = f"scale='min({MAX_IMAGE_SIZE},iw)':-2"
        try:
            completed = subprocess.run(
                [
                    ffmpeg, "-y", "-loglevel", "error", "-i", str(src),
                    "-vf", scale, "-t", "30", "-c:v", "libx264",
                    "-preset", "veryfast", "-c:a", "aac", str(out),
                ],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            if completed.returncode == 0 and out.exists() and out.stat().st_size > 0:
                return out, "video/mp4", "ffmpeg"
            logger.warning("FFmpeg falhou: %s", completed.stderr.strip()[:300])
        except (OSError, subprocess.SubprocessError) as exc:
            logger.warning("FFmpeg indisponível/falhou: %s", exc)

    out = src.with_name(f"{src.stem}-mock.mp4")
    out.write_bytes(b"MOCK-MP4\n" + src.read_bytes())
    return out, "video/mp4", "mock(no-ffmpeg-or-invalid-input)"


def _complete(media_id: str, out_key: str, content_type: str, mode: str, size: int) -> None:
    dynamodb.update_item(
        TableName=MEDIA_TABLE,
        Key={"media_id": {"S": media_id}},
        UpdateExpression=(
            "SET #s = :completed, s3_url = :url, output_key = :key, "
            "content_type_out = :ctype, transcode_mode = :mode, "
            "output_size = :size, processed_at = :ts REMOVE error_message"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":completed": {"S": "COMPLETED"},
            ":url": {"S": _url(PROCESSED_BUCKET, out_key)},
            ":key": {"S": out_key},
            ":ctype": {"S": content_type},
            ":mode": {"S": mode},
            ":size": {"N": str(size)},
            ":ts": {"S": _now()},
        },
    )
    _notify(
        media_id,
        "COMPLETED",
        s3_url=_url(PROCESSED_BUCKET, out_key),
        output_key=out_key,
        transcode_mode=mode,
    )


def _fail(media_id: str, error: str) -> None:
    dynamodb.update_item(
        TableName=MEDIA_TABLE,
        Key={"media_id": {"S": media_id}},
        UpdateExpression="SET #s = :failed, error_message = :err, failed_at = :ts",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":failed": {"S": "FAILED"},
            ":err": {"S": str(error)[:500]},
            ":ts": {"S": _now()},
        },
    )
    _notify(media_id, "FAILED", error=str(error)[:500])


def _process(media_id: str, bucket: str, key: str) -> None:
    _ensure_record(media_id, bucket, key)
    if not _claim(media_id):
        return

    _notify(media_id, "PROCESSING", raw_key=key)
    src = None
    out = None
    try:
        src = _download(bucket, key)
        suffix = src.suffix.lower()
        record = dynamodb.get_item(
            TableName=MEDIA_TABLE,
            Key={"media_id": {"S": media_id}},
            ConsistentRead=True,
        ).get("Item", {})
        content_type = record.get("content_type", {}).get("S", "application/octet-stream")

        if content_type.startswith("video/") or suffix in VIDEO_SUFFIXES:
            out, out_type, mode = _transcode_video(src)
        else:
            out, out_type, mode = _transcode_image(src)

        out_key = f"processed/{media_id}/{out.name}"
        s3.upload_file(
            str(out),
            PROCESSED_BUCKET,
            out_key,
            ExtraArgs={
                "ContentType": out_type,
                "Metadata": {
                    "media-id": media_id,
                    "source-key": key,
                    "transcode-mode": mode,
                },
            },
        )
        _complete(media_id, out_key, out_type, mode, out.stat().st_size)
        logger.info("Transcodificação concluída: media_id=%s modo=%s", media_id, mode)
    except Exception as exc:
        logger.exception("Falha na transcodificação de %s", media_id)
        try:
            _fail(media_id, str(exc))
        except Exception:  # não mascara a exceção original
            logger.exception("Falha ao registrar status FAILED de %s", media_id)
        raise
    finally:
        for path in (src, out):
            if path and path.exists():
                path.unlink(missing_ok=True)


def handler(event, context):
    retry = []
    for record in event.get("Records", []):
        body = record.get("body") or "{}"
        try:
            message = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("Mensagem SQS ignorada (JSON inválido): %s", body[:200])
            continue

        media_id = None
        key = None
        try:
            s3_records = message.get("Records")
            if (
                isinstance(s3_records, list)
                and s3_records
                and isinstance(s3_records[0], dict)
                and "s3" in s3_records[0]
            ):
                info = s3_records[0]["s3"]
                bucket = info["bucket"]["name"]
                key = unquote_plus(info["object"]["key"])
                head = s3.head_object(Bucket=bucket, Key=key)
                media_id = (head.get("Metadata") or {}).get("media-id") or hashlib.sha256(
                    key.encode("utf-8")
                ).hexdigest()[:16]
            else:
                bucket = message.get("bucket") or RAW_BUCKET
                key = message.get("key")
                media_id = message.get("media_id")
                if not key:
                    logger.warning("Mensagem sem 'key': %s", body[:200])
                    continue
                if not media_id:
                    head = s3.head_object(Bucket=bucket, Key=key)
                    media_id = (head.get("Metadata") or {}).get("media-id") or hashlib.sha256(
                        key.encode("utf-8")
                    ).hexdigest()[:16]

            _process(media_id, bucket, key)
        except ProcessingInProgress as exc:
            logger.info("Mídia ainda em processamento, a mensagem volta para a fila: %s", exc)
            retry.append(media_id)
        except Exception:
            # Inclui falhas no head_object: só esta mensagem volta para a fila,
            # o restante do lote continua sendo processado.
            label = media_id or key or record.get("messageId") or "?"
            logger.exception("Falha ao processar %s; a mensagem volta para a fila", label)
            retry.append(label)

    if retry:
        raise RuntimeError(f"Mensagens que voltam para a fila: {', '.join(retry)}")
    return {"batchItemFailures": []}
