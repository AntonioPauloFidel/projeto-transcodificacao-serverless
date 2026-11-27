import base64
import hashlib
import http.client
import json
import random
import socket
import struct
import subprocess
import time
import unittest
import uuid
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / ".localstack"
FIXTURES = ROOT / "build" / "regressao"
CONTAINER_FIXTURES = "/opt/lambda-packages/regressao"
COMPOSE_FILE = ROOT / "docker-compose.yml"
RUN_ID = uuid.uuid4().hex[:8]
TEST_VISIBILITY_TIMEOUT = 10
KINDS = ("imagem", "video")
SENDER = f"regressao-{RUN_ID}"


def _load_env() -> dict:
    env = {}
    for line in (STATE / "env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("export ") and "=" in line:
            key, value = line[len("export "):].split("=", 1)
            env[key] = value.strip().strip('"')
    return env


ENV = _load_env()
API_KEY = (STATE / "api-key.txt").read_text(encoding="utf-8").strip()
ENDPOINT = urlsplit(ENV.get("PUBLIC_S3_ENDPOINT", "http://localhost:4566"))
HOST = ENDPOINT.hostname or "localhost"
PORT = ENDPOINT.port or 4566
API_PATH = f"/restapis/{ENV['REST_API_ID']}/dev/_user_request_"


def awslocal(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T",
            "-e", f"AWS_DEFAULT_REGION={ENV.get('AWS_REGION', 'us-east-1')}",
            "localstack", "awslocal", *args,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"awslocal {' '.join(args[:2])} falhou: {result.stderr.strip()[:500]}")
    return result.stdout


def awslocal_json(*args: str) -> dict:
    output = awslocal(*args, "--output", "json")
    return json.loads(output) if output.strip() else {}


def _png(seed: int, size: int = 300) -> bytes:
    rnd = random.Random(seed)

    def chunk(kind: bytes, data: bytes) -> bytes:
        payload = kind + data
        return struct.pack(">I", len(data)) + payload + struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF)

    rows = b"".join(b"\x00" + rnd.randbytes(size * 3) for _ in range(size))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )


def _mp4(seed: int, size: int = 300_000) -> bytes:
    rnd = random.Random(seed)
    return b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso2mp41" + rnd.randbytes(size)


def media(kind: str, seed: int) -> tuple[bytes, str, str]:
    if kind == "imagem":
        return _png(seed), "regressao-foto.png", "image/png"
    return _mp4(seed), "regressao-video.mp4", "video/mp4"


def upload_body(kind: str, seed: int, media_id: str, user_id: str) -> bytes:
    content, name, content_type = media(kind, seed)
    return json.dumps(
        {
            "media_id": media_id,
            "file_name": name,
            "sender": SENDER,
            "user_id": user_id,
            "content_type": content_type,
            "content_base64": base64.b64encode(content).decode("ascii"),
        }
    ).encode("utf-8")


def post_media(body: bytes, chunk_size: int = 0, delay: float = 0.0) -> tuple[int, dict, float]:
    conn = http.client.HTTPConnection(HOST, PORT, timeout=180)
    started = time.monotonic()
    try:
        conn.putrequest("POST", f"{API_PATH}/media")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("x-api-key", API_KEY)
        conn.putheader("Content-Length", str(len(body)))
        conn.endheaders()
        step = chunk_size or len(body)
        for offset in range(0, len(body), step):
            conn.send(body[offset:offset + step])
            if delay:
                time.sleep(delay)
        response = conn.getresponse()
        data = response.read()
        return response.status, json.loads(data or b"{}"), time.monotonic() - started
    finally:
        conn.close()


def _abort(sock: socket.socket) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()


def post_media_and_drop(body: bytes, fraction: float) -> None:
    sock = socket.create_connection((HOST, PORT), timeout=10)
    head = (
        f"POST {API_PATH}/media HTTP/1.1\r\n"
        f"Host: {HOST}:{PORT}\r\n"
        "Content-Type: application/json\r\n"
        f"x-api-key: {API_KEY}\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    )
    sock.sendall(head.encode("ascii"))
    sock.sendall(body[: int(len(body) * fraction)])
    time.sleep(0.5)
    _abort(sock)


def get_status(media_id: str) -> dict:
    conn = http.client.HTTPConnection(HOST, PORT, timeout=30)
    try:
        conn.request("GET", f"{API_PATH}/media/{media_id}", headers={"x-api-key": API_KEY})
        response = conn.getresponse()
        data = response.read()
        return json.loads(data) if response.status == 200 else {}
    finally:
        conn.close()


def wait_status(media_ids: list[str], wanted: set[str], timeout: float = 90) -> dict[str, dict]:
    deadline = time.monotonic() + timeout
    latest = {media_id: {} for media_id in media_ids}
    while time.monotonic() < deadline:
        for media_id in media_ids:
            latest[media_id] = get_status(media_id)
        if all(item.get("status") in wanted for item in latest.values()):
            break
        time.sleep(1)
    return latest


def download(url: str, start: int = 0, stop_after: int | None = None, delay: float = 0.0) -> tuple[int, str, bytes]:
    parts = urlsplit(url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=60)
    conn.request("GET", parts.path, headers={"Range": f"bytes={start}-"} if start else {})
    response = conn.getresponse()
    received = bytearray()
    try:
        while stop_after is None or len(received) < stop_after:
            size = 8192 if stop_after is None else min(8192, stop_after - len(received))
            data = response.read(size)
            if not data:
                break
            received.extend(data)
            if delay:
                time.sleep(delay)
        return response.status, response.getheader("Content-Range") or "", bytes(received)
    finally:
        if stop_after is not None and conn.sock is not None:
            _abort(conn.sock)
        conn.close()


def get_item(media_id: str) -> dict:
    result = awslocal_json(
        "dynamodb", "get-item",
        "--table-name", ENV["MEDIA_TABLE"],
        "--key", json.dumps({"media_id": {"S": media_id}}),
        "--consistent-read",
    )
    return result.get("Item", {})


def put_record(media_id: str, bucket: str, key: str, kind: str, status: str, started_at: str | None = None) -> None:
    content, name, content_type = media(kind, 0)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    item = {
        "media_id": {"S": media_id},
        "remetente": {"S": SENDER},
        "usuario": {"S": SENDER},
        "timestamp": {"S": now},
        "status": {"S": status},
        "s3_url": {"S": f"{ENV['PUBLIC_S3_ENDPOINT']}/{bucket}/{key}"},
        "raw_key": {"S": key},
        "file_name": {"S": name},
        "content_type": {"S": content_type},
        "size_bytes": {"N": str(len(content))},
    }
    if started_at:
        item["processing_started_at"] = {"S": started_at}
    awslocal("dynamodb", "put-item", "--table-name", ENV["MEDIA_TABLE"], "--item", json.dumps(item))


def write_fixture(name: str, data: bytes) -> str:
    FIXTURES.mkdir(parents=True, exist_ok=True)
    (FIXTURES / name).write_bytes(data)
    return f"{CONTAINER_FIXTURES}/{name}"


def put_object(bucket: str, key: str, kind: str, metadata: dict | None = None) -> None:
    content, _, content_type = media(kind, 0)
    body_path = write_fixture(f"{RUN_ID}-{kind}-{hashlib.sha256(key.encode()).hexdigest()[:8]}", content)
    args = ["s3api", "put-object", "--bucket", bucket, "--key", key, "--body", body_path, "--content-type", content_type]
    if metadata:
        meta_path = write_fixture(f"{RUN_ID}-meta-{hashlib.sha256(key.encode()).hexdigest()[:8]}.json", json.dumps(metadata).encode())
        args += ["--metadata", f"file://{meta_path}"]
    awslocal(*args)


def raw_objects(prefix: str) -> list:
    return awslocal_json("s3api", "list-objects-v2", "--bucket", ENV["RAW_BUCKET"], "--prefix", prefix).get("Contents", [])


def send_job(media_id: str, bucket: str, key: str, queue_url: str | None = None) -> None:
    body = {"media_id": media_id, "bucket": bucket, "key": key, "source": "teste-regressao"}
    awslocal("sqs", "send-message", "--queue-url", queue_url or ENV["QUEUE_URL"], "--message-body", json.dumps(body))


def queue_size(queue_url: str) -> int:
    attrs = awslocal_json(
        "sqs", "get-queue-attributes", "--queue-url", queue_url,
        "--attribute-names", "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
    ).get("Attributes", {})
    return int(attrs.get("ApproximateNumberOfMessages", 0)) + int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))


class RegressaoQuedasEInternetLenta(unittest.TestCase):
    report: list[str] = []
    events: list[tuple[str, str]] = []

    @classmethod
    def setUpClass(cls) -> None:
        attrs = awslocal_json(
            "sqs", "get-queue-attributes", "--queue-url", ENV["QUEUE_URL"], "--attribute-names", "VisibilityTimeout"
        ).get("Attributes", {})
        original = attrs.get("VisibilityTimeout", "120")
        cls.addClassCleanup(
            awslocal, "sqs", "set-queue-attributes", "--queue-url", ENV["QUEUE_URL"],
            "--attributes", f"VisibilityTimeout={original}",
        )
        awslocal(
            "sqs", "set-queue-attributes", "--queue-url", ENV["QUEUE_URL"],
            "--attributes", f"VisibilityTimeout={TEST_VISIBILITY_TIMEOUT}",
        )
        awslocal("sqs", "purge-queue", "--queue-url", ENV["QUEUE_URL"])
        awslocal("sqs", "purge-queue", "--queue-url", ENV["DLQ_URL"])

        cls.status_queue_url = awslocal_json(
            "sqs", "create-queue", "--queue-name", "media-status-regressao-queue"
        )["QueueUrl"]
        awslocal("sqs", "purge-queue", "--queue-url", cls.status_queue_url)
        queue_arn = awslocal_json(
            "sqs", "get-queue-attributes", "--queue-url", cls.status_queue_url, "--attribute-names", "QueueArn"
        )["Attributes"]["QueueArn"]
        awslocal("sns", "subscribe", "--topic-arn", ENV["TOPIC_ARN"], "--protocol", "sqs", "--notification-endpoint", queue_arn)

    @classmethod
    def tearDownClass(cls) -> None:
        print("\n[regressao] O que aconteceu com cada mídia:")
        for line in cls.report:
            print(f"  - {line}")

    def note(self, kind: str, text: str) -> None:
        type(self).report.append(f"{self._testMethodName[5:7]} {kind}: {text}")

    def temp_bucket(self, suffix: str) -> str:
        bucket = f"regressao-{suffix}-{RUN_ID}"
        self.addClassCleanup(awslocal, "s3", "rb", f"s3://{bucket}", "--force", check=False)
        return bucket

    def notifications_for(self, media_id: str) -> list[str]:
        while True:
            result = awslocal_json(
                "sqs", "receive-message", "--queue-url", self.status_queue_url,
                "--max-number-of-messages", "10", "--wait-time-seconds", "1",
            )
            messages = result.get("Messages", [])
            if not messages:
                break
            for message in messages:
                envelope = json.loads(message["Body"])
                event = json.loads(envelope.get("Message", "{}"))
                type(self).events.append((event.get("media_id"), event.get("status")))
                awslocal("sqs", "delete-message", "--queue-url", self.status_queue_url, "--receipt-handle", message["ReceiptHandle"])
        return [status for event_id, status in type(self).events if event_id == media_id]

    def test_01_upload_que_cai_no_meio_nao_deixa_lixo_e_reenvio_conclui(self) -> None:
        for kind in KINDS:
            with self.subTest(midia=kind):
                media_id = f"reg-queda-upload-{kind}-{RUN_ID}"
                user_id = f"reg-queda-upload-{kind}-{RUN_ID}"
                body = upload_body(kind, 1, media_id, user_id)

                post_media_and_drop(body, 0.4)
                time.sleep(3)
                self.assertEqual(get_item(media_id), {}, "upload interrompido criou registro no DynamoDB")
                self.assertEqual(raw_objects(f"raw/{user_id}/"), [], "upload interrompido deixou arquivo parcial no S3")

                status, response, _ = post_media(body)
                self.assertEqual(status, 202, response)
                final = wait_status([media_id], {"COMPLETED", "FAILED"})[media_id]
                self.assertEqual(final.get("status"), "COMPLETED", final)
                self.assertEqual(len(raw_objects(f"raw/{user_id}/")), 1, "reenvio duplicou o arquivo original")
                self.note(
                    kind,
                    "conexão caiu com 40% do upload enviado: nada foi salvo; "
                    f"o reenvio recomeça do zero e termina COMPLETED ({final.get('transcode_mode')})",
                )

    def test_02_upload_com_internet_lenta_conclui(self) -> None:
        for kind in KINDS:
            with self.subTest(midia=kind):
                media_id = f"reg-upload-lento-{kind}-{RUN_ID}"
                body = upload_body(kind, 2, media_id, f"reg-upload-lento-{RUN_ID}")

                status, response, elapsed = post_media(body, chunk_size=16 * 1024, delay=0.3)
                self.assertEqual(status, 202, response)
                self.assertGreaterEqual(elapsed, 5, "o envio não foi lento como o teste pretendia")
                final = wait_status([media_id], {"COMPLETED", "FAILED"})[media_id]
                self.assertEqual(final.get("status"), "COMPLETED", final)
                rate = len(body) / 1024 / elapsed
                self.note(kind, f"upload de {len(body) // 1024} KB a ~{rate:.0f} KB/s levou {elapsed:.1f}s e terminou COMPLETED")

    def test_03_download_que_cai_retoma_de_onde_parou(self) -> None:
        for kind in KINDS:
            with self.subTest(midia=kind):
                media_id = f"reg-download-{kind}-{RUN_ID}"
                status, response, _ = post_media(upload_body(kind, 3, media_id, f"reg-download-{RUN_ID}"))
                self.assertEqual(status, 202, response)
                final = wait_status([media_id], {"COMPLETED", "FAILED"})[media_id]
                self.assertEqual(final.get("status"), "COMPLETED", final)
                url = final["s3_url"]

                full_status, _, full = download(url)
                self.assertEqual(full_status, 200)
                self.assertEqual(len(full), final.get("output_size"), "tamanho baixado difere do registrado")
                cut = len(full) * 2 // 5

                _, _, first = download(url, stop_after=cut, delay=0.02)
                self.assertEqual(len(first), cut)
                resume_status, content_range, rest = download(url, start=len(first))
                self.assertEqual(resume_status, 206, "o servidor não aceitou retomar com Range")
                self.assertTrue(content_range.startswith(f"bytes {cut}-"), content_range)
                self.assertEqual(len(rest), len(full) - cut, "a retomada baixou de novo o que já tinha chegado")
                self.assertEqual(hashlib.sha256(first + rest).hexdigest(), hashlib.sha256(full).hexdigest())
                self.note(
                    kind,
                    f"download caiu com {cut} de {len(full)} bytes; retomou do byte {cut} "
                    "(HTTP 206) e o arquivo final é idêntico (sha256)",
                )

    def test_04_api_cai_depois_de_salvar_no_s3_e_trigger_conclui(self) -> None:
        media_ids = {}
        for kind in KINDS:
            media_id = f"reg-api-caiu-{kind}-{RUN_ID}"
            _, name, _ = media(kind, 0)
            put_object(
                ENV["RAW_BUCKET"],
                f"raw/reg-api-caiu-{RUN_ID}/{media_id}/{name}",
                kind,
                {"media-id": media_id, "sender": SENDER, "user-id": SENDER, "original-name": name},
            )
            media_ids[kind] = media_id

        finals = wait_status(list(media_ids.values()), {"COMPLETED", "FAILED"})
        for kind, media_id in media_ids.items():
            with self.subTest(midia=kind):
                self.assertEqual(finals[media_id].get("status"), "COMPLETED", finals[media_id])
                self.assertEqual(finals[media_id].get("source"), "s3-event")
                self.note(kind, "API caiu depois de gravar no S3 (sem registro nem fila): o trigger do S3 recuperou e terminou COMPLETED")

    def test_05_armazenamento_cai_e_retry_automatico_conclui(self) -> None:
        bucket = self.temp_bucket("origem-temporaria")
        media_ids = {}
        for kind in KINDS:
            media_id = f"reg-storage-temp-{kind}-{RUN_ID}"
            key = f"raw/{media_id}/{media(kind, 0)[1]}"
            put_record(media_id, bucket, key, kind, "PENDING")
            send_job(media_id, bucket, key)
            media_ids[kind] = (media_id, key)

        failed = wait_status([m for m, _ in media_ids.values()], {"FAILED", "COMPLETED"}, timeout=60)
        for kind, (media_id, _) in media_ids.items():
            with self.subTest(midia=kind, fase="queda"):
                self.assertEqual(failed[media_id].get("status"), "FAILED", failed[media_id])
                self.assertTrue(failed[media_id].get("error_message"))

        awslocal("s3api", "create-bucket", "--bucket", bucket)
        for kind, (_, key) in media_ids.items():
            put_object(bucket, key, kind)

        finals = wait_status([m for m, _ in media_ids.values()], {"COMPLETED"}, timeout=90)
        for kind, (media_id, _) in media_ids.items():
            with self.subTest(midia=kind, fase="volta"):
                self.assertEqual(finals[media_id].get("status"), "COMPLETED", finals[media_id])
                events = self.notifications_for(media_id)
                self.assertIn("FAILED", events)
                self.assertIn("COMPLETED", events)
                self.note(
                    kind,
                    "armazenamento do original fora do ar: ficou FAILED e avisou no SNS; quando voltou, "
                    f"a fila tentou de novo sozinha e terminou COMPLETED (eventos: {' -> '.join(events)})",
                )

    def test_06_falha_persistente_vai_para_dlq_e_pode_ser_reprocessada(self) -> None:
        bucket = self.temp_bucket("origem-fora")
        media_ids = {}
        for kind in KINDS:
            media_id = f"reg-dlq-{kind}-{RUN_ID}"
            key = f"raw/{media_id}/{media(kind, 0)[1]}"
            put_record(media_id, bucket, key, kind, "PENDING")
            send_job(media_id, bucket, key)
            media_ids[kind] = (media_id, key)

        deadline = time.monotonic() + 180
        while time.monotonic() < deadline and queue_size(ENV["DLQ_URL"]) < len(media_ids):
            time.sleep(3)
        self.assertGreaterEqual(queue_size(ENV["DLQ_URL"]), len(media_ids), "as mensagens não chegaram na DLQ")
        for kind, (media_id, _) in media_ids.items():
            with self.subTest(midia=kind, fase="dlq"):
                self.assertEqual(get_status(media_id).get("status"), "FAILED")

        awslocal("s3api", "create-bucket", "--bucket", bucket)
        for kind, (_, key) in media_ids.items():
            put_object(bucket, key, kind)

        wanted = {media_id for media_id, _ in media_ids.values()}
        moved = set()
        deadline = time.monotonic() + 30
        while moved != wanted and time.monotonic() < deadline:
            result = awslocal_json(
                "sqs", "receive-message", "--queue-url", ENV["DLQ_URL"],
                "--max-number-of-messages", "10", "--wait-time-seconds", "1",
            )
            for message in result.get("Messages", []):
                media_id = json.loads(message["Body"]).get("media_id")
                if media_id in wanted:
                    awslocal("sqs", "send-message", "--queue-url", ENV["QUEUE_URL"], "--message-body", message["Body"])
                    awslocal("sqs", "delete-message", "--queue-url", ENV["DLQ_URL"], "--receipt-handle", message["ReceiptHandle"])
                    moved.add(media_id)
        self.assertEqual(moved, wanted, "não foi possível reenviar as mensagens da DLQ")

        finals = wait_status(list(wanted), {"COMPLETED"}, timeout=90)
        for kind, (media_id, _) in media_ids.items():
            with self.subTest(midia=kind, fase="reprocesso"):
                self.assertEqual(finals[media_id].get("status"), "COMPLETED", finals[media_id])
                self.note(
                    kind,
                    "armazenamento ficou fora além das 3 tentativas: mensagem foi para a DLQ e a mídia ficou FAILED; "
                    "reenviando a mensagem da DLQ depois da volta, terminou COMPLETED",
                )

    def test_07_worker_cai_no_meio_do_processamento_e_mensagem_volta(self) -> None:
        bucket = self.temp_bucket("origem-worker")
        awslocal("s3api", "create-bucket", "--bucket", bucket)
        claimed_at = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat(timespec="seconds")
        media_ids = {}
        for kind in KINDS:
            media_id = f"reg-worker-caiu-{kind}-{RUN_ID}"
            key = f"raw/{media_id}/{media(kind, 0)[1]}"
            put_object(bucket, key, kind)
            put_record(media_id, bucket, key, kind, "PROCESSING", started_at=claimed_at)
            send_job(media_id, bucket, key)
            media_ids[kind] = media_id

        finals = wait_status(list(media_ids.values()), {"COMPLETED", "FAILED"}, timeout=45)
        for kind, media_id in media_ids.items():
            with self.subTest(midia=kind):
                status = finals[media_id].get("status")
                self.note(
                    kind,
                    "worker morreu depois de marcar PROCESSING (15 min atrás) e a mensagem voltou para a fila: "
                    f"status final {status}",
                )
                self.assertEqual(
                    status,
                    "COMPLETED",
                    f"{kind} ficou presa em {status}: o worker descarta a mensagem reentregue porque só reprocessa "
                    "PENDING/FAILED, então uma queda no meio do processamento não volta de onde parou",
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
