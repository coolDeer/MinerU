"""研报解析 Worker.

数据源 + 状态表: 单集合 `ResearchReportRecord`
产物: AWS S3

状态机 (parseStatus):
  pending    → 待处理 (上游/人工初始化此值)
  processing → 处理中 (parseSubStatus 细分 downloading/parsing/uploading)
  completed  → 已完成 (不会再被 worker 扫到)
  failed     → 超过重试上限,需人工 (不会再被 worker 扫到)

抢任务: parseStatus=pending 或 (parseStatus=processing 且锁超时且重试未超限)

注: 直接调 do_parse(同步)而非经由 mineru-api,
    让 MLX 推理在主线程运行,避免 asyncio.to_thread 的 GPU Stream 跨线程问题。
"""
import os
import shutil
import socket
import subprocess
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import boto3
import httpx
from loguru import logger
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)


def _env_flag_enabled(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in ("0", "false", "no", "off")


def load_env_file(env_path: Path, override: bool = True) -> None:
    if not env_path.exists():
        return

    loaded = 0
    env_lines = env_path.read_text(encoding="utf-8").splitlines()
    for line_no, raw_line in enumerate(env_lines, 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            logger.warning(f"Skip invalid .env line {env_path}:{line_no}")
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            logger.warning(f"Skip empty .env key {env_path}:{line_no}")
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if override or key not in os.environ:
            os.environ[key] = value
            loaded += 1

    logger.info(f"Loaded {loaded} env vars from {env_path}")


DEFAULT_ENV_PATH = Path(__file__).with_name(".env")
ENV_PATH = Path(os.environ.get("MONGODB_WORKER_ENV_FILE", DEFAULT_ENV_PATH))
ENV_OVERRIDE = _env_flag_enabled(os.environ.get("MONGODB_WORKER_ENV_OVERRIDE"), True)
load_env_file(ENV_PATH, override=ENV_OVERRIDE)

# MinerU modules may inspect environment variables at import time.
from mineru.cli.common import do_parse


# ========== 环境变量 ==========
MONGODB_DATABASE_URL = os.environ["MONGODB_DATABASE_URL"]
MONGODB_DB = os.environ.get("MONGODB_DB")
COLL_NAME = os.environ.get("MONGODB_COLL", "ResearchReportRecord")

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.environ["AWS_S3_BUCKET_NAME"]
S3_PREFIX = os.environ.get("AWS_S3_PREFIX", "research-reports/parsed")

MINERU_BACKEND = os.environ.get("MINERU_BACKEND", "hybrid-auto-engine")

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))
LOCK_TTL_SECONDS = int(os.environ.get("LOCK_TTL_SECONDS", "3600"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
POLL_IDLE_SECONDS = int(os.environ.get("POLL_IDLE_SECONDS", "30"))
INTER_TASK_SLEEP_SECONDS = int(os.environ.get("INTER_TASK_SLEEP_SECONDS", "5"))
LIBREOFFICE_BIN = os.environ.get("LIBREOFFICE_BIN", "soffice")
DOWNLOAD_TIMEOUT_SECONDS = float(os.environ.get("DOWNLOAD_TIMEOUT_SECONDS", "300"))
DOWNLOAD_RETRIES = max(1, int(os.environ.get("DOWNLOAD_RETRIES", "3")))
DOWNLOAD_RETRY_SLEEP_SECONDS = float(
    os.environ.get("DOWNLOAD_RETRY_SLEEP_SECONDS", "5")
)
MONGODB_SERVER_SELECTION_TIMEOUT_MS = int(
    os.environ.get("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "30000")
)
MONGODB_CONNECT_TIMEOUT_MS = int(os.environ.get("MONGODB_CONNECT_TIMEOUT_MS", "20000"))
MONGODB_SOCKET_TIMEOUT_MS = int(os.environ.get("MONGODB_SOCKET_TIMEOUT_MS", "20000"))
MONGODB_OP_RETRIES = max(1, int(os.environ.get("MONGODB_OP_RETRIES", "3")))
MONGODB_OP_RETRY_SLEEP_SECONDS = float(
    os.environ.get("MONGODB_OP_RETRY_SLEEP_SECONDS", "5")
)


# ========== 状态常量 ==========
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

SUB_DOWNLOADING = "downloading"
SUB_PARSING = "parsing"
SUB_UPLOADING = "uploading"


# ========== 连接 ==========
_mongo: MongoClient | None = None


TRANSIENT_MONGO_ERRORS = (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)


def get_coll():
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(
            MONGODB_DATABASE_URL,
            serverSelectionTimeoutMS=MONGODB_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=MONGODB_CONNECT_TIMEOUT_MS,
            socketTimeoutMS=MONGODB_SOCKET_TIMEOUT_MS,
        )
    db = _mongo[MONGODB_DB] if MONGODB_DB else _mongo.get_default_database()
    if db is None:
        raise RuntimeError("MONGODB_DATABASE_URL 没带默认 DB,且未设 MONGODB_DB")
    return db[COLL_NAME]


def reset_mongo() -> None:
    global _mongo
    if _mongo is not None:
        try:
            _mongo.close()
        except Exception:
            pass
        _mongo = None


def is_transient_mongo_error(exc: Exception) -> bool:
    # PyMongo 4.x may expose socket cancellations as private _OperationCancelled.
    return isinstance(exc, TRANSIENT_MONGO_ERRORS) or type(exc).__name__ == "_OperationCancelled"


def with_mongo_retry(description: str, func):
    last_error = None
    for attempt in range(1, MONGODB_OP_RETRIES + 1):
        try:
            return func(get_coll())
        except Exception as e:
            if not is_transient_mongo_error(e):
                raise
            last_error = e
            reset_mongo()
            if attempt >= MONGODB_OP_RETRIES:
                break
            sleep_seconds = MONGODB_OP_RETRY_SLEEP_SECONDS * attempt
            logger.warning(
                f"MongoDB {description} 失败({type(e).__name__}: {e}), "
                f"{sleep_seconds:.1f}s 后重试 {attempt}/{MONGODB_OP_RETRIES}"
            )
            time.sleep(sleep_seconds)
    raise last_error


def make_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ========== 任务领取 ==========
def ensure_indexes() -> None:
    try:
        with_mongo_retry(
            "建索引",
            lambda coll: coll.create_index([("parseStatus", 1), ("parseLockedUntil", 1)]),
        )
    except Exception as e:
        logger.warning(f"建索引失败(权限不足?),Worker 仍可运行: {e}")


def claim_task() -> dict | None:
    def _claim(coll):
        now = _now()
        lock_until = now + timedelta(seconds=LOCK_TTL_SECONDS)
        return coll.find_one_and_update(
            {
                "reportUrl": {"$exists": True, "$nin": [None, ""]},
                "$or": [
                    {"parseStatus": STATUS_PENDING},
                    {
                        "parseStatus": STATUS_PROCESSING,
                        "parseLockedUntil": {"$lt": now},
                        "parseRetryCount": {"$lt": MAX_RETRIES},
                    },
                ],
            },
            {
                "$set": {
                    "parseStatus": STATUS_PROCESSING,
                    "parseSubStatus": SUB_DOWNLOADING,
                    "parseLockedBy": WORKER_ID,
                    "parseLockedUntil": lock_until,
                    "parseStartedAt": now,
                    "parseUpdatedAt": now,
                },
            },
            return_document=ReturnDocument.AFTER,
            # Always prioritize the most recently published reports. createTime
            # provides a deterministic fallback for equal/missing publishDate.
            sort=[("publishDate", -1), ("createTime", -1)],
        )

    return with_mongo_retry("领取任务", _claim)


def patch(record_id, **fields) -> None:
    fields.setdefault("parseUpdatedAt", _now())
    with_mongo_retry(
        "更新任务",
        lambda coll: coll.update_one({"_id": record_id}, {"$set": fields}),
    )


# ========== 下载 + 类型识别 ==========
PDF_MAGIC = b"%PDF-"
ZIP_MAGIC = b"PK\x03\x04"
OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
SUPPORTED_SOURCE_SUFFIXES = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx"}
OLE_SUFFIXES = {"doc", "xls", "ppt"}
FILENAME_HINT_FIELDS = (
    "fileName",
    "file_name",
    "filename",
    "originalFileName",
    "reportName",
    "reportTitle",
    "title",
)


def download_file(url: str, dest: Path) -> Path:
    if not url:
        raise ValueError("reportUrl 为空")
    last_error = None
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            dest.unlink(missing_ok=True)
            with httpx.stream(
                "GET",
                url,
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
                follow_redirects=True,
            ) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_bytes():
                        f.write(chunk)
            size = dest.stat().st_size
            if size == 0:
                raise RuntimeError(f"下载为空: {url}")
            logger.info(f"Downloaded {url} -> {dest.name} ({size} bytes)")
            return dest
        except (httpx.HTTPError, OSError, RuntimeError) as e:
            last_error = e
            dest.unlink(missing_ok=True)
            if attempt >= DOWNLOAD_RETRIES:
                break
            sleep_seconds = DOWNLOAD_RETRY_SLEEP_SECONDS * attempt
            logger.warning(
                f"Download failed ({type(e).__name__}: {e}), "
                f"{sleep_seconds:.1f}s 后重试 {attempt}/{DOWNLOAD_RETRIES}: {url}"
            )
            time.sleep(sleep_seconds)
    raise last_error


def suffix_from_hint(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip().strip('"').strip("'")
    if not value:
        return None

    parsed = urlparse(value)
    path_text = parsed.path if (parsed.scheme or parsed.netloc) else value
    path_text = unquote(path_text).split("?", 1)[0].split("#", 1)[0]
    suffix = Path(path_text).suffix.lower().lstrip(".")
    return suffix if suffix in SUPPORTED_SOURCE_SUFFIXES else None


def filename_hints_from_task(task: dict) -> list[str]:
    hints = []
    for field in FILENAME_HINT_FIELDS:
        value = task.get(field)
        if isinstance(value, str):
            hints.append(value)
    report_url = task.get("reportUrl")
    if isinstance(report_url, str):
        hints.append(report_url)
    return hints


def detect_file_type(path: Path, filename_hints: list[str] | None = None) -> str:
    hinted_suffixes = [
        suffix
        for suffix in (suffix_from_hint(hint) for hint in filename_hints or [])
        if suffix
    ]

    with open(path, "rb") as f:
        head = f.read(8)
    if head.startswith(PDF_MAGIC):
        return "pdf"
    if head.startswith(OLE_MAGIC):
        for suffix in hinted_suffixes:
            if suffix in OLE_SUFFIXES:
                return suffix
        return "ole"
    if head.startswith(ZIP_MAGIC):
        try:
            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
        except zipfile.BadZipFile:
            return "unknown"
        if "word/document.xml" in names:
            return "docx"
        if any(n.startswith("xl/") for n in names):
            return "xlsx"
        if any(n.startswith("ppt/") for n in names):
            return "pptx"
        return "unknown"
    return "unknown"


def docx_has_embedded_images(path: Path) -> bool:
    """docx 是 zip,图片资源存放在 word/media/ 下。只要该目录有任意文件即视为含图。"""
    try:
        with zipfile.ZipFile(path) as zf:
            for name in zf.namelist():
                if name.startswith("word/media/") and not name.endswith("/"):
                    return True
    except zipfile.BadZipFile:
        return True  # 坏 zip 走保守路径(PDF 转换)
    return False


# ========== 格式转换 ==========
def libreoffice_convert(src: Path, out_dir: Path, target: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    lo_profile = (out_dir.parent / f"lo-{os.getpid()}").resolve()
    lo_profile.mkdir(parents=True, exist_ok=True)
    cmd = [
        LIBREOFFICE_BIN, "--headless",
        f"-env:UserInstallation={lo_profile.as_uri()}",
        "--convert-to", target,
        "--outdir", str(out_dir),
        str(src),
    ]
    logger.info(f"LibreOffice {src.name} -> {target}")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(
            f"LibreOffice convert 失败 (exit={proc.returncode}): {proc.stderr.strip()}"
        )
    candidates = list(out_dir.glob(f"*.{target}"))
    if not candidates:
        raise RuntimeError(f"LibreOffice 没产出 .{target}: {proc.stdout.strip()}")
    return candidates[0]


def prepare_for_parse(
    downloaded: Path, ftype: str, workdir: Path, ts: str,
) -> tuple[Path, str, Path, Path | None]:
    """返回 (parse_file, final_type, original_file, converted_pdf_or_None)

    对纯文本 docx(无嵌入图片)跳过 LibreOffice→PDF 的重环节,直接走 MinerU 的 native
    office 后端(`office_docx_analyze`),解析时间能从分钟级降到秒级。
    """
    if ftype == "pdf":
        f = downloaded.rename(workdir / f"{ts}.pdf")
        return f, "pdf", f, None
    if ftype == "docx":
        original = downloaded.rename(workdir / f"{ts}.docx")
        if docx_has_embedded_images(original):
            logger.info(f"{original.name}: 含嵌入图片,转 PDF 走 hybrid 解析")
            pdf = libreoffice_convert(original, workdir / "converted", "pdf")
            return pdf, "pdf", original, pdf
        logger.info(f"{original.name}: 无图片,直接走 office 后端")
        return original, "docx", original, None
    if ftype == "doc":
        original = downloaded.rename(workdir / f"{ts}.doc")
        pdf = libreoffice_convert(original, workdir / "converted", "pdf")
        return pdf, "pdf", original, pdf
    if ftype == "pptx":
        f = downloaded.rename(workdir / f"{ts}.pptx")
        return f, "pptx", f, None
    if ftype == "ppt":
        original = downloaded.rename(workdir / f"{ts}.ppt")
        pdf = libreoffice_convert(original, workdir / "converted", "pdf")
        return pdf, "pdf", original, pdf
    if ftype == "xlsx":
        f = downloaded.rename(workdir / f"{ts}.xlsx")
        return f, "xlsx", f, None
    if ftype == "xls":
        original = downloaded.rename(workdir / f"{ts}.xls")
        xlsx = libreoffice_convert(original, workdir / "converted", "xlsx")
        return xlsx, "xlsx", original, None
    raise RuntimeError(f"不支持的文件类型: {ftype}")


# ========== 解析(直接调 do_parse,主线程,无线程池) ==========
def parse_local(source_file: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    source_bytes = source_file.read_bytes()
    do_parse(
        output_dir=str(output_dir),
        pdf_file_names=[source_file.stem],
        pdf_bytes_list=[source_bytes],
        p_lang_list=[""],
        backend=MINERU_BACKEND,
        parse_method="auto",
        formula_enable=True,
        table_enable=True,
        f_draw_layout_bbox=True,
        f_draw_span_bbox=False,
        f_dump_md=True,
        f_dump_middle_json=False,
        f_dump_model_output=False,
        f_dump_orig_pdf=False,
        f_dump_content_list=True,
    )
    return output_dir


# ========== S3 上传 ==========
import re as _re


def _rewrite_md_image_urls(content: str, md_s3_dir: str) -> str:
    """把 markdown 里的相对图片路径替换为完整 S3 URL."""
    base = md_s3_dir.rstrip("/")

    def _replace(m: _re.Match) -> str:
        alt, path = m.group(1), m.group(2)
        if path.startswith("http://") or path.startswith("https://"):
            return m.group(0)
        full = base + "/" + path.lstrip("./")
        return f"![{alt}]({full})"

    return _re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", _replace, content)


def s3_url(key: str) -> str:
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"


def s3_upload(s3, local_path: str, key: str) -> str:
    s3.upload_file(local_path, S3_BUCKET, key, ExtraArgs={"ACL": "public-read"})
    return s3_url(key)


def upload_result(
    output_dir: Path,
    research_id: str,
    converted_pdf: Path | None = None,
) -> dict:
    """把解析产物扁平上传到 s3://{bucket}/{S3_PREFIX}/{research_id}/。

    顶层文件名前面会加 "{research_id}_" 前缀,方便单文件识别所属 research;
    images/ 子目录下的图片保持原名,以免破坏 markdown 里的相对图片链接。
    """
    s3 = make_s3()
    key_prefix = f"{S3_PREFIX}/{research_id}"
    uploaded: dict[str, str] = {}

    def _key_for(rel: Path) -> str:
        if rel.parent == Path("."):
            name = rel.name
            if not name.startswith(f"{research_id}_"):
                name = f"{research_id}_{name}"
            return f"{key_prefix}/{name}"
        return f"{key_prefix}/{rel}"

    # Word 转来的 PDF（原始文件已有 reportUrl，无需重复上传）
    if converted_pdf is not None:
        conv_key = _key_for(Path(converted_pdf.name))
        uploaded["converted_pdf"] = s3_upload(s3, str(converted_pdf), conv_key)

    # do_parse 产物结构: output_dir/<stem>/<method>/{files, images/}
    # 以含 .md 的那层目录为根，扁平上传到 key_prefix/
    all_files = [p for p in output_dir.rglob("*") if p.is_file()]
    md_files = [p for p in all_files if p.suffix == ".md"]
    content_root = md_files[0].parent if md_files else output_dir

    # 先上传非 .md 文件
    for p in all_files:
        if p.suffix == ".md":
            continue
        rel = p.relative_to(content_root)
        key = _key_for(rel)
        url = s3_upload(s3, str(p), key)
        # 注意 v2 检查必须放在 v1 前面,因为 v2 的文件名 xxx_content_list_v2.json
        # 也以 _content_list_v2.json 收尾,防御性匹配
        if p.name.endswith("_content_list_v2.json"):
            uploaded["content_list_v2_json"] = url
        elif p.name.endswith("_content_list.json"):
            uploaded["content_list_json"] = url
        elif p.name.endswith("_layout.pdf"):
            uploaded["layout_pdf"] = url

    # 上传 markdown：把相对图片路径替换为完整 S3 URL
    images_base = s3_url(f"{key_prefix}")
    for md_path in md_files:
        content = md_path.read_text(encoding="utf-8")
        content = _rewrite_md_image_urls(content, images_base)
        md_path.write_text(content, encoding="utf-8")
        rel = md_path.relative_to(content_root)
        key = _key_for(rel)
        url = s3_upload(s3, str(md_path), key)
        uploaded["markdown"] = url

    uploaded["images_prefix"] = s3_url(f"{key_prefix}/")
    logger.info(f"Uploaded to s3://{S3_BUCKET}/{key_prefix}/")
    return uploaded


# ========== 主流程(同步) ==========
def process_one(task: dict) -> None:
    record_id = task["_id"]
    research_id = str(task["researchId"])
    report_url = task.get("reportUrl")
    label = research_id

    logger.info(f"▶ {label}: {report_url}")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    workdir = Path(tempfile.mkdtemp(prefix="mineru_report_"))
    try:
        downloaded = download_file(report_url, workdir / "download.bin")

        ftype = detect_file_type(downloaded, filename_hints_from_task(task))
        logger.info(f"{label}: detected type = {ftype}")
        patch(record_id, detectedFileType=ftype)

        parse_file, final_type, original_file, converted_pdf = prepare_for_parse(
            downloaded, ftype, workdir, ts,
        )
        patch(record_id, finalType=final_type, parseSubStatus=SUB_PARSING)

        output_dir = workdir / "output"
        try:
            parse_local(parse_file, output_dir)
        except Exception as native_err:
            # MinerU native docx 解析器对某些列表样式会 IndexError。
            # 只要走的是 native docx 路径,失败就回退 LibreOffice→PDF 重试一次。
            if final_type != "docx":
                raise
            logger.warning(
                f"{label}: native docx 解析失败({type(native_err).__name__}: {native_err}),"
                f"回退到 LibreOffice→PDF"
            )
            shutil.rmtree(output_dir, ignore_errors=True)
            converted_pdf = libreoffice_convert(original_file, workdir / "converted", "pdf")
            parse_file = converted_pdf
            final_type = "pdf"
            patch(record_id, finalType=final_type)
            parse_local(parse_file, output_dir)
        patch(record_id, parseSubStatus=SUB_UPLOADING)

        s3_keys = upload_result(output_dir, research_id, converted_pdf)

        patch(
            record_id,
            parseStatus=STATUS_COMPLETED,
            parseSubStatus=None,
            parsedS3Bucket=S3_BUCKET,
            convertedPdfS3=s3_keys.get("converted_pdf"),
            parsedMarkdownS3=s3_keys.get("markdown"),
            parsedContentListS3=s3_keys.get("content_list_json"),
            parsedContentListV2S3=s3_keys.get("content_list_v2_json"),
            parsedLayoutPdfS3=s3_keys.get("layout_pdf"),
            parsedImagesS3Prefix=s3_keys.get("images_prefix"),
            parsedByWorker=WORKER_ID,
            parseCompletedAt=_now(),
            parseLockedBy=None,
            parseLockedUntil=None,
            parseErrorMessage=None,
        )
        logger.success(f"✅ {label} done")
    except Exception as e:
        logger.exception(f"❌ {label} failed: {e}")
        error_message = str(e)[:2000]

        def _mark_failed(coll):
            current = coll.find_one({"_id": record_id}, {"parseRetryCount": 1}) or {}
            next_retry = (current.get("parseRetryCount") or 0) + 1
            is_dead = next_retry >= MAX_RETRIES
            return coll.update_one(
                {"_id": record_id},
                {
                    "$set": {
                        "parseStatus": STATUS_FAILED if is_dead else STATUS_PENDING,
                        "parseSubStatus": None,
                        "parseErrorMessage": error_message,
                        "parseUpdatedAt": _now(),
                        "parseLockedBy": None,
                        "parseLockedUntil": None,
                    },
                    "$inc": {"parseRetryCount": 1},
                },
            )

        try:
            with_mongo_retry("回写失败状态", _mark_failed)
        except Exception as mark_err:
            logger.error(
                f"{label}: 回写失败状态也失败({type(mark_err).__name__}: {mark_err}); "
                f"任务会在锁超时后重新被领取"
            )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main_loop() -> None:
    ensure_indexes()
    logger.info(f"Worker {WORKER_ID} started (coll={COLL_NAME}, backend={MINERU_BACKEND})")
    while True:
        processed = 0
        for i in range(BATCH_SIZE):
            try:
                task = claim_task()
            except Exception as e:
                logger.exception(f"领取任务失败, {POLL_IDLE_SECONDS}s 后重试: {e}")
                break
            if not task:
                break
            if i > 0 and INTER_TASK_SLEEP_SECONDS > 0:
                logger.debug(f"Sleep {INTER_TASK_SLEEP_SECONDS}s before next task")
                time.sleep(INTER_TASK_SLEEP_SECONDS)
            process_one(task)
            processed += 1
        if processed == 0:
            time.sleep(POLL_IDLE_SECONDS)
        else:
            logger.info(f"Batch done: {processed}")


if __name__ == "__main__":
    main_loop()
