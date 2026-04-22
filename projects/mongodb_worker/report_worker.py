"""研报解析 Worker.

数据源 + 状态表: 单集合 `ResearchReportRecord`
产物: AWS S3

状态机 (parseStatus):
  pending    → 待处理 (上游/人工初始化此值)
  processing → 处理中 (worker 持锁,parseSubStatus 细分 downloading/parsing/uploading)
  completed  → 已完成 (不会再被 worker 扫到)
  failed     → 超过重试上限,需人工 (不会再被 worker 扫到)

抢任务: parseStatus=pending 或 (parseStatus=processing 且锁超时且重试未超限)
"""
import asyncio
import os
import shutil
import socket
import subprocess
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import httpx
from loguru import logger
from pymongo import MongoClient, ReturnDocument

from mineru.cli import api_client as _api_client


# ========== 环境变量 ==========
MONGODB_DATABASE_URL = os.environ["MONGODB_DATABASE_URL"]
MONGODB_DB = os.environ.get("MONGODB_DB")
COLL_NAME = os.environ.get("MONGODB_COLL", "ResearchReportRecord")

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.environ["AWS_S3_BUCKET_NAME"]
S3_PREFIX = os.environ.get("AWS_S3_PREFIX", "research-reports/parsed")

MINERU_API_URL = os.environ.get("MINERU_API_URL", "http://127.0.0.1:8000")
MINERU_BACKEND = os.environ.get("MINERU_BACKEND", "hybrid-auto-engine")

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))
LOCK_TTL_SECONDS = int(os.environ.get("LOCK_TTL_SECONDS", "3600"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
POLL_IDLE_SECONDS = int(os.environ.get("POLL_IDLE_SECONDS", "30"))
LIBREOFFICE_BIN = os.environ.get("LIBREOFFICE_BIN", "soffice")


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


def get_coll():
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(MONGODB_DATABASE_URL)
    db = _mongo[MONGODB_DB] if MONGODB_DB else _mongo.get_default_database()
    if db is None:
        raise RuntimeError("MONGODB_DATABASE_URL 没带默认 DB,且未设 MONGODB_DB")
    return db[COLL_NAME]


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
    get_coll().create_index([("parseStatus", 1), ("parseLockedUntil", 1)])


def claim_task() -> dict | None:
    """原子抢一个可处理的记录。"""
    now = _now()
    lock_until = now + timedelta(seconds=LOCK_TTL_SECONDS)
    return get_coll().find_one_and_update(
        {
            "reportUrl": {"$exists": True, "$ne": None, "$ne": ""},
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
        sort=[("createTime", 1)],
    )


def patch(record_id, **fields) -> None:
    fields.setdefault("parseUpdatedAt", _now())
    get_coll().update_one({"_id": record_id}, {"$set": fields})


# ========== 下载 + 类型识别 ==========
PDF_MAGIC = b"%PDF-"
ZIP_MAGIC = b"PK\x03\x04"
OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"  # 老版 .doc/.xls OLE2


def download_file(url: str, dest: Path) -> Path:
    if not url:
        raise ValueError("reportUrl 为空")
    with httpx.stream("GET", url, timeout=300, follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)
    size = dest.stat().st_size
    if size == 0:
        raise RuntimeError(f"下载为空: {url}")
    logger.info(f"Downloaded {url} -> {dest.name} ({size} bytes)")
    return dest


def detect_file_type(path: Path) -> str:
    """magic bytes + zip 内部指纹识别真实类型,不依赖 URL 后缀。

    返回: 'pdf' | 'docx' | 'doc' | 'xlsx' | 'xls' | 'pptx' | 'unknown'
    """
    with open(path, "rb") as f:
        head = f.read(8)
    if head.startswith(PDF_MAGIC):
        return "pdf"
    if head.startswith(OLE_MAGIC):
        suf = path.suffix.lower().lstrip(".")
        return suf if suf in ("doc", "xls", "ppt") else "ole"
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


# ========== 格式转换 (Word/老 Excel → PDF/XLSX) ==========
def libreoffice_convert(src: Path, out_dir: Path, target: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        LIBREOFFICE_BIN, "--headless",
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
        raise RuntimeError(
            f"LibreOffice 没产出 .{target}: stdout={proc.stdout.strip()}"
        )
    return candidates[0]


def prepare_for_parse(downloaded: Path, ftype: str, workdir: Path) -> tuple[Path, str]:
    """把下载下来的文件整理成 mineru 可直接解析的形式。"""
    if ftype == "pdf":
        return downloaded.rename(workdir / "source.pdf"), "pdf"
    if ftype in ("docx", "doc"):
        renamed = downloaded.rename(workdir / f"source.{ftype}")
        pdf = libreoffice_convert(renamed, workdir / "converted", "pdf")
        return pdf, "pdf"
    if ftype == "xlsx":
        return downloaded.rename(workdir / "source.xlsx"), "xlsx"
    if ftype == "xls":
        renamed = downloaded.rename(workdir / "source.xls")
        xlsx = libreoffice_convert(renamed, workdir / "converted", "xlsx")
        return xlsx, "xlsx"
    raise RuntimeError(f"不支持的文件类型: {ftype}")


# ========== 调 mineru-api ==========
async def parse_via_mineru(source_file: Path, workdir: Path) -> Path:
    form_data = _api_client.build_parse_request_form_data(
        lang_list=[""],
        backend=MINERU_BACKEND,
        parse_method="auto",
        formula_enable=True,
        table_enable=True,
        server_url=None,
        start_page_id=0,
        end_page_id=None,
        return_md=True,
        return_middle_json=False,
        return_model_output=False,
        return_content_list=True,
        return_images=True,
        response_format_zip=True,
        return_original_file=False,
    )
    assets = [_api_client.UploadAsset(path=source_file, upload_name=source_file.name)]

    async with httpx.AsyncClient(
        timeout=_api_client.build_http_timeout(),
        follow_redirects=True,
    ) as client:
        base_url = _api_client.normalize_base_url(MINERU_API_URL)
        submit_resp = await _api_client.submit_parse_task(
            base_url=base_url,
            upload_assets=assets,
            form_data=form_data,
        )
        logger.info(f"mineru task submitted: {submit_resp.task_id}")
        await _api_client.wait_for_task_result(
            client=client,
            submit_response=submit_resp,
            task_label=source_file.stem,
        )
        zip_path = await _api_client.download_result_zip(
            client=client,
            submit_response=submit_resp,
            task_label=source_file.stem,
        )

    result_dir = workdir / "result"
    result_dir.mkdir(exist_ok=True)
    _api_client.safe_extract_zip(zip_path, result_dir)
    zip_path.unlink(missing_ok=True)
    return result_dir


# ========== S3 上传 ==========
def upload_result(result_dir: Path, source_file: Path, key_prefix: str) -> dict:
    s3 = make_s3()
    uploaded: dict[str, str] = {}

    src_key = f"{key_prefix}/source{source_file.suffix}"
    s3.upload_file(str(source_file), S3_BUCKET, src_key)
    uploaded["source"] = src_key

    for p in result_dir.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(result_dir)
        key = f"{key_prefix}/{rel}"
        s3.upload_file(str(p), S3_BUCKET, key)
        name = p.name
        if name.endswith(".md"):
            uploaded["markdown"] = key
        elif name.endswith("_content_list.json"):
            uploaded["content_list_json"] = key
        elif name.endswith("_layout.pdf"):
            uploaded["layout_pdf"] = key

    uploaded["images_prefix"] = f"{key_prefix}/images/"
    logger.info(f"Uploaded to s3://{S3_BUCKET}/{key_prefix}/")
    return uploaded


# ========== 主流程 ==========
async def process_one(task: dict) -> None:
    record_id = task["_id"]
    research_id = task.get("researchId")
    report_url = task.get("reportUrl")
    label = research_id or str(record_id)

    logger.info(f"▶ {label}: {report_url}")
    workdir = Path(tempfile.mkdtemp(prefix="mineru_report_"))
    try:
        downloaded = download_file(report_url, workdir / "download.bin")

        ftype = detect_file_type(downloaded)
        logger.info(f"{label}: detected type = {ftype}")
        patch(record_id, detectedFileType=ftype)

        source_file, final_type = prepare_for_parse(downloaded, ftype, workdir)
        patch(record_id, finalType=final_type, parseSubStatus=SUB_PARSING)

        result_dir = await parse_via_mineru(source_file, workdir)
        patch(record_id, parseSubStatus=SUB_UPLOADING)

        key_prefix = f"{S3_PREFIX}/{research_id or str(record_id)}"
        s3_keys = upload_result(result_dir, source_file, key_prefix)

        patch(
            record_id,
            parseStatus=STATUS_COMPLETED,
            parseSubStatus=None,
            parsedS3Bucket=S3_BUCKET,
            parsedSourceS3=s3_keys.get("source"),
            parsedMarkdownS3=s3_keys.get("markdown"),
            parsedContentListS3=s3_keys.get("content_list_json"),
            parsedLayoutPdfS3=s3_keys.get("layout_pdf"),
            parsedImagesS3Prefix=s3_keys.get("images_prefix"),
            parseCompletedAt=_now(),
            parseLockedBy=None,
            parseLockedUntil=None,
            parseErrorMessage=None,
        )
        logger.success(f"✅ {label} done")
    except Exception as e:
        logger.exception(f"❌ {label} failed: {e}")
        # 先拉现状看看重试次数是否超限
        current = get_coll().find_one(
            {"_id": record_id},
            {"parseRetryCount": 1},
        ) or {}
        next_retry = (current.get("parseRetryCount") or 0) + 1
        is_dead = next_retry >= MAX_RETRIES

        get_coll().update_one(
            {"_id": record_id},
            {
                "$set": {
                    "parseStatus": STATUS_FAILED if is_dead else STATUS_PENDING,
                    "parseSubStatus": None,
                    "parseErrorMessage": str(e)[:2000],
                    "parseUpdatedAt": _now(),
                    "parseLockedBy": None,
                    "parseLockedUntil": None,
                },
                "$inc": {"parseRetryCount": 1},
            },
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def main_loop() -> None:
    ensure_indexes()
    logger.info(
        f"Worker {WORKER_ID} started "
        f"(coll={COLL_NAME}, batch={BATCH_SIZE}, backend={MINERU_BACKEND})"
    )
    while True:
        processed = 0
        for _ in range(BATCH_SIZE):
            task = claim_task()
            if not task:
                break
            await process_one(task)
            processed += 1
        if processed == 0:
            await asyncio.sleep(POLL_IDLE_SECONDS)
        else:
            logger.info(f"Batch done: {processed}")


if __name__ == "__main__":
    asyncio.run(main_loop())
