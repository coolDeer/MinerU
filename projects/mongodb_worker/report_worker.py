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

import boto3
import httpx
from loguru import logger
from pymongo import MongoClient, ReturnDocument

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
    try:
        get_coll().create_index([("parseStatus", 1), ("parseLockedUntil", 1)])
    except Exception as e:
        logger.warning(f"建索引失败(权限不足?),Worker 仍可运行: {e}")


def claim_task() -> dict | None:
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
OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


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


# ========== 格式转换 ==========
def libreoffice_convert(src: Path, out_dir: Path, target: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        LIBREOFFICE_BIN, "--headless",
        f"-env:UserInstallation=file:///tmp/lo-{os.getpid()}",
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
    """返回 (parse_file, final_type, original_file, converted_pdf_or_None)"""
    if ftype == "pdf":
        f = downloaded.rename(workdir / f"source_{ts}.pdf")
        return f, "pdf", f, None
    if ftype in ("docx", "doc"):
        original = downloaded.rename(workdir / f"source_{ts}.{ftype}")
        pdf = libreoffice_convert(original, workdir / "converted", "pdf")
        return pdf, "pdf", original, pdf
    if ftype == "xlsx":
        f = downloaded.rename(workdir / f"source_{ts}.xlsx")
        return f, "xlsx", f, None
    if ftype == "xls":
        original = downloaded.rename(workdir / f"source_{ts}.xls")
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
    key_prefix: str,
    converted_pdf: Path | None = None,
) -> dict:
    s3 = make_s3()
    uploaded: dict[str, str] = {}

    # Word 转来的 PDF（原始文件已有 reportUrl，无需重复上传）
    if converted_pdf is not None:
        conv_key = f"{key_prefix}/{converted_pdf.name}"
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
        key = f"{key_prefix}/{rel}"
        url = s3_upload(s3, str(p), key)
        if p.name.endswith("_content_list.json"):
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
        key = f"{key_prefix}/{rel}"
        url = s3_upload(s3, str(md_path), key)
        uploaded["markdown"] = url

    uploaded["images_prefix"] = s3_url(f"{key_prefix}/")
    logger.info(f"Uploaded to s3://{S3_BUCKET}/{key_prefix}/")
    return uploaded


# ========== 主流程(同步) ==========
def process_one(task: dict) -> None:
    record_id = task["_id"]
    research_id = task.get("researchId")
    report_url = task.get("reportUrl")
    label = research_id or str(record_id)

    logger.info(f"▶ {label}: {report_url}")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    workdir = Path(tempfile.mkdtemp(prefix="mineru_report_"))
    try:
        downloaded = download_file(report_url, workdir / "download.bin")

        ftype = detect_file_type(downloaded)
        logger.info(f"{label}: detected type = {ftype}")
        patch(record_id, detectedFileType=ftype)

        parse_file, final_type, original_file, converted_pdf = prepare_for_parse(
            downloaded, ftype, workdir, ts,
        )
        patch(record_id, finalType=final_type, parseSubStatus=SUB_PARSING)

        output_dir = workdir / "output"
        parse_local(parse_file, output_dir)
        patch(record_id, parseSubStatus=SUB_UPLOADING)

        folder = f"{research_id}/{record_id}" if research_id else str(record_id)
        key_prefix = f"{S3_PREFIX}/{folder}"
        s3_keys = upload_result(output_dir, key_prefix, converted_pdf)

        patch(
            record_id,
            parseStatus=STATUS_COMPLETED,
            parseSubStatus=None,
            parsedS3Bucket=S3_BUCKET,
            convertedPdfS3=s3_keys.get("converted_pdf"),
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
        current = get_coll().find_one({"_id": record_id}, {"parseRetryCount": 1}) or {}
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


def main_loop() -> None:
    ensure_indexes()
    logger.info(f"Worker {WORKER_ID} started (coll={COLL_NAME}, backend={MINERU_BACKEND})")
    while True:
        processed = 0
        for _ in range(BATCH_SIZE):
            task = claim_task()
            if not task:
                break
            process_one(task)
            processed += 1
        if processed == 0:
            time.sleep(POLL_IDLE_SECONDS)
        else:
            logger.info(f"Batch done: {processed}")


if __name__ == "__main__":
    main_loop()
