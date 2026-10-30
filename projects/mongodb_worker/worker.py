# Copyright (c) Your Team.
"""MinerU 定时解析 Worker

从 MongoDB 拉取待解析文件 → 下载 → 送 mineru-api → 上传 S3 → 更新 MongoDB。
"""
import asyncio
import os
import shutil
import socket
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from mineru.data.io.node_s3 import SignedS3Client, upload_tree, download_source_file
import httpx
from loguru import logger
from pymongo import MongoClient, ReturnDocument

from mineru.cli import api_client as _api_client


# ========== 配置(从环境变量读) ==========
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ.get("MONGO_DB", "mineru")
MONGO_COLL = os.environ.get("MONGO_COLL", "documents")

MINERU_API_URL = os.environ.get("MINERU_API_URL", "http://127.0.0.1:8000")
MINERU_BACKEND = os.environ.get("MINERU_BACKEND", "hybrid-auto-engine")

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "mineru-parsed")

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))
LOCK_TTL_SECONDS = int(os.environ.get("LOCK_TTL_SECONDS", "3600"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))


# ========== 客户端初始化 ==========
def make_mongo():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][MONGO_COLL]


def make_s3():
    return SignedS3Client()


# ========== 核心逻辑 ==========
def claim_one_task(coll) -> dict | None:
    """原子地抢一个 pending 任务。"""
    now = datetime.now(timezone.utc)
    return coll.find_one_and_update(
        {
            "$or": [
                {"status": "pending"},
                {
                    "status": {"$in": ["downloading", "parsing", "uploading"]},
                    "locked_until": {"$lt": now},  # 锁过期的可抢占
                },
            ],
            "retry_count": {"$lt": MAX_RETRIES},
        },
        {
            "$set": {
                "status": "downloading",
                "locked_by": WORKER_ID,
                "locked_until": now + timedelta(seconds=LOCK_TTL_SECONDS),
                "updated_at": now,
            },
        },
        return_document=ReturnDocument.AFTER,
        sort=[("created_at", 1)],
    )


def update_status(coll, doc_id, status: str, **fields):
    coll.update_one(
        {"_id": doc_id},
        {"$set": {"status": status, "updated_at": datetime.now(timezone.utc), **fields}},
    )


def download_source(url: str, dest: Path) -> Path:
    """从 HTTP/HTTPS/S3 URL 下载文件到本地。"""
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix or ".bin"
    local_path = dest / f"source{suffix}"

    if parsed.scheme in ("http", "https"):
        download_source_file(url, local_path)
    elif parsed.scheme == "s3":
        # s3://bucket/key 格式
        s3 = make_s3()
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        s3.download_file(bucket, key, str(local_path))
    else:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")

    logger.info(f"Downloaded {url} -> {local_path} ({local_path.stat().st_size} bytes)")
    return local_path


async def parse_via_mineru(source_file: Path, workdir: Path) -> Path:
    """调用 mineru-api 解析,返回解压后的结果目录。"""
    form_data = _api_client.build_parse_request_form_data(
        lang_list=[""],  # 空字符串 = 自动检测语言
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
    upload_assets = [
        _api_client.UploadAsset(path=source_file, upload_name=source_file.name)
    ]

    async with httpx.AsyncClient(
        timeout=_api_client.build_http_timeout(),
        follow_redirects=True,
    ) as client:
        base_url = _api_client.normalize_base_url(MINERU_API_URL)
        submit_resp = await _api_client.submit_parse_task(
            base_url=base_url,
            upload_assets=upload_assets,
            form_data=form_data,
        )
        logger.info(f"Submitted task: {submit_resp.task_id}")

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


def upload_to_s3(result_dir: Path, doc_id: str) -> dict:
    client = make_s3()
    prefix = f"{S3_PREFIX}/{doc_id}"
    results = upload_tree(client, result_dir, prefix, S3_BUCKET)
    uploaded = {"images_prefix": f"images/mineru/{prefix}/images/", "s3_bucket": client.bucket}
    for name, result in results.items():
        if name.endswith('.md'): uploaded['markdown'] = result['key']
        elif name.endswith('_content_list.json'): uploaded['content_list_json'] = result['key']
        elif name.endswith('_layout.pdf'): uploaded['layout_pdf'] = result['key']
    return uploaded


async def process_one(coll, task: dict) -> None:
    doc_id = task["_id"]
    source_url = task["source_url"]
    logger.info(f"▶ Processing {doc_id}: {source_url}")

    workdir = Path(tempfile.mkdtemp(prefix="mineru_worker_"))
    try:
        # 1. 下载
        source_file = download_source(source_url, workdir)

        # 2. 解析
        update_status(coll, doc_id, "parsing")
        result_dir = await parse_via_mineru(source_file, workdir)

        # 3. 上传
        update_status(coll, doc_id, "uploading")
        s3_keys = upload_to_s3(result_dir, str(doc_id))

        # 4. 完成
        update_status(
            coll, doc_id, "completed",
            parsed_markdown_s3=s3_keys.get("markdown"),
            parsed_json_s3=s3_keys.get("content_list_json"),
            parsed_images_s3_prefix=s3_keys.get("images_prefix"),
            locked_by=None,
            locked_until=None,
            error_message=None,
        )
        logger.success(f"✅ Completed {doc_id}")

    except Exception as e:
        logger.exception(f"❌ Failed {doc_id}: {e}")
        coll.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "status": "failed",
                    "error_message": str(e),
                    "updated_at": datetime.now(timezone.utc),
                    "locked_by": None,
                    "locked_until": None,
                },
                "$inc": {"retry_count": 1},
            },
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def main_loop():
    coll = make_mongo()
    logger.info(f"Worker {WORKER_ID} started, polling MongoDB...")

    while True:
        processed = 0
        for _ in range(BATCH_SIZE):
            task = claim_one_task(coll)
            if not task:
                break
            await process_one(coll, task)
            processed += 1

        if processed == 0:
            logger.debug("No pending tasks, sleeping 30s")
            await asyncio.sleep(30)
        else:
            logger.info(f"Batch done: {processed} files")


if __name__ == "__main__":
    asyncio.run(main_loop())
