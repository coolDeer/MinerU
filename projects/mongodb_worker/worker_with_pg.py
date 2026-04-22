# Copyright (c) Your Team.
"""MinerU 任务 Worker(增强版:PostgreSQL + pgvector + S3)

任务队列:MongoDB
解析结果:
  - 原始文件/Markdown/图片 → S3
  - 元数据/chunks/向量 → PostgreSQL
  - 任务状态 → MongoDB
"""
import asyncio
import json
import os
import shutil
import socket
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
import httpx
import psycopg
from loguru import logger
from pgvector.psycopg import register_vector
from pymongo import MongoClient, ReturnDocument
from sentence_transformers import SentenceTransformer

from mineru.cli import api_client as _api_client


# ========== 配置 ==========
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB = os.environ.get("MONGO_DB", "mineru")
MONGO_COLL = os.environ.get("MONGO_COLL", "documents")

PG_DSN = os.environ["PG_DSN"]  # "postgresql://user:pass@host/db"

MINERU_API_URL = os.environ.get("MINERU_API_URL", "http://127.0.0.1:8000")
MINERU_BACKEND = os.environ.get("MINERU_BACKEND", "hybrid-auto-engine")

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "mineru")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]

EMBED_MODEL_NAME = os.environ.get("EMBED_MODEL", "BAAI/bge-m3")
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"


# ========== 单例资源 ==========
_embed_model = None
def get_embedder() -> SentenceTransformer:
    global _embed_model
    if _embed_model is None:
        logger.info(f"Loading embedder: {EMBED_MODEL_NAME}")
        _embed_model = SentenceTransformer(EMBED_MODEL_NAME)
    return _embed_model


def make_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
    )


def make_mongo():
    return MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLL]


def make_pg():
    conn = psycopg.connect(PG_DSN, autocommit=False)
    register_vector(conn)
    return conn


# ========== Chunk 切分 ==========
def blocks_to_chunks(blocks: list[dict], doc_id: str) -> list[dict]:
    chunks = []
    buffer = []
    buffer_page = 0
    buffer_size = 0
    section_stack: list[str] = []

    def flush():
        nonlocal buffer, buffer_size
        if buffer:
            chunks.append({
                "chunk_index": len(chunks),
                "chunk_type": "text",
                "text": "\n".join(buffer),
                "page_idx": buffer_page,
                "section_path": " > ".join(section_stack),
                "title_level": None,
                "extra": {},
            })
            buffer = []
            buffer_size = 0

    for b in blocks:
        page = b.get("page_idx", 0)
        btype = b["type"]
        if btype == "title":
            flush()
            level = b.get("text_level", 1)
            section_stack = section_stack[: max(0, level - 1)]
            section_stack.append(b["text"])
            chunks.append({
                "chunk_index": len(chunks),
                "chunk_type": "title",
                "text": b["text"],
                "page_idx": page,
                "section_path": " > ".join(section_stack),
                "title_level": level,
                "extra": {},
            })
        elif btype == "text":
            text = b["text"]
            if buffer_size + len(text) > 800:
                flush()
            if not buffer:
                buffer_page = page
            buffer.append(text)
            buffer_size += len(text)
        elif btype == "table":
            flush()
            chunks.append({
                "chunk_index": len(chunks),
                "chunk_type": "table",
                "text": " ".join(b.get("table_caption", [])) or "[table]",
                "page_idx": page,
                "section_path": " > ".join(section_stack),
                "title_level": None,
                "extra": {"html": b.get("table_body", ""), "caption": b.get("table_caption", [])},
            })
        elif btype == "equation":
            flush()
            chunks.append({
                "chunk_index": len(chunks),
                "chunk_type": "equation",
                "text": b.get("text", ""),
                "page_idx": page,
                "section_path": " > ".join(section_stack),
                "title_level": None,
                "extra": {"latex": b.get("text", "")},
            })
        elif btype == "image":
            caption = " ".join(b.get("img_caption", []))
            if caption:
                flush()
                chunks.append({
                    "chunk_index": len(chunks),
                    "chunk_type": "image",
                    "text": caption,
                    "page_idx": page,
                    "section_path": " > ".join(section_stack),
                    "title_level": None,
                    "extra": {"img_path": b.get("img_path", ""), "caption": caption},
                })
    flush()
    return chunks


# ========== 主流程 ==========
async def parse_via_mineru(source_file: Path, workdir: Path) -> Path:
    form_data = _api_client.build_parse_request_form_data(
        lang_list=[""], backend=MINERU_BACKEND, parse_method="auto",
        formula_enable=True, table_enable=True, server_url=None,
        start_page_id=0, end_page_id=None,
        return_md=True, return_middle_json=False, return_model_output=False,
        return_content_list=True, return_images=True,
        response_format_zip=True, return_original_file=False,
    )
    upload_assets = [_api_client.UploadAsset(path=source_file, upload_name=source_file.name)]

    async with httpx.AsyncClient(
        timeout=_api_client.build_http_timeout(), follow_redirects=True,
    ) as client:
        base_url = _api_client.normalize_base_url(MINERU_API_URL)
        submit_resp = await _api_client.submit_parse_task(
            base_url=base_url, upload_assets=upload_assets, form_data=form_data,
        )
        await _api_client.wait_for_task_result(
            client=client, submit_response=submit_resp, task_label=source_file.stem,
        )
        zip_path = await _api_client.download_result_zip(
            client=client, submit_response=submit_resp, task_label=source_file.stem,
        )
    result_dir = workdir / "result"
    result_dir.mkdir(exist_ok=True)
    _api_client.safe_extract_zip(zip_path, result_dir)
    zip_path.unlink(missing_ok=True)
    return result_dir


def upload_assets_to_s3(result_dir: Path, doc_id: str, source_file: Path) -> dict:
    s3 = make_s3()
    prefix = f"{S3_PREFIX}/docs/{doc_id}"

    # 上传原文件
    source_key = f"{prefix}/source{source_file.suffix}"
    s3.upload_file(str(source_file), S3_BUCKET, source_key)

    # 上传解析产物
    uploaded = {"source": source_key, "images_prefix": f"{prefix}/images/"}
    for p in result_dir.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(result_dir)
        key = f"{prefix}/{rel}"
        s3.upload_file(str(p), S3_BUCKET, key)
        if p.name.endswith(".md"):
            uploaded["markdown"] = key
        elif p.name.endswith("_layout.pdf"):
            uploaded["layout_pdf"] = key

    return uploaded


def write_to_postgres(doc_id: str, source_file: Path, meta: dict,
                      chunks: list[dict], s3_info: dict) -> None:
    texts = [c["text"] for c in chunks]
    logger.info(f"Embedding {len(texts)} chunks...")
    embeddings = get_embedder().encode(texts, normalize_embeddings=True, show_progress_bar=False)

    conn = make_pg()
    try:
        with conn.cursor() as cur:
            # upsert document
            cur.execute("""
                INSERT INTO documents
                    (doc_id, title, source_url, file_type, total_pages, total_chunks,
                     s3_markdown, s3_layout_pdf, s3_images_prefix, metadata, tags,
                     parse_method, parse_version, status, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
                        %s, %s, 'completed', now())
                ON CONFLICT (doc_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    total_pages = EXCLUDED.total_pages,
                    total_chunks = EXCLUDED.total_chunks,
                    s3_markdown = EXCLUDED.s3_markdown,
                    s3_layout_pdf = EXCLUDED.s3_layout_pdf,
                    updated_at = now()
            """, (
                doc_id, meta["title"], s3_info.get("source"),
                source_file.suffix.lstrip("."),
                meta["total_pages"], len(chunks),
                s3_info.get("markdown"), s3_info.get("layout_pdf"),
                s3_info.get("images_prefix"),
                json.dumps(meta.get("extra", {})),
                meta.get("tags", []),
                MINERU_BACKEND, meta.get("parse_version", ""),
            ))

            # 清掉旧 chunks(允许重复解析覆盖)
            cur.execute("DELETE FROM chunks WHERE doc_id = %s", (doc_id,))

            # 批量插入 chunks
            args = [
                (doc_id, c["chunk_index"], c["chunk_type"], c["text"],
                 c["page_idx"], c["section_path"], c["title_level"],
                 json.dumps(c["extra"]), embeddings[i].tolist())
                for i, c in enumerate(chunks)
            ]
            cur.executemany("""
                INSERT INTO chunks
                    (doc_id, chunk_index, chunk_type, text, page_idx, section_path,
                     title_level, extra, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::vector)
            """, args)
        conn.commit()
        logger.success(f"PG: wrote {len(chunks)} chunks for {doc_id}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def extract_meta(result_dir: Path, blocks: list[dict]) -> dict:
    titles = [b for b in blocks if b["type"] == "title"]
    return {
        "title": next(
            (b["text"] for b in titles if b.get("text_level", 99) == 1),
            titles[0]["text"] if titles else "Untitled",
        ),
        "total_pages": max((b.get("page_idx", 0) for b in blocks), default=0) + 1,
        "tags": [],
        "extra": {"h1_sections": [b["text"] for b in titles if b.get("text_level") == 1]},
    }


# ========== Worker 主循环 ==========
def download_source(url: str, dest: Path) -> Path:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix or ".bin"
    local_path = dest / f"source{suffix}"
    if parsed.scheme in ("http", "https"):
        with httpx.stream("GET", url, timeout=300, follow_redirects=True) as resp:
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_bytes():
                    f.write(chunk)
    elif parsed.scheme == "s3":
        s3 = make_s3()
        s3.download_file(parsed.netloc, parsed.path.lstrip("/"), str(local_path))
    else:
        raise ValueError(f"Unsupported scheme: {parsed.scheme}")
    return local_path


async def process_one(coll, task: dict) -> None:
    doc_id = str(task.get("doc_id") or task["_id"])
    source_url = task["source_url"]

    workdir = Path(tempfile.mkdtemp(prefix="mineru_worker_"))
    try:
        coll.update_one({"_id": task["_id"]}, {"$set": {"status": "processing"}})

        source_file = download_source(source_url, workdir)
        result_dir = await parse_via_mineru(source_file, workdir)

        # 读 content_list.json
        json_files = list(result_dir.rglob("*_content_list.json"))
        if not json_files:
            raise RuntimeError("No content_list.json in output")
        blocks = json.loads(json_files[0].read_text(encoding="utf-8"))

        meta = extract_meta(result_dir, blocks)
        chunks = blocks_to_chunks(blocks, doc_id)

        s3_info = upload_assets_to_s3(result_dir, doc_id, source_file)
        write_to_postgres(doc_id, source_file, meta, chunks, s3_info)

        coll.update_one({"_id": task["_id"]}, {"$set": {
            "status": "completed",
            "doc_id": doc_id,
            "completed_at": datetime.now(timezone.utc),
        }})
        logger.success(f"✅ {doc_id}: {len(chunks)} chunks indexed")
    except Exception as e:
        logger.exception(f"❌ Failed: {e}")
        coll.update_one({"_id": task["_id"]}, {"$set": {
            "status": "failed", "error_message": str(e),
        }, "$inc": {"retry_count": 1}})
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def main_loop():
    coll = make_mongo()
    logger.info(f"Worker {WORKER_ID} started, polling MongoDB...")
    get_embedder()  # 预加载

    while True:
        task = coll.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "claimed", "locked_by": WORKER_ID}},
            return_document=ReturnDocument.AFTER,
            sort=[("created_at", 1)],
        )
        if task:
            await process_one(coll, task)
        else:
            await asyncio.sleep(15)


if __name__ == "__main__":
    asyncio.run(main_loop())
