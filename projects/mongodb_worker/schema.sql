-- MinerU 知识库完整 Postgres Schema
-- Postgres 版本: 15+, pgvector: 0.7+

-- =========================================================
-- 扩展
-- =========================================================
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()


-- =========================================================
-- 表 1: documents (文档元数据)
-- =========================================================
CREATE TABLE IF NOT EXISTS documents (
    doc_id                  TEXT PRIMARY KEY,
    title                   TEXT NOT NULL,
    source_filename         TEXT NOT NULL,
    source_url              TEXT,
    file_type               TEXT NOT NULL,
    file_size_bytes         BIGINT,
    file_hash               TEXT NOT NULL,
    total_pages             INTEGER,
    total_chunks            INTEGER,
    total_images            INTEGER DEFAULT 0,
    total_tables            INTEGER DEFAULT 0,
    total_equations         INTEGER DEFAULT 0,
    language                TEXT,
    s3_source               TEXT,
    s3_markdown             TEXT,
    s3_content_list         TEXT,
    s3_layout_pdf           TEXT,
    s3_images_prefix        TEXT,
    parse_backend           TEXT,
    parse_method            TEXT,
    parse_version           TEXT,
    parse_duration_seconds  REAL,
    status                  TEXT NOT NULL DEFAULT 'active',
    tags                    TEXT[] DEFAULT '{}',
    metadata                JSONB DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_doc_hash ON documents(file_hash);
CREATE INDEX IF NOT EXISTS idx_doc_tags ON documents USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_doc_metadata ON documents USING GIN(metadata);
CREATE INDEX IF NOT EXISTS idx_doc_title_trgm ON documents USING GIN(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_doc_status ON documents(status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_doc_created ON documents(created_at DESC);


-- =========================================================
-- 表 2: chunks (内容块 + 向量)
-- =========================================================
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id            BIGSERIAL PRIMARY KEY,
    doc_id              TEXT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    chunk_index         INTEGER NOT NULL,
    chunk_type          TEXT NOT NULL,
    text                TEXT NOT NULL,
    text_length         INTEGER,
    page_idx            INTEGER NOT NULL,
    page_start          INTEGER,
    page_end            INTEGER,
    section_path        TEXT,
    title_level         SMALLINT,
    bbox                JSONB,
    table_html          TEXT,
    table_caption       TEXT,
    equation_latex      TEXT,
    image_path          TEXT,
    image_caption       TEXT,
    embedding           vector(1024),
    embedding_model     TEXT,
    extra               JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_chunks_doc_index UNIQUE(doc_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON chunks
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id);
CREATE INDEX IF NOT EXISTS idx_chunks_type ON chunks(chunk_type);
CREATE INDEX IF NOT EXISTS idx_chunks_doc_page ON chunks(doc_id, page_idx);
CREATE INDEX IF NOT EXISTS idx_chunks_text_trgm ON chunks USING GIN(text gin_trgm_ops);


-- =========================================================
-- 表 3: images (图片资源,可选)
-- =========================================================
CREATE TABLE IF NOT EXISTS images (
    image_id            BIGSERIAL PRIMARY KEY,
    image_hash          TEXT NOT NULL UNIQUE,
    doc_id              TEXT NOT NULL REFERENCES documents(doc_id) ON DELETE CASCADE,
    chunk_id            BIGINT REFERENCES chunks(chunk_id) ON DELETE SET NULL,
    s3_path             TEXT NOT NULL,
    width               INTEGER,
    height              INTEGER,
    mime_type           TEXT,
    size_bytes          INTEGER,
    ocr_text            TEXT,
    vlm_description     TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_images_doc ON images(doc_id);
CREATE INDEX IF NOT EXISTS idx_images_chunk ON images(chunk_id);


-- =========================================================
-- 表 4: conversations (对话历史)
-- =========================================================
CREATE TABLE IF NOT EXISTS conversations (
    conv_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             TEXT,
    session_id          TEXT,
    title               TEXT,
    doc_ids             TEXT[] DEFAULT '{}',
    messages            JSONB NOT NULL DEFAULT '[]'::jsonb,
    message_count       INTEGER DEFAULT 0,
    last_question       TEXT,
    last_answer         TEXT,
    total_input_tokens  INTEGER DEFAULT 0,
    total_output_tokens INTEGER DEFAULT 0,
    model               TEXT,
    feedback_score      SMALLINT,
    feedback_note       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_conv_user ON conversations(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conv_session ON conversations(session_id);
CREATE INDEX IF NOT EXISTS idx_conv_docs ON conversations USING GIN(doc_ids);


-- =========================================================
-- 自动更新 updated_at 的触发器
-- =========================================================
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_documents_updated ON documents;
CREATE TRIGGER trg_documents_updated BEFORE UPDATE ON documents
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_chunks_updated ON chunks;
CREATE TRIGGER trg_chunks_updated BEFORE UPDATE ON chunks
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_conversations_updated ON conversations;
CREATE TRIGGER trg_conversations_updated BEFORE UPDATE ON conversations
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();


-- =========================================================
-- 常用视图:文档 + 统计
-- =========================================================
CREATE OR REPLACE VIEW documents_with_stats AS
SELECT
    d.*,
    (SELECT COUNT(*) FROM chunks c WHERE c.doc_id = d.doc_id AND c.embedding IS NOT NULL) AS chunks_with_embedding,
    (SELECT COUNT(*) FROM chunks c WHERE c.doc_id = d.doc_id AND c.chunk_type = 'text') AS chunks_text,
    (SELECT COUNT(*) FROM chunks c WHERE c.doc_id = d.doc_id AND c.chunk_type = 'table') AS chunks_table
FROM documents d;
