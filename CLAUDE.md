# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

MinerU is a document-parsing engine that converts PDF, images, DOCX, PPTX, and XLSX into Markdown and JSON. It is a Python package (`mineru`) with multiple CLI entry points defined in `pyproject.toml`, a FastAPI server, a router for multi-GPU/multi-service fan-out, and a Gradio UI. Python 3.10–3.13 supported.

## Common commands

Install from source (editable, all optional extras):

```bash
pip install -U pip && pip install uv
uv pip install -e .[all]        # core = vlm + pipeline + gradio
uv pip install -e .[test]       # adds pytest, pytest-cov, fuzzywuzzy
```

Backend-specific extras (do not install more than needed): `vlm`, `vllm` (linux only), `lmdeploy` (win only), `mlx` (darwin only), `pipeline`, `gradio`, `core`, `all`.

Run the CLI:

```bash
mineru -p <input_path> -o <output_path>           # hybrid-auto-engine default
mineru -p <input_path> -o <output_path> -b pipeline   # pure CPU, no VLM
```

Entry points (all console scripts registered in `pyproject.toml`):

- `mineru` — orchestration client (`mineru.cli.client:main`). When `--api-url` is not supplied it spawns a local `mineru-api` process automatically.
- `mineru-api` — FastAPI server (`mineru.cli.fast_api:main`). Sync `POST /file_parse` + async `POST /tasks`.
- `mineru-router` — unified multi-GPU/multi-upstream router (`mineru.cli.router:main`). Interface-compatible with `mineru-api`.
- `mineru-vllm-server` / `mineru-lmdeploy-server` / `mineru-openai-server` — VLM model servers.
- `mineru-models-download` — pre-fetch weights.
- `mineru-gradio` — Gradio demo UI.

Run the demo end-to-end (starts a local `mineru-api` if no URL given):

```bash
python demo/demo.py
```

Tests (E2E, coverage-driven):

```bash
python tests/clean_coverage.py    # wipes htmlcov/
coverage run                       # runs tests/unittest/test_e2e.py (see pyproject [tool.coverage.run])
python tests/get_coverage.py       # asserts coverage >= 0.2%
# Or: pytest tests/unittest/test_e2e.py
```

Note: `pyproject.toml` pins `addopts = "-s --cov=mineru --cov-report html"`, so any `pytest` invocation writes `htmlcov/`. Single test: `pytest tests/unittest/test_e2e.py::test_pipeline_with_two_config`.

## Architecture

### Backends — three parsing engines, selected by CLI `-b` / `backend` form field

Each backend lives under `mineru/backend/<name>/` and exposes a `*_analyze` entrypoint plus a `<name>_middle_json_mkcontent.union_make` emitter.

- `pipeline` (`mineru/backend/pipeline/`) — classic OCR + layout + formula/table pipeline. Runs on CPU or GPU. Model init is gated by `PIPELINE_MODEL_INIT_LOCK`. Streaming variant: `doc_analyze_streaming`.
- `vlm` (`mineru/backend/vlm/`) — vision-language model via `mineru_vl_utils.MinerUClient`. Variants selected by the `*-auto-engine` vs `*-http-client` suffix in backend name. Model singleton lives in `vlm_analyze.ModelSingleton` and dispatches to vllm / lmdeploy / mlx / transformers per platform.
- `hybrid` (`mineru/backend/hybrid/`) — combines pipeline text extraction with VLM layout for low-hallucination output. Requires `mineru[pipeline]` (torch) even when used as `hybrid-auto-engine`; `common.ensure_backend_dependencies` raises `HybridDependencyError` otherwise.
- `office` (`mineru/backend/office/`) — native DOCX/PPTX/XLSX parsers (no PDF conversion). `docx_analyze`, `pptx_analyze`, `xlsx_analyze` each produce the same middle-JSON shape.

Backend names accepted by the CLI/API follow the pattern `<engine>[-auto-engine|-http-client]`, e.g. `hybrid-auto-engine`, `vlm-http-client`. `*-http-client` requires a `server_url` pointing at an OpenAI-compatible VLM server.

### The "middle JSON" is the pivot format

Every backend produces an intermediate middle JSON (`init_middle_json` → page-by-page `append_*` → `finalize_middle_json`). Downstream, each backend's `*_middle_json_mkcontent.union_make(middle_json, mode)` turns it into Markdown, content-list JSON, or other outputs (`MakeMode` enum in `mineru/utils/enum_class.py`). When changing output formatting, edit the `union_make` for the relevant backend — do not rewrite analyze code.

### CLI ↔ API orchestration

`mineru.cli.client` is not a monolithic parser: it packages inputs, submits them to a `mineru-api` endpoint (starting a local one if needed via `LocalAPIServer`), polls `GET /tasks/{id}`, downloads a result zip, and extracts it. The single source of truth for request shape is `mineru.cli.api_client.build_parse_request_form_data`; both `client.py` and `demo/demo.py` call it. Do NOT duplicate form-data construction.

`mineru-router` spawns multiple `mineru-api` workers (one per GPU) and load-balances over them plus any configured upstream URLs. Router↔worker config flows through env vars prefixed `MINERU_ROUTER_*` and `MINERU_API_*`.

### Model weights and config

- Config file resolves via `MINERU_TOOLS_CONFIG_JSON` (absolute path) or `~/mineru.json`. Template: `mineru.template.json`. Used for S3 buckets, LaTeX delimiters, LLM-aided title cleanup, and model dirs. Missing config is fine — defaults kick in.
- Weights are fetched via `modelscope` or `huggingface-hub`. Set `MINERU_MODEL_SOURCE=modelscope` for CN network (see commented line in `demo/demo.py:236`).
- `MINERU_LMDEPLOY_DEVICE=maca` disables cuDNN at import time (see `cli/common.py:28`).
- Log level: `MINERU_LOG_LEVEL` (default `INFO`).
- PDF rendering threads/timeout: `MINERU_PDF_RENDER_THREADS`, `MINERU_PDF_RENDER_TIMEOUT`.

### Module map for quick navigation

- `mineru/cli/` — all CLI/FastAPI entry points, `common.py` is the shared plumbing (task-stem normalization, backend dispatch, `do_parse` / `aio_do_parse`).
- `mineru/backend/` — engines (see above).
- `mineru/model/` — model implementations: `layout/` (pp_doclayoutv2), `ocr/` (pytorch-paddle port), `mfr/` (formula recognition), `table/`, `vlm/` (server wrappers), `docx/`, `pptx/`, `xlsx/`, `ori_cls/`.
- `mineru/data/` — readers/writers (`data_reader_writer/`), IO abstractions, S3 helpers.
- `mineru/utils/` — bbox math, language detection, pdfium guards, model download, draw_bbox visualization, env/config readers.
- `mineru/resources/` — bundled assets (ships in wheel via `[tool.setuptools.package-data]`).

## Conventions to respect

- All new file-name stems flow through `normalize_task_stem` / `uniquify_task_stems` in `cli/common.py` (UTF-8 byte cap of 200 for filesystem safety). Use them instead of rolling your own sanitization.
- Use `mineru.utils.pdfium_guard` wrappers (`open_pdfium_document`, `close_pdfium_document`, `get_pdfium_document_page_count`, `rewrite_pdf_bytes_with_pdfium`) instead of raw `pypdfium2` calls — they exist to work around pdfium lifetime bugs.
- Logging uses `loguru` (`from loguru import logger`) — not stdlib `logging`.
- Bilingual docs: every README/doc update should be mirrored between `README.md` / `README_zh-CN.md` and under `docs/en/` / `docs/zh/`.
- Versioning: `mineru/version.py` is the single source; `setuptools.dynamic` reads it. `update_version.py` exists to bump it.
- Do not reintroduce AGPLv3 models (`doclayoutyolo`, `mfd_yolov8`) or `layoutreader` (CC-BY-NC-SA 4.0) — they were deliberately removed in 3.0 for licensing reasons.

## Hardware/backend compatibility gotchas

- Only Volta-and-later NVIDIA GPUs or Apple Silicon are supported for local VLM/hybrid inference.
- Windows: `ray` has no py3.13 wheel, so Windows is 3.10–3.12 only.
- macOS: requires 14.0+ for `mlx`-based VLM.
- Pure-CPU path is only viable through the `pipeline` backend.
- Under WSL, vLLM ZeroMQ IPC fails on `/mnt/*` tempdirs — `demo.prepare_local_api_temp_dir` forces `TMPDIR=/tmp`; reuse that helper when launching local servers from WSL-mounted paths.
