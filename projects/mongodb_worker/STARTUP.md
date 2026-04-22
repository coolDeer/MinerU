# 研报解析 Worker — 启动指南

## 前置依赖

| 依赖 | 说明 |
|---|---|
| Python 3.10–3.13 | `python3 --version` 确认 |
| uv | Python 包管理器 |
| LibreOffice | Word → PDF 转换 |

### 安装 uv（只做一次）

```bash
python3 -m ensurepip --upgrade
python3 -m pip install uv
```

### 安装 LibreOffice（只做一次）

```bash
brew install --cask libreoffice
```

---

## 第一次初始化（只做一次）

```bash
cd /Users/bububot/Desktop/project/MinerU   # 换成你的实际路径

# 1. 建虚拟环境
uv venv .venv

# 2. 激活
source .venv/bin/activate

# 3. 装 MinerU + 所有依赖
uv pip install -e ".[all]"
uv pip install pymongo boto3 httpx loguru

# 4. 确认装好
mineru-api --help
python3 -c "import pymongo, boto3; print('deps OK')"
```

---

## 配置环境变量（只做一次）

复制模板：

```bash
cp projects/mongodb_worker/.env.example projects/mongodb_worker/.env
```

编辑 `.env` 填入真实值：

```bash
# ---- MongoDB ----
MONGODB_DATABASE_URL=mongodb://user:pass@host:27017/dbname?authSource=dbname

# ---- AWS S3 ----
AWS_ACCESS_KEY_ID=你的key
AWS_SECRET_ACCESS_KEY=你的secret
AWS_REGION=ap-southeast-1
AWS_S3_BUCKET_NAME=你的bucket
AWS_S3_PREFIX=research-reports/parsed

# ---- MinerU ----
MINERU_API_URL=http://127.0.0.1:8000
MINERU_BACKEND=hybrid-auto-engine

# ---- Worker ----
BATCH_SIZE=5
LOCK_TTL_SECONDS=3600
MAX_RETRIES=3
POLL_IDLE_SECONDS=30
LIBREOFFICE_BIN=/Applications/LibreOffice.app/Contents/MacOS/soffice
```

---

## 每次启动（需要两个终端窗口）

### 终端 A — mineru-api（保持开着）

```bash
cd /Users/bububot/Desktop/project/MinerU
source .venv/bin/activate
mineru-api --host 127.0.0.1 --port 8000
```

等出现这行再开 B：

```
INFO: Uvicorn running on http://127.0.0.1:8000
```

> 首次启动会下载模型权重（几 GB），需要等待。

### 终端 B — Worker

```bash
cd /Users/bububot/Desktop/project/MinerU
source .venv/bin/activate
set -a && source projects/mongodb_worker/.env && set +a

python3 projects/mongodb_worker/report_worker.py
```

---

## 向 MongoDB 投入任务

将要解析的记录 `parseStatus` 置为 `pending`，Worker 自动拉取：

```js
// 单条
db.ResearchReportRecord.updateOne(
  { researchId: "xxx" },
  { $set: { parseStatus: "pending", parseRetryCount: 0 } }
)

// 批量（所有没有 parseStatus 字段的）
db.ResearchReportRecord.updateMany(
  { parseStatus: { $exists: false } },
  { $set: { parseStatus: "pending", parseRetryCount: 0 } }
)
```

---

## 状态说明

| parseStatus | 含义 |
|---|---|
| `pending` | 待处理（初始值） |
| `processing` | 处理中 |
| `completed` | 已完成，不再处理 |
| `failed` | 超过重试上限，需人工介入 |

**查看当前各状态数量：**

```js
db.ResearchReportRecord.aggregate([
  { $group: { _id: "$parseStatus", n: { $sum: 1 } } }
])
```

**人工重试 failed 的记录：**

```js
db.ResearchReportRecord.updateMany(
  { parseStatus: "failed" },
  { $set: { parseStatus: "pending", parseRetryCount: 0, parseErrorMessage: null } }
)
```

---

## S3 产物结构

```
s3://<bucket>/research-reports/parsed/<researchId>/
├── source.pdf / source.docx / source.xlsx   # 原始文件
├── converted.pdf                             # Word 转来的 PDF（仅 docx/doc 输入时有）
├── <name>.md                                 # Markdown 正文
├── <name>_content_list.json                  # 结构化块（标题/段落/表/图）
├── <name>_layout.pdf                         # 带 bbox 的可视化 PDF
└── images/                                   # 抽出的图片
```

---

## 常见问题

| 现象 | 原因 | 解决 |
|---|---|---|
| `command not found: pip` | venv 未激活 | `source .venv/bin/activate` |
| `Error: source file could not be loaded` | LibreOffice 无法读文件 | 检查文件路径是否存在，文件是否损坏 |
| soffice 进程不退出 | 多实例抢锁 | `pkill -9 -f soffice` |
| 记录卡在 `processing` | Worker 崩溃了 | 等 `LOCK_TTL_SECONDS`（1h）超时自动释放，或手动 `updateMany({parseStatus:"processing"}, {$set:{parseStatus:"pending"}})` |
| `SignatureDoesNotMatch` | S3 region 不对 | 确认 `AWS_REGION` 与 bucket 实际 region 一致 |
| mineru-api 响应慢 | 第一次加载模型 | 等模型加载完（日志不再有大量 INFO 输出）|
