# MinerU MongoDB Worker

定时从 MongoDB 拉取待解析文件 → 下载 → 调 mineru-api 解析 → 上传 S3 → 更新 MongoDB。

## 架构

```
[scheduler] → [worker.py] ─(HTTP)─► [mineru-api 长驻服务]
                   ↓                       ↓
              [MongoDB]            模型推理(本地 MLX / GPU)
                   ↓
                [S3]
```

## MongoDB 文档结构

```js
{
  _id: ObjectId,
  source_url: "https://example.com/file.pdf",  // http/https/s3://
  file_name: "xxx.pdf",
  file_type: "pdf",

  status: "pending",  // pending | downloading | parsing | uploading | completed | failed
  retry_count: 0,
  error_message: null,

  // 完成后回填:
  parsed_markdown_s3: "mineru-parsed/<id>/xxx/auto/xxx.md",
  parsed_json_s3: "mineru-parsed/<id>/xxx/auto/xxx_content_list.json",
  parsed_images_s3_prefix: "mineru-parsed/<id>/xxx/auto/images/",

  created_at: ISODate,
  updated_at: ISODate,
  locked_by: null,
  locked_until: null
}
```

索引:
```js
db.documents.createIndex({ status: 1, locked_until: 1 });
db.documents.createIndex({ source_url: 1 }, { unique: true });
```

## 部署

### 1. 启动 mineru-api(长驻)
```bash
cd /path/to/MinerU
source .venv/bin/activate
mineru-api --host 127.0.0.1 --port 8000
```

建议用 `tmux` / `launchd` / `systemd` 守护。

### 2. 配置 worker 环境变量

```bash
export MONGO_URI="mongodb://user:pass@host:27017"
export MONGO_DB="mineru"
export MONGO_COLL="documents"

export MINERU_API_URL="http://127.0.0.1:8000"
export MINERU_BACKEND="hybrid-auto-engine"

export NODE_SERVICE_URL="https://your-node-service"
export AUTH_API_KEY="your-node-api-key"
# 可选：校验 Node 配置的 bucket
# export S3_BUCKET="your-bucket"
export S3_PREFIX="mineru-parsed"

export BATCH_SIZE=5
export LOCK_TTL_SECONDS=3600
export MAX_RETRIES=3
```

### 3. 运行

#### 常驻守护(推荐)
```bash
python projects/mongodb_worker/worker.py
```
脚本内置循环:有任务就处理,没任务 sleep 30 秒。

#### Cron 定时(备选)
如果想走 cron 风格,把 `main_loop` 里的 `while True` 改成跑一轮就退出,然后:
```cron
*/5 * * * * cd /path/to/MinerU && source .venv/bin/activate && python projects/mongodb_worker/worker.py
```

## 任务调度策略

- **乐观锁**:`claim_one_task` 用 MongoDB `findAndModify` 原子抢任务,多 worker 安全
- **锁超时**:默认 1 小时,worker 挂了其他 worker 可抢占
- **重试**:失败任务 `retry_count < MAX_RETRIES` 时可被重抢
- **死信**:`status=failed` 且 `retry_count >= MAX_RETRIES` 需要人工介入

## 向 MongoDB 插入任务

```python
from pymongo import MongoClient
from datetime import datetime, timezone

coll = MongoClient("mongodb://...")["mineru"]["documents"]
coll.insert_one({
    "source_url": "https://example.com/paper.pdf",
    "file_name": "paper.pdf",
    "file_type": "pdf",
    "status": "pending",
    "retry_count": 0,
    "created_at": datetime.now(timezone.utc),
    "updated_at": datetime.now(timezone.utc),
})
```

## 监控

```js
// 待处理数量
db.documents.countDocuments({ status: "pending" });

// 正在处理(可能卡死)
db.documents.find({ status: { $in: ["downloading","parsing","uploading"] } });

// 失败任务
db.documents.find({ status: "failed" }).sort({ updated_at: -1 });

// 吞吐(最近 1 小时)
db.documents.countDocuments({
  status: "completed",
  updated_at: { $gt: new Date(Date.now() - 3600000) }
});
```

## 扩展点

- **多 worker**:多台机器同时跑 `worker.py`,靠乐观锁不会重复
- **多 mineru-api**:部署 `mineru-router` 做多 GPU 负载均衡,worker 只需改 `MINERU_API_URL` 指向 router
- **Webhook 通知**:完成后发消息到 Slack / 钉钉
- **SLA 告警**:Prometheus 监控 pending 队列长度 + 失败率

## Node 签名上传与私有文件读取

三个 worker 和底层 S3Reader/S3Writer 均通过 Node 获取签名，再使用 HTTP PUT/GET 传输文件。MinerU 不再需要 AWS 密钥、S3 endpoint 或 boto3；Node 服务保留 AWS 配置。旧构造函数的凭证参数仅为兼容调用保留，不再使用，`bucket_info` 也不再读取。当前只支持 Node 配置的 AWS bucket，不支持任意多 bucket 或 MinIO。

- 先部署 Node 端本次目录白名单更新，再更新 MinerU；远程鉴权使用 `AUTH_API_KEY`，沿用 Node 的 IP 白名单规则。
- 主流程沿用 `AWS_S3_PREFIX=research-reports/parsed`；通用 worker 沿用 `S3_PREFIX`。Node 放行 `research-reports`、`mineru-parsed`、`mineru`。
- Markdown、JSON、PDF 等按私有文件上传。图片进入 `images/mineru/{原业务目录}/...`，Markdown 和 JSON 的相对图片引用替换为实际返回的地址。本地原始产物保留。图片是否公开读取由 AWS 的 `images/*` Bucket 策略控制，需要部署时确认。
- 数据库保存永久 `fileUrl` 或实际 `key`（沿用各 worker 原有字段格式），不保存会过期的签名链接，也没有 complete 接口。
- 下载私有原文件时，`s3://bucket/key` 和已编码的 AWS S3 永久地址会先申请下载签名；外部 HTTP 链接和已有签名链接直接使用。历史含未编码 `#` 的地址有歧义，应提供正确编码地址或原始 key。
- 每次上传计算 SHA-256，使用签名返回的请求头；PUT 遇到网络故障、429 或指定 5xx 时最多尝试 3 次，并重新获取签名。Node 取签名失败由 worker 任务重试处理。
- 单文件范围为 1 字节至 1 GiB，格式须通过 Node 校验。旧自定义目录需要在 Node 白名单中配置；超长/特殊文件名可能被 Node 规范化，所以必须保存返回结果。

轻量回归测试（仅需 httpx，不加载解析模型）：

```bash
python -m unittest discover -s projects/mongodb_worker/tests -v
```
