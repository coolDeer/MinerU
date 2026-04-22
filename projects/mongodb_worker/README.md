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

export S3_BUCKET="your-bucket"
export S3_PREFIX="mineru-parsed"
export S3_ENDPOINT=""  # AWS 留空; MinIO/OSS 填 endpoint
export AWS_ACCESS_KEY="..."
export AWS_SECRET_KEY="..."

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
