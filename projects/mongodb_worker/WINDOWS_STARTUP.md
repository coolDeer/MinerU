# Windows 研报解析 Worker 启动指南

本文档用于在 Windows 机器上运行 `projects\mongodb_worker\report_worker.py`。

适用场景：

- Windows 电脑或服务器
- NVIDIA GPU
- 使用 conda 管理 Python 环境
- 使用 `.env` 保存 MongoDB、AWS S3、MinerU、LibreOffice 配置

## 1. 安装 Miniconda

PowerShell 或 Warp 中执行：

```powershell
winget install -e --id Anaconda.Miniconda3
```

安装完成后关闭当前终端，重新打开 PowerShell 或 Warp，检查：

```powershell
conda --version
```

如果提示找不到 `conda`，打开开始菜单里的 `Anaconda Prompt (miniconda3)`，执行：

```bat
conda init powershell
```

然后关闭所有终端窗口，重新打开。

如果 conda 要求接受 Terms of Service，执行：

```powershell
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/msys2
```

## 2. 创建并激活 conda 环境

```powershell
conda create -n mineru python=3.11 -y
conda activate mineru
python --version
```

确认输出是 `Python 3.11.x`。

如果 `conda activate mineru` 报 PowerShell 禁止运行脚本，执行：

```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
```

确认输入 `Y`，关闭当前终端并重新打开，再执行：

```powershell
conda activate mineru
```

## 3. 进入 MinerU 项目目录

```powershell
cd C:\path\to\MinerU
```

把 `C:\path\to\MinerU` 替换成真实项目路径。

确认当前分支包含 Windows 修复：

```powershell
git branch --show-current
```

应为：

```text
feature/win_0607
```

## 4. 安装依赖

在已激活的 `(mineru)` 环境中执行：

```powershell
python -m pip install -U pip uv
uv pip install -e ".[all]"
uv pip install -r projects\mongodb_worker\requirements.txt
```

检查 worker 基础依赖：

```powershell
python -c "import pymongo, boto3, httpx, loguru; print('worker deps ok')"
```

## 5. 安装 LibreOffice

如果尚未安装 LibreOffice，可以用：

```powershell
winget install -e --id TheDocumentFoundation.LibreOffice
```

默认可执行文件路径通常是：

```text
C:\Program Files\LibreOffice\program\soffice.exe
```

## 6. 配置 `.env`

复制模板：

```powershell
copy projects\mongodb_worker\.env.example projects\mongodb_worker\.env
notepad projects\mongodb_worker\.env
```

示例配置：

```env
# ---- MongoDB ----
MONGODB_DATABASE_URL=mongodb://user:pass@host:27017/dbname?authSource=dbname
MONGODB_COLL=ResearchReportRecord

# ---- AWS S3 ----
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-southeast-1
AWS_S3_BUCKET_NAME=your_bucket
AWS_S3_PREFIX=research-reports/parsed

# ---- MinerU ----
MINERU_BACKEND=hybrid-auto-engine

# ---- Worker ----
BATCH_SIZE=5
LOCK_TTL_SECONDS=3600
MAX_RETRIES=3
POLL_IDLE_SECONDS=30
LIBREOFFICE_BIN=C:\Program Files\LibreOffice\program\soffice.exe
```

注意：

- `.env` 里有密钥，不要提交到 git。
- MongoDB 密码里的特殊字符需要 URL 编码，例如 `$` 写成 `%24`。
- 当前 `report_worker.py` 不会自动读取 `.env`，需要启动前在 PowerShell 中加载。

## 7. 加载 `.env`

每次新开终端后，在项目根目录执行：

```powershell
Get-Content .\projects\mongodb_worker\.env | Where-Object { $_ -match '^\s*[^#\s].*=' } | ForEach-Object {
  $name, $value = $_ -split '=', 2
  $name = $name.Trim()
  $value = $value.Trim().Trim('"').Trim("'")
  [Environment]::SetEnvironmentVariable($name, $value, 'Process')
}
```

可以检查关键变量：

```powershell
echo $env:MINERU_BACKEND
echo $env:LIBREOFFICE_BIN
echo $env:MONGODB_COLL
```

## 8. 启动 Worker

```powershell
python projects\mongodb_worker\report_worker.py
```

Worker 会循环领取 MongoDB 中 `parseStatus=pending` 的任务，处理完成后把 Markdown、JSON、图片等产物上传到 S3，并回写 `ResearchReportRecord`。

## 9. 每次重启终端后的最小流程

```powershell
conda activate mineru
cd C:\path\to\MinerU

Get-Content .\projects\mongodb_worker\.env | Where-Object { $_ -match '^\s*[^#\s].*=' } | ForEach-Object {
  $name, $value = $_ -split '=', 2
  $name = $name.Trim()
  $value = $value.Trim().Trim('"').Trim("'")
  [Environment]::SetEnvironmentVariable($name, $value, 'Process')
}

python projects\mongodb_worker\report_worker.py
```

## 常见问题

### `conda` 无法识别

执行：

```powershell
conda init powershell
```

然后关闭终端并重新打开。

### PowerShell 禁止运行 conda 脚本

执行：

```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
```

确认输入 `Y`，重开终端。

### `python --versino` 报错

命令拼写应为：

```powershell
python --version
```

### LibreOffice 转换失败

检查路径：

```powershell
& "C:\Program Files\LibreOffice\program\soffice.exe" --version
```

如果路径不同，把 `.env` 中的 `LIBREOFFICE_BIN` 改成真实路径。

### Worker 启动时报缺少环境变量

说明 `.env` 没有加载到当前 PowerShell。重新执行第 7 步加载命令。

