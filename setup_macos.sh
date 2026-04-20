#!/usr/bin/env bash
# MinerU Mac mini (Apple Silicon) 一键部署脚本
# 方案一:hybrid-auto-engine + MLX,适用于 64GB 内存 / macOS 14.0+
# 用法:
#   chmod +x setup_macos.sh
#   ./setup_macos.sh              # 完整安装 + smoke test
#   ./setup_macos.sh --skip-test  # 跳过 smoke test(只装环境)
#   ./setup_macos.sh --verify     # 只做环境验证,不安装

set -euo pipefail

# ---------- 颜色输出 ----------
RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'
BLUE=$'\033[0;34m'
NC=$'\033[0m'

info()  { printf "%s[INFO]%s %s\n"  "$BLUE"   "$NC" "$*"; }
ok()    { printf "%s[ OK ]%s %s\n"  "$GREEN"  "$NC" "$*"; }
warn()  { printf "%s[WARN]%s %s\n"  "$YELLOW" "$NC" "$*"; }
fail()  { printf "%s[FAIL]%s %s\n"  "$RED"    "$NC" "$*" >&2; exit 1; }
step()  { printf "\n%s========== %s ==========%s\n" "$BLUE" "$*" "$NC"; }

# ---------- 参数解析 ----------
SKIP_TEST=0
VERIFY_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --skip-test) SKIP_TEST=1 ;;
    --verify)    VERIFY_ONLY=1 ;;
    -h|--help)
      sed -n '2,8p' "$0"
      exit 0
      ;;
    *) fail "未知参数: $arg" ;;
  esac
done

# ---------- 路径 ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON_VERSION="3.12"

# =========================================================
# Step 0 - 环境预检
# =========================================================
step "Step 0 / 环境预检"

# 0.1 macOS 版本
if [[ "$(uname -s)" != "Darwin" ]]; then
  fail "本脚本仅支持 macOS,当前系统: $(uname -s)"
fi
MACOS_VER=$(sw_vers -productVersion)
MACOS_MAJOR=${MACOS_VER%%.*}
if (( MACOS_MAJOR < 14 )); then
  fail "macOS 版本过低($MACOS_VER),MLX 后端要求 ≥ 14.0"
fi
ok "macOS 版本: $MACOS_VER"

# 0.2 架构
ARCH=$(uname -m)
if [[ "$ARCH" != "arm64" ]]; then
  fail "芯片架构为 $ARCH,本脚本仅支持 Apple Silicon (arm64)"
fi
ok "芯片架构: arm64 (Apple Silicon)"

# 0.3 内存
MEM_BYTES=$(sysctl -n hw.memsize)
MEM_GB=$((MEM_BYTES / 1024 / 1024 / 1024))
ok "物理内存: ${MEM_GB} GB"
if (( MEM_GB < 16 )); then
  warn "内存小于 16GB,hybrid 模式可能吃紧,建议改用 pipeline 后端"
fi

# 0.4 磁盘空间(需要 ≥ 20GB)
DISK_FREE_GB=$(df -g "$HOME" | awk 'NR==2 {print $4}')
ok "Home 可用磁盘: ${DISK_FREE_GB} GB"
if (( DISK_FREE_GB < 20 )); then
  warn "可用磁盘不足 20GB,模型+缓存可能装不下"
fi

# 0.5 Homebrew
if ! command -v brew >/dev/null 2>&1; then
  warn "未检测到 Homebrew,如需安装 uv/python 请先装 brew: https://brew.sh"
else
  ok "Homebrew: $(brew --version | head -n1)"
fi

if (( VERIFY_ONLY == 1 )); then
  ok "仅验证模式,已完成"
  exit 0
fi

# =========================================================
# Step 1 - 安装 uv
# =========================================================
step "Step 1 / 安装 uv (快速 Python 包管理器)"

if command -v uv >/dev/null 2>&1; then
  ok "uv 已安装: $(uv --version)"
else
  if command -v brew >/dev/null 2>&1; then
    info "通过 Homebrew 安装 uv..."
    brew install uv
  else
    info "通过官方脚本安装 uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
  fi
  command -v uv >/dev/null 2>&1 || fail "uv 安装失败,请手动安装"
  ok "uv 安装成功: $(uv --version)"
fi

# =========================================================
# Step 2 - 创建虚拟环境
# =========================================================
step "Step 2 / 创建 Python $PYTHON_VERSION 虚拟环境"

if [[ -d "$VENV_DIR" ]]; then
  warn ".venv 已存在: $VENV_DIR"
  read -r -p "是否删除并重建?(y/N) " ans
  if [[ "$ans" =~ ^[Yy]$ ]]; then
    rm -rf "$VENV_DIR"
    info "已删除旧 venv"
  else
    info "复用现有 venv"
  fi
fi

if [[ ! -d "$VENV_DIR" ]]; then
  uv venv --python "$PYTHON_VERSION" "$VENV_DIR"
  ok "venv 创建完成: $VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"
ok "venv 已激活,当前 Python: $(python --version)"

# =========================================================
# Step 3 - 安装 MinerU 依赖
# =========================================================
step "Step 3 / 安装 MinerU 及全部依赖 (.[all])"

info "预计下载 2-4GB,耗时 3-8 分钟(无网络限制)..."
uv pip install -e ".[all]"
ok "依赖安装完成"

# 校验关键命令
for cmd in mineru mineru-api mineru-gradio mineru-models-download; do
  command -v "$cmd" >/dev/null 2>&1 || fail "$cmd 未注册,安装异常"
done
ok "CLI 入口全部就绪: mineru, mineru-api, mineru-gradio, mineru-models-download"
info "MinerU 版本: $(mineru --version 2>&1 | head -n1 || echo '未知')"

# =========================================================
# Step 4 - 写入会话环境文件
# =========================================================
step "Step 4 / 生成会话激活脚本 activate_mineru.sh"

cat > "$SCRIPT_DIR/activate_mineru.sh" <<EOF
#!/usr/bin/env bash
# 每次使用 MinerU 前执行: source activate_mineru.sh
# (由 setup_macos.sh 自动生成,可手动编辑)

# 激活虚拟环境
source "$VENV_DIR/bin/activate"

# 日志 & 性能
export MINERU_LOG_LEVEL=INFO
export TOKENIZERS_PARALLELISM=false
export PYTORCH_ENABLE_MPS_FALLBACK=1

# PDF 渲染并发
export MINERU_PDF_RENDER_THREADS=4
export MINERU_PDF_RENDER_TIMEOUT=300

# 模型下载源:HK 网络无限制,默认走 HuggingFace;如需切换放开下一行
# export MINERU_MODEL_SOURCE=modelscope

# 如需自定义模型/缓存路径,放开并改成实际路径
# export HF_HOME="\$HOME/.cache/huggingface"
# export MODELSCOPE_CACHE="\$HOME/.cache/modelscope"
# export MINERU_TOOLS_CONFIG_JSON="\$HOME/mineru.json"

echo "[MinerU] venv 已激活,Python: \$(python --version)"
echo "[MinerU] 使用示例: mineru -p <input> -o <output> -b hybrid-auto-engine"
EOF

chmod +x "$SCRIPT_DIR/activate_mineru.sh"
ok "会话脚本已生成: $SCRIPT_DIR/activate_mineru.sh"
info "以后每次开 terminal 进入项目后: source ./activate_mineru.sh"

# =========================================================
# Step 5 - Smoke Test (首次会拉模型)
# =========================================================
if (( SKIP_TEST == 1 )); then
  step "Step 5 / 跳过 smoke test (--skip-test)"
else
  step "Step 5 / Smoke Test(首次运行会下载 2-3GB 模型)"

  TEST_INPUT="$SCRIPT_DIR/demo/pdfs"
  TEST_OUTPUT="$SCRIPT_DIR/test_output"

  if [[ ! -d "$TEST_INPUT" ]]; then
    warn "未找到 demo/pdfs,跳过 smoke test"
  else
    # 应用 activate 脚本中的 env
    # shellcheck source=/dev/null
    source "$SCRIPT_DIR/activate_mineru.sh"

    info "测试输入: $TEST_INPUT"
    info "测试输出: $TEST_OUTPUT"
    info "后端: hybrid-auto-engine"
    mkdir -p "$TEST_OUTPUT"

    if mineru -p "$TEST_INPUT" -o "$TEST_OUTPUT" -b hybrid-auto-engine; then
      ok "Smoke test 成功"
      echo
      info "产物目录结构:"
      find "$TEST_OUTPUT" -maxdepth 3 -type f | head -n 20
    else
      fail "Smoke test 失败,请检查上方日志"
    fi
  fi
fi

# =========================================================
# 完成
# =========================================================
step "部署完成"
cat <<EOF

${GREEN}下一步使用姿势:${NC}

  cd $SCRIPT_DIR
  source ./activate_mineru.sh

  # 单文件
  mineru -p ~/Documents/paper.pdf -o ~/Documents/parsed -b hybrid-auto-engine

  # 批量目录
  mineru -p ~/Documents/pdfs/ -o ~/Documents/parsed/

  # 指定中文 OCR
  mineru -p paper.pdf -o out -l ch

  # 启动 API 服务(供其他程序调用 / Gradio)
  mineru-api --host 0.0.0.0 --port 8000
  mineru-gradio --host 0.0.0.0 --port 7860

${GREEN}环境变量在:${NC} $SCRIPT_DIR/activate_mineru.sh
${GREEN}虚拟环境在:${NC} $VENV_DIR
${GREEN}模型缓存在:${NC} \$HOME/.cache/huggingface/hub/  (默认)

EOF
