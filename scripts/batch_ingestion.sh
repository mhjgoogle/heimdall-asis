#!/bin/bash
# 批处理数据摄入脚本 - 从指定数据源获取数据
# 用法: bash batch_ingestion.sh Daily
#       bash batch_ingestion.sh HOURLY
#       bash batch_ingestion.sh Monthly

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB_PATH="$PROJECT_ROOT/local/data/heimdall.db"
LOG_FILE="$PROJECT_ROOT/local/logs/batch_ingestion.log"
PYTHON_SCRIPT="$PROJECT_ROOT/scripts/run_batch_ingestion.py"

FREQUENCY="${1:?请指定频率: HOURLY|Daily|Monthly|Quarterly}"

# 创建日志目录
mkdir -p "$(dirname "$LOG_FILE")"

# 执行 Python 摄入脚本
cd "$PROJECT_ROOT"
poetry run python3 "$PYTHON_SCRIPT" "$FREQUENCY" >> "$LOG_FILE" 2>&1

exit_code=$?
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 完成 (exit code: $exit_code)" >> "$LOG_FILE"

exit $exit_code
