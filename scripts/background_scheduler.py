#!/usr/bin/env python3
"""
简单的后台调度器 - 定时运行 batch_ingestion.sh
"""

import schedule
import time
import subprocess
import logging
from pathlib import Path

# 配置
PROJECT_ROOT = Path(__file__).parent.parent
LOG_FILE = PROJECT_ROOT / "local" / "logs" / "scheduler.log"
SCRIPT = PROJECT_ROOT / "scripts" / "batch_ingestion.sh"

# 日志
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_batch(frequency):
    """运行批处理"""
    logger.info(f"运行 {frequency} 摄入...")
    result = subprocess.run(
        [str(SCRIPT), frequency],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        logger.info(f"✅ {frequency} 完成")
    else:
        logger.error(f"❌ {frequency} 失败: {result.stderr[:200]}")

# 调度任务
# NEWS catalogs 已改为 Daily，只需在 00:05 统一运行
schedule.every().day.at("00:05").do(run_batch, frequency="Daily")   # Daily 包括所有 NEWS
schedule.every().day.at("00:10").do(run_batch, frequency="Monthly")
schedule.every().day.at("00:15").do(run_batch, frequency="Quarterly")

logger.info("后台调度器已启动")
logger.info("  • Daily: 每天 00:05（包括 NEWS 和其他 Daily 数据）")
logger.info("  • Monthly: 每月 1 号 00:10")
logger.info("  • Quarterly: 每季度 1 号 00:15")

# 主循环（带错误恢复）
retry_count = 0
max_retries = 5

while True:
    try:
        schedule.run_pending()
        time.sleep(60)
        retry_count = 0  # 成功运行，重置重试计数
    except Exception as e:
        retry_count += 1
        logger.error(f"调度器错误 (重试 {retry_count}/{max_retries}): {e}")
        if retry_count >= max_retries:
            logger.critical(f"调度器连续失败 {max_retries} 次，退出")
            break
        time.sleep(10)  # 等待 10 秒后重试
