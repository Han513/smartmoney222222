"""
啟動 Celery worker 的腳本，特別處理 Windows 環境下的兼容性問題
使用 python -m celery 方式啟動，避免直接呼叫 celery.exe
"""
import os
import sys
import platform
import logging
from subprocess import call
from dotenv import load_dotenv

# 加載 .env 文件中的環境變量
load_dotenv()

os.makedirs("app/logs", exist_ok=True)

# 為 Worker 創建專門的 logger
worker_logger = logging.getLogger("worker")
worker_logger.setLevel(logging.INFO)

# 清除現有的 handlers
for handler in worker_logger.handlers[:]:
    worker_logger.removeHandler(handler)

# 添加文件 handler
file_handler = logging.FileHandler("app/logs/worker.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
worker_logger.addHandler(file_handler)

# 添加控制台 handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
worker_logger.addHandler(console_handler)

# 防止日誌向上傳播
worker_logger.propagate = False

logger = worker_logger

# 確保環境變量已設置，如果沒有則使用默認值
if not os.getenv('CELERY_BROKER_URL'):
    os.environ["CELERY_BROKER_URL"] = "redis://localhost:6379/1"
    
if not os.getenv('CELERY_RESULT_BACKEND'):
    os.environ["CELERY_RESULT_BACKEND"] = "redis://localhost:6379/1"

# 設置 Celery 日誌環境變量
os.environ["CELERYD_LOG_FILE"] = "app/logs/worker.log"
os.environ["CELERYD_LOG_LEVEL"] = "INFO"

print("啟動 Celery worker...")
print(f"環境變量設置為: CELERY_BROKER_URL={os.getenv('CELERY_BROKER_URL')}")
print(f"日誌文件設置為: {os.getenv('CELERYD_LOG_FILE')}")

# 使用 Python 的 -m 選項啟動 Celery
cmd = [
    sys.executable,  # 使用當前 Python 解釋器
    "-m", "celery",
    "-A", "app.workers.tasks",
    "worker",
    "--loglevel=info",
    "--logfile=app/logs/worker.log"  # 指定日誌文件
]

# 針對 Windows 環境的特殊處理
if platform.system() == "Windows":
    cmd.extend(["-P", "threads"])  # Windows 上使用線程池
else:
    cmd.append("--pool=solo")      # 其他系統使用 solo 池

# 添加隊列設置，使用與 celeryconfig.py 相同的隊列名稱
cmd.extend(["-Q", "wallet_analysis,batch_processing"])

# 啟動 worker
print(f"命令: {' '.join(cmd)}")
try:
    return_code = call(cmd)
    sys.exit(return_code)
except Exception as e:
    print(f"啟動 Celery worker 時出錯: {e}")
    sys.exit(1) 