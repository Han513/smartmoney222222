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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app/logs/worker.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# 確保環境變量已設置，如果沒有則使用默認值
if not os.getenv('CELERY_BROKER_URL'):
    os.environ["CELERY_BROKER_URL"] = "redis://localhost:6379/1"
    
if not os.getenv('CELERY_RESULT_BACKEND'):
    os.environ["CELERY_RESULT_BACKEND"] = "redis://localhost:6379/1"

print("啟動 Celery worker...")
print(f"環境變量設置為: CELERY_BROKER_URL={os.getenv('CELERY_BROKER_URL')}")

# 使用 Python 的 -m 選項啟動 Celery
cmd = [
    sys.executable,  # 使用當前 Python 解釋器
    "-m", "celery",
    "-A", "app.workers.tasks",
    "worker",
    "--loglevel=info"
]

# 針對 Windows 環境的特殊處理
if platform.system() == "Windows":
    cmd.extend(["-P", "threads"])  # Windows 上使用線程池
else:
    cmd.append("--pool=solo")      # 其他系統使用 solo 池

# 添加隊列設置
cmd.extend(["-Q", "celery,wallet_tasks"])

# 啟動 worker
print(f"命令: {' '.join(cmd)}")
try:
    return_code = call(cmd)
    sys.exit(return_code)
except Exception as e:
    print(f"啟動 Celery worker 時出錯: {e}")
    sys.exit(1) 