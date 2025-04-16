"""
啟動 Celery worker 的腳本，強制設置正確的 Redis URL
"""
import os
import sys
from subprocess import call

# 強制設置環境變量
os.environ["CELERY_BROKER_URL"] = "redis://127.0.0.1:6379/1"
os.environ["CELERY_RESULT_BACKEND"] = "redis://127.0.0.1:6379/1"

# 構建 Celery 啟動命令
cmd = [
    "celery",
    "-A", "app.workers.tasks",
    "worker",
    "--loglevel=info",
    "--pool=solo",
    "-Q", "celery,wallet_tasks"
]

# 啟動 worker
print("啟動 Celery worker...")
print(f"環境變量設置為: CELERY_BROKER_URL={os.environ['CELERY_BROKER_URL']}")
print(f"命令: {' '.join(cmd)}")
return_code = call(cmd)
sys.exit(return_code) 