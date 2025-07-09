import signal
import os
import uvicorn
import logging
import platform
import multiprocessing
from app.main import create_app

# 配置日誌
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# )

os.makedirs("app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app/logs/api.log", encoding="utf-8"),
        logging.StreamHandler()  # 仍然輸出到 console
    ]
)

logger = logging.getLogger(__name__)

# 只在非 Windows 系統上使用 uvloop
if platform.system() != "Windows":
    try:
        import uvloop
        uvloop.install()
        logger.info("uvloop successfully installed")
    except ImportError:
        logger.info("uvloop not available, using default event loop")
else:
    # 針對 Windows 的 asyncio 策略
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    logger.info("Running on Windows, setting asyncio event loop policy to WindowsSelectorEventLoopPolicy")

def force_exit(signum=None, frame=None):
    logger.warning("強制終止程序")
    os._exit(1)

def handle_sigint(signum, frame):
    logger.info("接收到中斷信號，正在優雅關閉...")
    try:
        # 向進程組發送信號
        os.killpg(os.getpgid(0), signal.SIGTERM)
        
        # 終止所有 Celery worker 進程
        logger.info("嘗試終止所有 Celery worker 進程...")
        if platform.system() == "Windows":
            os.system("taskkill /F /IM celery.exe")
        else:
            os.system("pkill -f 'celery worker'")
    except Exception as e:
        logger.error(f"終止進程失敗: {e}")
    
    # 設置超時強制退出
    if platform.system() == "Windows":
        import threading
        timer = threading.Timer(10.0, force_exit)
        timer.daemon = True
        timer.start()
    else:
        signal.signal(signal.SIGALRM, force_exit)
        signal.alarm(10)

# 設置信號處理
signal.signal(signal.SIGINT, handle_sigint)

if __name__ == "__main__":
    # 獲取 CPU 核心數
    cpu_count = multiprocessing.cpu_count()
    # 設置 worker 數為 CPU 核心數(最多 8 個)
    workers = min(cpu_count, 1)
    
    logger.info(f"系統 CPU 核心數: {cpu_count}")
    logger.info(f"設置 worker 數量: {workers}")
    
    # 啟動服務器
    uvicorn.run(
        "app.main:create_app",
        host="0.0.0.0",
        port=8070,
        reload=False,  # 生產環境關閉 reload
        workers=workers,
        factory=True,  # 使用工廠模式，確保只有一個進程初始化服務
        log_level="info"
    )