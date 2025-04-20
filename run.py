import signal
import os
import uvicorn
import logging
import argparse
import sys
import platform
import multiprocessing
import threading

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
    logger.info("Running on Windows, skipping uvloop")

def force_exit(signum=None, frame=None):
    logger.warning("強制終止程序")
    os._exit(1)

# 設置 SIGINT 處理
def handle_sigint(signum, frame):
    logger.info("接收到中斷信號，正在優雅關閉...")
    try:
        import subprocess
        import os
        import signal
        
        # 向進程組發送信號
        os.killpg(os.getpgid(0), signal.SIGTERM)
        
        logger.info("嘗試終止所有 Celery worker 進程...")
        subprocess.run("pkill -f 'celery worker'", shell=True)
    except Exception as e:
        logger.error(f"終止進程失敗: {e}")
    
    # 強制退出
    force_exit()
    
    # 立即嘗試終止所有 Celery worker 進程
    try:
        import subprocess
        logger.info("嘗試終止所有 Celery worker 進程...")
        subprocess.run("pkill -f 'celery worker'", shell=True)
    except Exception as e:
        logger.error(f"終止 Celery worker 失敗: {e}")
    
    # Windows 平台使用定時器替代 SIGALRM
    if platform.system() == "Windows":
        timer = threading.Timer(10.0, force_exit)
        timer.daemon = True
        timer.start()
    else:
        # Unix/Linux 平台使用 SIGALRM
        signal.signal(signal.SIGALRM, force_exit)
        signal.alarm(10)

signal.signal(signal.SIGINT, handle_sigint)

def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = True):
    """
    運行伺服器
    """
    # 在函數內部引用 app 避免循環引用
    from app.main import app
    
    # 詳細記錄所有可用路由
    try:
        routes = []
        for route in app.routes:
            if hasattr(route, "path") and hasattr(route, "methods"):
                routes.append(f"{', '.join(route.methods)} {route.path}")
            elif hasattr(route, "path"):
                routes.append(f"PATH {route.path}")

        logger.info(f"Available routes ({len(routes)}):")
        for route in sorted(routes):
            logger.info(f"  {route}")
    except Exception as e:
        logger.error(f"Error listing routes: {str(e)}")

    # 記錄中間件
    try:
        if hasattr(app, "user_middleware") and app.user_middleware:
            middleware_count = len(app.user_middleware)
            logger.info(f"Active middleware ({middleware_count}):")
            for middleware in app.user_middleware:
                logger.info(f"  {middleware.__class__.__name__}")
    except Exception as e:
        logger.error(f"Error listing middleware: {str(e)}")

    logger.info(f"Starting server at http://{host}:{port}")
    logger.info(f"API docs available at http://{host}:{port}/docs")
    
    uvicorn.run("app.main:app", host=host, port=port, reload=reload, log_level="info")

if __name__ == "__main__":
    # 获取CPU核心数
    cpu_count = multiprocessing.cpu_count()
    # 设置worker数为CPU核心数(最多8个)
    workers = min(cpu_count, 8)
    
    # 启动带有多个worker的uvicorn
    uvicorn.run(
        "app.main:app", 
        host="0.0.0.0", 
        port=8070, 
        reload=False,  # 生产环境关闭reload
        workers=workers,
        log_level="info"
    )