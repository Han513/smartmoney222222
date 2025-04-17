import logging
import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.services.wallet_analyzer import wallet_analyzer
from app.api.router import router
from app.core.config import settings
from app.services.transaction_processor import transaction_processor
from app.services.kafka_consumer import kafka_consumer
from app.services.kafka_processor import message_processor

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# 定義應用生命週期管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    應用程式生命週期管理器，替代原來的 startup 和 shutdown 事件
    """
    # 啟動時執行的操作
    logger.info("Starting up the application...")
    logger.info(f"環境設定: {settings.model_config}")
    
    # 記錄資料庫設定
    logger.info(f"資料庫 URL: {settings.DATABASE_URL}")
    logger.info(f"資料庫功能: {'啟用' if settings.DB_ENABLED else '停用'}")
    
    # 初始化 transaction_processor
    try:
        logger.info("初始化交易處理器...")
        # 直接啟用 transaction_processor 並初始化資料庫連接
        activation_result = transaction_processor.activate()
        if activation_result:
            logger.info("交易處理器已成功啟用")
            if transaction_processor._connection_tested_successfully:
                logger.info("資料庫連接測試成功")
            else:
                logger.warning("資料庫連接測試失敗，但應用程式仍會繼續啟動")
        else:
            logger.error("交易處理器啟用失敗")
            
    except Exception as e:
        logger.error(f"初始化交易處理器時發生錯誤: {e}")
        logger.warning("初始化問題不會阻止應用啟動，但某些功能可能無法正常工作")
    
    # 初始化 Kafka 消費者
    try:
        logger.info("初始化 Kafka 消費者...")
        kafka_start_result = await kafka_consumer.start()
        if kafka_start_result:
            logger.info("Kafka 消費者服務已成功啟動")
        else:
            logger.warning("Kafka 消費者服務啟動失敗，但應用程式仍會繼續啟動")
    except Exception as e:
        logger.error(f"初始化 Kafka 消費者時發生錯誤: {e}")
        logger.warning("Kafka 消費者初始化問題不會阻止應用啟動，但即時交易更新功能可能無法正常工作")

    # 初始化消息處理器
    try:
        logger.info("初始化消息處理器...")
        await message_processor.start()
        logger.info("消息處理器已成功啟動")
    except Exception as e:
        logger.error(f"初始化消息處理器時發生錯誤: {e}")
        logger.warning("消息處理器初始化問題不會阻止應用啟動，但消息處理功能可能無法正常工作")
    
    # 日誌所有路由
    routes = []
    for r in router.routes:
        if hasattr(r, "methods"):
            methods = ", ".join(r.methods)
            routes.append(f"{methods} {settings.API_PREFIX}{r.path}")
        else:
            routes.append(f"PATH {settings.API_PREFIX}{r.path}")
    
    logger.info(f"API 路由總數: {len(routes)}")
    
    yield  # 應用運行中
    
    # 關閉時執行的操作
    logger.info("Shutting down the application...")
    
    # 先關閉消息處理器
    try:
        logger.info("正在關閉消息處理器...")
        await message_processor.stop()
        logger.info("消息處理器已成功關閉")
    except Exception as e:
        logger.error(f"關閉消息處理器時發生錯誤: {e}")
    
    # 關閉 Kafka 消費者
    try:
        logger.info("正在關閉 Kafka 消費者...")
        await kafka_consumer.stop()
        logger.info("Kafka 消費者已成功關閉")
    except Exception as e:
        logger.error(f"關閉 Kafka 消費者時發生錯誤: {e}")
    
    # 關閉 wallet_analyzer
    try:
        import asyncio
        from asyncio import wait_for
        
        logger.info("正在關閉 wallet_analyzer...")
        # 增加超時機制
        await asyncio.wait_for(wallet_analyzer.close(), timeout=3.0)
        logger.info("wallet_analyzer 已成功關閉")
    except asyncio.TimeoutError:
        logger.warning("關閉 wallet_analyzer 超時，繼續關閉其他資源")
    except Exception as e:
        logger.error(f"關閉 wallet_analyzer 時發生錯誤: {e}")

    # 關閉數據庫連接
    try:
        logger.info("正在關閉資料庫連接...")
        transaction_processor.close_db_connection()
        logger.info("數據庫連接已成功關閉")
    except Exception as e:
        logger.error(f"關閉數據庫連接時發生錯誤: {e}")

    try:
        logger.info("正在關閉 Celery 連接...")
        from app.workers.tasks import celery_app
        celery_app.control.purge()  # 清空任務隊列
        celery_app.close()  # 關閉連接
        logger.info("Celery 連接已成功關閉")
    except Exception as e:
        logger.error(f"關閉 Celery 連接時發生錯誤: {e}")

# 創建應用
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    lifespan=lifespan  # 使用新的生命週期管理器
)

# 添加 CORS 中間件 - 允許所有來源
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允許所有來源
    allow_credentials=True,
    allow_methods=["*"],  # 允許所有方法
    allow_headers=["*"],  # 允許所有標頭
)

# 請求記錄中間件
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # 簡化請求記錄，只記錄路徑
    path = request.url.path
    if path != "/health" and path != "/ping":  # 不記錄健康檢查和 ping 請求
        logger.info(f"Request: {request.method} {path}")
    
    # 處理請求
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # 只對非健康檢查路徑記錄響應時間
        if path != "/health" and path != "/ping":
            logger.info(f"Response: {response.status_code} (處理時間: {process_time:.4f}秒)")
        return response
    except Exception as e:
        logger.exception(f"Request failed: {str(e)}")
        raise
    finally:
        # 不再讀取請求體
        pass

# 測試端點
@app.get("/ping")
def ping():
    """
    簡單的測試端點，確認 API 服務器工作正常
    """
    logger.info("Ping endpoint called")
    return {"status": "ok", "message": "API is running"}

# 註冊路由
logger.info(f"Registering router with prefix: {settings.API_PREFIX}")
app.include_router(router, prefix=settings.API_PREFIX)

# 健康檢查端點
@app.get("/health")
def health_check():
    """
    健康檢查端點
    """
    return {"status": "healthy"}