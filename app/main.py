import os
import logging
import time
import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.services.wallet_analyzer import wallet_analyzer
from app.api.router import router
from app.core.config import settings, setup_logging
from app.services.transaction_processor import transaction_processor
from app.services.kafka_consumer import kafka_consumer
from app.services.kafka_processor import message_processor
from app.services.wallet_sync_service import wallet_sync_service
from app.services.wallet_cache_service import wallet_cache_service

# 设置日志配置
setup_logging()

os.makedirs("app/logs", exist_ok=True)

# 为 API 服务创建专门的 logger
api_logger = logging.getLogger("api")
api_logger.setLevel(logging.INFO)

# 清除现有的 handlers
for handler in api_logger.handlers[:]:
    api_logger.removeHandler(handler)

# 添加文件 handler
file_handler = logging.FileHandler("app/logs/api.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
api_logger.addHandler(file_handler)

# 添加控制台 handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
api_logger.addHandler(console_handler)

# 防止日志向上传播
api_logger.propagate = False

logger = api_logger


# 定义应用生命周期管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用程序生命周期管理器，替代原来的 startup 和 shutdown 事件
    """
    # 启动时执行的操作
    logger.info("Starting up the application...")
    logger.info(f"环境设定: {settings.model_config}")
    
    # 记录数据库设定
    logger.info(f"数据库 URL: {settings.DATABASE_URL}")
    logger.info(f"数据库功能: {'启用' if settings.DB_ENABLED else '停用'}")
    
    # 初始化 transaction_processor
    try:
        logger.info("初始化交易处理器...")
        # 直接启用 transaction_processor 并初始化数据库连接
        activation_result = transaction_processor.activate()
        if activation_result:
            logger.info("交易处理器已成功启用")
            if transaction_processor._connection_tested_successfully:
                logger.info("数据库连接测试成功")
            else:
                logger.warning("数据库连接测试失败，但应用程序仍会继续启动")
        else:
            logger.error("交易处理器启用失败")
            
    except Exception as e:
        logger.error(f"初始化交易处理器时发生错误: {e}")
        logger.warning("初始化问题不会阻止应用启动，但某些功能可能无法正常工作")
    
    # 初始化钱包缓存服务
    try:
        logger.info("初始化钱包缓存服务...")
        await wallet_cache_service.initialize()
        logger.info("钱包缓存服务已成功初始化")
    except Exception as e:
        logger.error(f"初始化钱包缓存服务时发生错误: {e}")
        logger.warning("钱包缓存服务初始化问题不会阻止应用启动，但消息处理功能可能无法正常工作")

    # 添加启动延迟，确保 Redis 连接稳定
    logger.info("等待 15 秒以确保 Redis 连接稳定...")
    await asyncio.sleep(15)
    logger.info("启动延迟完成，开始启动后台处理任务")

    # 初始化 Kafka 消费者
    try:
        logger.info("初始化 Kafka 消费者...")
        kafka_start_result = await kafka_consumer.start()
        if kafka_start_result:
            logger.info("Kafka 消费者服务已成功启动")
        else:
            logger.warning("Kafka 消费者服务启动失败，但应用程序仍会继续启动")
    except Exception as e:
        logger.error(f"初始化 Kafka 消费者时发生错误: {e}")
        logger.warning("Kafka 消费者初始化问题不会阻止应用启动，但即时交易更新功能可能无法正常工作")

    # 初始化消息处理器
    try:
        logger.info("初始化消息处理器...")
        await message_processor.start()
        logger.info("消息处理器已成功启动")
    except Exception as e:
        logger.error(f"初始化消息处理器时发生错误: {e}")
        logger.warning("消息处理器初始化问题不会阻止应用启动，但消息处理功能可能无法正常工作")

    # 延迟启动钱包同步服务，避免与消息处理器竞争资源
    try:
        logger.info("延迟 30 秒后启动钱包同步服务...")
        await asyncio.sleep(30)
        logger.info("启动钱包同步服务...")
        await wallet_sync_service.start()
        logger.info("钱包同步服务已启动")
    except Exception as e:
        logger.error(f"启动钱包同步服务时发生错误: {e}")
        logger.warning("钱包同步服务启动问题不会阻止应用启动，但钱包数据同步功能可能无法正常工作")
    
    # 日志所有路由
    routes = []
    for r in router.routes:
        if hasattr(r, "methods"):
            methods = ", ".join(r.methods)
            routes.append(f"{methods} {settings.API_PREFIX}{r.path}")
        else:
            routes.append(f"PATH {settings.API_PREFIX}{r.path}")
    
    logger.info(f"API 路由总数: {len(routes)}")
    
    yield  # 应用运行中
    
    # 关闭时执行的操作
    logger.info("Shutting down the application...")
    
    # 先关闭消息处理器
    try:
        logger.info("正在关闭消息处理器...")
        await message_processor.stop()
        logger.info("消息处理器已成功关闭")
    except Exception as e:
        logger.error(f"关闭消息处理器时发生错误: {e}")
    
    # 关闭 Kafka 消费者
    try:
        logger.info("正在关闭 Kafka 消费者...")
        await kafka_consumer.stop()
        logger.info("Kafka 消费者已成功关闭")
    except Exception as e:
        logger.error(f"关闭 Kafka 消费者时发生错误: {e}")
    
    # 关闭 wallet_analyzer
    try:
        from asyncio import wait_for
        
        logger.info("正在关闭 wallet_analyzer...")
        # 增加超时机制
        await wait_for(wallet_analyzer.close(), timeout=3.0)
        logger.info("wallet_analyzer 已成功关闭")
    except asyncio.TimeoutError:
        logger.warning("关闭 wallet_analyzer 超时，继续关闭其他资源")
    except Exception as e:
        logger.error(f"关闭 wallet_analyzer 时发生错误: {e}")

    # 关闭数据库连接
    try:
        logger.info("正在关闭数据库连接...")
        transaction_processor.close_db_connection()
        logger.info("数据库连接已成功关闭")
    except Exception as e:
        logger.error(f"关闭数据库连接时发生错误: {e}")

    try:
        logger.info("正在关闭 Celery 连接...")
        from app.workers.tasks import celery_app
        celery_app.control.purge()  # 清空任务队列
        celery_app.close()  # 关闭连接
        logger.info("Celery 连接已成功关闭")
    except Exception as e:
        logger.error(f"关闭 Celery 连接时发生错误: {e}")

    try:
        logger.info("停止钱包同步服务...")
        await wallet_sync_service.stop()
        logger.info("钱包同步服务已停止")
    except Exception as e:
        logger.error(f"停止钱包同步服务时发生错误: {e}")

def create_app() -> FastAPI:
    """创建 FastAPI 应用实例"""
    app = FastAPI(
        title=settings.API_TITLE,
        description=settings.API_DESCRIPTION,
        version=settings.API_VERSION,
        lifespan=lifespan  # 使用新的生命周期管理器
    )

    # 添加 CORS 中间件 - 允许所有来源
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # 允许所有来源
        allow_credentials=True,
        allow_methods=["*"],  # 允许所有方法
        allow_headers=["*"],  # 允许所有标头
    )

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start_time = time.time()
        
        # 简化请求记录，只记录路径
        path = request.url.path
        if path != "/health" and path != "/ping":  # 不记录健康检查和 ping 请求
            logger.info(f"Request: {request.method} {path}")
        
        # 处理请求
        try:
            response = await call_next(request)
            process_time = time.time() - start_time
            
            # 只对非健康检查路径记录响应时间
            if path != "/health" and path != "/ping":
                logger.info(f"Response: {response.status_code} (处理时间: {process_time:.4f}秒)")
            return response
        except Exception as e:
            logger.exception(f"Request failed: {str(e)}")
            raise
        finally:
            # 不再读取请求体
            pass

    # 测试端点
    @app.get("/ping")
    def ping():
        """
        简单的测试端点，确认 API 服务器工作正常
        """
        logger.info("Ping endpoint called")
        return {"status": "ok", "message": "API is running"}

    # 注册路由
    logger.info(f"Registering router with prefix: {settings.API_PREFIX}")
    app.include_router(router, prefix=settings.API_PREFIX)

    # 健康检查端点
    @app.get("/health")
    def health_check():
        """
        健康检查端点
        """
        return {"status": "healthy"}

    return app

# 创建应用实例
app = create_app()