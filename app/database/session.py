from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

# 創建異步數據庫URL
# 假設同步URL格式為 "postgresql://user:password@host/dbname"
# 異步URL格式為 "postgresql+asyncpg://user:password@host/dbname"
if settings.DATABASE_URL.startswith("postgresql://"):
    ASYNC_DATABASE_URL = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    logger.info(f"自動生成異步數據庫URL: {ASYNC_DATABASE_URL}")
else:
    logger.warning("無法自動從同步URL生成異步URL，將直接使用同步URL。請確保您的數據庫URL是兼容異步的。")
    ASYNC_DATABASE_URL = settings.DATABASE_URL


# 創建同步引擎
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600
)

# 創建異步引擎
async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600
)


# 創建同步會話工廠
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 創建異步會話工廠
async_session = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# 創建基礎模型
Base = declarative_base()

def get_db_session() -> Session:
    """
    獲取資料庫會話，用於依賴注入
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()