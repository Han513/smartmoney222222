# app/core/db.py

import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from app.core.config import settings

logger = logging.getLogger(__name__)

def get_session_factory():
    """
    獲取數據庫會話工廠
    
    Returns:
        sessionmaker: SQLAlchemy 會話工廠
    """
    # 優先使用 DATABASE_URL_SYNC 環境變數
    db_url = os.getenv('DATABASE_URL_SYNC')
    
    # 如果 DATABASE_URL_SYNC 未設定，則使用 settings.DATABASE_URL 並轉換
    if not db_url:
        db_url = settings.DATABASE_URL
        if not db_url:
            logger.error("資料庫 URL 未設定，無法建立連線")
            raise ValueError("資料庫 URL 未設定")
            
        if db_url.startswith('postgresql+asyncpg://'):
            db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://')
            logger.info(f"轉換異步數據庫 URL 為同步 URL: {db_url}")
    else:
        logger.info(f"使用環境變數 DATABASE_URL_SYNC: {db_url}")
    
    # 檢查 URL 格式
    if not db_url or not isinstance(db_url, str):
        logger.error(f"資料庫 URL 無效: {db_url}")
        raise ValueError(f"資料庫 URL 無效: {db_url}")
    
    if not (db_url.startswith('postgresql://') or db_url.startswith('mysql://') or db_url.startswith('sqlite:///')):
        logger.error(f"資料庫 URL 格式無效，需要以 postgresql:// 或其他支援的資料庫前綴開始: {db_url}")
        raise ValueError(f"資料庫 URL 格式無效: {db_url}")
    
    logger.info(f"使用資料庫 URL: {db_url}")
    
    # 增强设置，加大连接池和超时时间
    engine = create_engine(
        db_url,
        echo=False,
        future=True,
        pool_size=10,           # 增加连接池大小
        max_overflow=20,        # 增加最大溢出连接数
        pool_timeout=60,        # 增加获取连接超时时间
        pool_recycle=1800,      # 每30分钟回收连接
        pool_pre_ping=True,     # 使用前测试连接活跃性
        connect_args={'options': '-csearch_path=dex_query_v1,public'}
    )
    
    session_factory = sessionmaker(
        bind=engine,
        expire_on_commit=False
    )
    
    return session_factory