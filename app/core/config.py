import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Optional
from functools import lru_cache
import logging

class Settings(BaseSettings):
    """
    系統配置類
    """
    # API 配置
    API_PREFIX: str = "/api"
    API_TITLE: str = "Wallet Analysis API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "批量分析錢包交易歷史和指標"
    
    # Redis 配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_URL: str = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    
    # Celery 配置
    CELERY_BROKER_URL: str = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"
    CELERY_RESULT_BACKEND: str = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"
    CELERY_TASK_SERIALIZER: str = "json"
    CELERY_RESULT_SERIALIZER: str = "json"
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]
    CELERY_TIMEZONE: str = "UTC"

    # Kafka 配置
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("127.0.0.1:8998", "localhost:8998")
    KAFKA_TOPIC: str = os.getenv("web3_trade_events", "web3_trade_events")
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "wallet_analyzer_group")
    
    # 安全配置 - 移除 API_KEYS
    SECRET_KEY: str = Field(default="your-secret-key-here")
    
    # CORS 配置
    CORS_ORIGINS: List[str] = ["*"]
    
    # 資料庫配置 - 使用您提供的 DATABASE_URI_Solana
    DATABASE_URL: str = "postgresql://postgres:henrywork8812601@localhost:5432/smartmoney"
    
    # 資料庫功能開關
    DB_ENABLED: bool = True
    
    SOLANA_RPC_URL: str = "https://methodical-capable-firefly.solana-mainnet.quiknode.pro/f660ad44a1d7512bb5f81c93144712e8ddc5c2dc"
    SOLANA_RPC_URL_BACKUP: str = "https://patient-fabled-needle.solana-mainnet.quiknode.pro/befd34a7534b2733f326b0df7cf2fb89b979cbb7/"

    # Solscan API 配置
    SOLSCAN_API_TOKEN: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NDQxODI3MTA0MzksImVtYWlsIjoid2lubmlmb3J3b3JrQGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc0NDE4MjcxMH0.T7ofb-SMx2PHAompUQhjBbCNyubwyAZ0IuOp71aMC_M"
    SOLSCAN_API_URL: str = "https://pro-api.solscan.io/v2.0"
    
    # 系統限制
    MAX_CONCURRENT_REQUESTS: int = 10
    MAX_ADDRESSES_PER_REQUEST: int = 300
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore"
    }
    
    # 使用 __getattribute__ 來處理 DATABASE_URI_Solana
    def __getattribute__(self, name):
        if name == 'DATABASE_URL':
            # 優先使用 DATABASE_URI_Solana
            import os
            logger = logging.getLogger(__name__)
            
            db_uri = os.getenv('DATABASE_URI_Solana')
            if db_uri:
                logger.info(f"使用環境變數 DATABASE_URI_Solana: {db_uri}")
                return db_uri
            
            # 回退到預設值
            default_url = super().__getattribute__(name)
            logger.info(f"未找到 DATABASE_URI_Solana 環境變數，使用預設 URL: {default_url}")
            return default_url
        
        return super().__getattribute__(name)

@lru_cache()
def get_settings() -> Settings:
    """
    獲取設置單例
    """
    return Settings()

# 導出單例
settings = get_settings()