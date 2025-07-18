import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Optional
from functools import lru_cache
import logging
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    """
    系統配置類
    """
    # API 配置
    API_PREFIX: str = "/api"
    API_TITLE: str = "Wallet Analysis API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "批量分析錢包交易歷史和指標"
    
    # 日誌配置
    LOG_LEVEL: str = Field(default="WARNING", description="日誌級別: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    
    # Redis 配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD", "")
    
    # 組裝 Redis 認證字串
    _REDIS_AUTH: str = f":{REDIS_PASSWORD}@" if REDIS_PASSWORD else ""

    # Redis 連線字串，優先使用環境變量 REDIS_URL
    REDIS_URL: str = os.getenv(
        "REDIS_URL",
        f"redis://{_REDIS_AUTH}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    )
    
    # Celery Broker & Backend，優先使用環境變量，如未指定則動態組裝
    CELERY_BROKER_URL: str = os.getenv(
        "CELERY_BROKER_URL",
        f"redis://{_REDIS_AUTH}{REDIS_HOST}:{REDIS_PORT}/1"
    )
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND",
        f"redis://{_REDIS_AUTH}{REDIS_HOST}:{REDIS_PORT}/1"
    )
    CELERY_TASK_SERIALIZER: str = "json"
    CELERY_RESULT_SERIALIZER: str = "json"
    CELERY_ACCEPT_CONTENT: List[str] = ["json"]
    CELERY_TIMEZONE: str = "UTC"

    # Kafka 配置
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-node2:9792")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPICS", "web3_trade_sm_events")
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "web3_trade_sm_events")
    
    # 安全配置 - 移除 API_KEYS
    SECRET_KEY: str = Field(default="your-secret-key-here")
    
    # CORS 配置
    CORS_ORIGINS: List[str] = ["*"]

    # 資料庫配置 - 使用您提供的 DATABASE_URI_Solana
    DATABASE_URL: str = os.getenv("DATABASE_URI_Solana", "DATABASE_URL_SYNC")
    
    # Ian 資料庫配置 - 用於查詢 trades 表
    DATABASE_URI_Ian: str = os.getenv("DATABASE_URI_Ian", "")
    
    # 資料庫功能開關
    DB_ENABLED: bool = True
   
    SOLANA_RPC_URL: str =  os.getenv("SOLANA_RPC_URL", "https://falling-damp-sound.solana-mainnet.quiknode.pro/2f4248baa9ee5737e1eff05d16dcd5b52d759adb/")
    SOLANA_RPC_URL_BACKUP: str = os.getenv("SOLANA_RPC_URL_BACKUP", "http://13.113.243.82:64001")

    # Solscan API 配置
    SOLSCAN_API_TOKEN: str = os.getenv("SOLSCAN_API_TOKEN", "1234567890")
    SOLSCAN_API_URL: str = os.getenv("SOLSCAN_API_URL", "https://pro-api.solscan.io/v2.0")

    WALLET_SYNC_API_ENDPOINT: str = os.getenv("WALLET_SYNC_API_ENDPOINT", "http://moonx.backend:4200/internal/sync_kol_wallets")

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

def setup_logging():
    """
    設置日誌配置
    """
    # 獲取日誌級別
    log_level = getattr(settings, 'LOG_LEVEL', 'WARNING').upper()
    
    # 設置根 logger 級別
    logging.basicConfig(
        level=getattr(logging, log_level, logging.WARNING),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 設置特定模組的 logger 級別
    logging.getLogger('app.api.router').setLevel(getattr(logging, log_level, logging.WARNING))
    logging.getLogger('app.services').setLevel(getattr(logging, log_level, logging.WARNING))
    logging.getLogger('app.workers').setLevel(getattr(logging, log_level, logging.WARNING))
    
    # 設置第三方庫的日誌級別為 WARNING 或更高
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)