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
    REDIS_URL: str = os.getenv("REDIS_URL", "")
    
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
    
    # SelectDB（MySQL 相容）配置 - 用於查詢 trades 表
    SELECTDB_URI: str = os.getenv("SELECTDB_URI", "")

    # SelectDB 環境變數（推薦使用）—— 若未提供 SELECTDB_URI 時用這些組裝
    SELECTDB_HOST: str = os.getenv("SELECTDB_HOST", os.getenv("DB_HOST", "127.0.0.1"))
    SELECTDB_PORT: str = os.getenv("SELECTDB_PORT", os.getenv("DB_PORT", "5012"))
    SELECTDB_USER: str = os.getenv("SELECTDB_USER", os.getenv("DB_USER", "admin"))
    SELECTDB_PASSWORD: str = os.getenv("SELECTDB_PASSWORD", os.getenv("DB_PASSWORD", "Ro5w214Cd79b4D"))
    SELECTDB_NAME: str = os.getenv("SELECTDB_NAME", os.getenv("DB_NAME", "smart_money"))
    
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
        
        if name == 'SELECTDB_URI':
            # 優先使用顯式設定的 SELECTDB_URI
            import os
            from urllib.parse import quote_plus
            logger = logging.getLogger(__name__)
            env_url = os.getenv('SELECTDB_URI')
            if env_url:
                logger.info(f"使用環境變數 SELECTDB_URI: {env_url}")
                return env_url
            # 若未提供，依照 .env 的 DB_* 參數自動組裝
            # 先用 SELECTDB_*，若缺失再回退 DB_*
            host = os.getenv('SELECTDB_HOST', self.SELECTDB_HOST)
            port = os.getenv('SELECTDB_PORT', self.SELECTDB_PORT)
            user = os.getenv('SELECTDB_USER', self.SELECTDB_USER)
            password = os.getenv('SELECTDB_PASSWORD', self.SELECTDB_PASSWORD)
            dbname = os.getenv('SELECTDB_NAME', self.SELECTDB_NAME)
            user_enc = quote_plus(user)
            pwd_enc = quote_plus(password)
            assembled = f"mysql+pymysql://{user_enc}:{pwd_enc}@{host}:{port}/{dbname}"
            logger.info(f"未提供 SELECTDB_URI，使用 SELECTDB_* / DB_* 組裝: {assembled}")
            return assembled
        
        if name == 'REDIS_URL':
            # 優先使用環境變量 REDIS_URL；若未提供，依當前設定動態組裝
            import os
            from urllib.parse import quote_plus
            logger = logging.getLogger(__name__)
            env_url = os.getenv('REDIS_URL')
            if env_url:
                logger.info(f"使用環境變數 REDIS_URL: {env_url}")
                return env_url

            host = os.getenv('REDIS_HOST', self.REDIS_HOST)
            port = os.getenv('REDIS_PORT', str(self.REDIS_PORT))
            db = os.getenv('REDIS_DB', str(self.REDIS_DB))
            password = os.getenv('REDIS_PASSWORD', self.REDIS_PASSWORD or "")
            is_cluster = os.getenv('REDIS_CLUSTER', 'false').lower() in ('1', 'true', 'yes')

            # URL 需要對密碼做 URL encode
            pwd_enc = quote_plus(password) if password else ""
            auth = f":{pwd_enc}@" if pwd_enc else ""

            assembled = f"redis://{auth}{host}:{port}"
            if not is_cluster:
                assembled = f"{assembled}/{db}"
            else:
                if db not in (None, '', '0', 0):
                    logger.warning("Redis cluster 模式不支援多 DB，已忽略 REDIS_DB 設定")

            logger.info(f"未提供 REDIS_URL，使用當前設定組裝: {assembled}")
            return assembled
        
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