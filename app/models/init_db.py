import asyncio
import logging
import sys
import os
import traceback
from sqlalchemy import text

# 添加項目根目錄到系統路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.config import settings
from sqlalchemy.ext.asyncio import create_async_engine

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

async def init_database():
    """
    初始化資料庫結構（僅需執行一次）
    """
    logger.info("開始初始化資料庫...")
    logger.info(f"使用資料庫連接 URL: {settings.DATABASE_URL}")
    
    try:
        # 創建引擎並測試連接
        engine = create_async_engine(
            settings.DATABASE_URL,
            echo=False,
            future=True
        )
        
        # 測試連接
        logger.info("測試資料庫連接...")
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logger.info("數據庫連接成功")
            else:
                logger.error("數據庫連接失敗")
                return
        
        # 檢查 schema 是否存在
        logger.info("檢查 solana schema 是否存在...")
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'solana'"))
            schema_exists = result.scalar()
            
            if not schema_exists:
                logger.info("solana schema 不存在，創建中...")
                async with engine.begin() as conn:
                    await conn.execute(text("CREATE SCHEMA IF NOT EXISTS solana"))
                logger.info("已創建 solana schema")
            else:
                logger.info("solana schema 已存在")
        
        # 創建缺失的表
        logger.info("創建 transaction_records 表（如果不存在）...")
        
        # 檢查表是否存在
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'solana' AND table_name = 'transaction_records')"))
            table_exists = result.scalar()
            
            if not table_exists:
                logger.info("transaction_records 表不存在，創建中...")
                # 使用直接SQL創建表
                create_table_sql = """
                CREATE TABLE solana.transaction_records (
                    id SERIAL PRIMARY KEY,
                    wallet_address VARCHAR(100) NOT NULL,
                    transaction_hash VARCHAR(100) NOT NULL,
                    token_address VARCHAR(100) NOT NULL,
                    activity_type VARCHAR(20) NOT NULL,
                    amount FLOAT NOT NULL,
                    cost FLOAT,
                    price FLOAT,
                    timestamp TIMESTAMP NOT NULL,
                    block_number BIGINT,
                    from_address VARCHAR(100),
                    to_address VARCHAR(100),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
                async with engine.begin() as conn:
                    await conn.execute(text(create_table_sql))
                logger.info("transaction_records 表創建成功")
                
                # 添加索引
                logger.info("創建索引...")
                index_sql = [
                    "CREATE INDEX idx_tx_hash_wallet ON solana.transaction_records(transaction_hash, wallet_address)",
                    "CREATE INDEX idx_wallet_token_rec ON solana.transaction_records(wallet_address, token_address)"
                ]
                async with engine.begin() as conn:
                    for sql in index_sql:
                        try:
                            await conn.execute(text(sql))
                            logger.info(f"執行成功: {sql[:50]}...")
                        except Exception as e:
                            logger.error(f"創建索引失敗: {e}")
                
                logger.info("索引創建完成")
            else:
                logger.info("transaction_records 表已存在")
        
        # 檢查 dex_query_v1 schema 是否存在
        logger.info("檢查 dex_query_v1 schema 是否存在...")
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dex_query_v1'"))
            schema_exists = result.scalar()
            
            if not schema_exists:
                logger.info("dex_query_v1 schema 不存在，創建中...")
                async with engine.begin() as conn:
                    await conn.execute(text("CREATE SCHEMA IF NOT EXISTS dex_query_v1"))
                logger.info("已創建 dex_query_v1 schema")
            else:
                logger.info("dex_query_v1 schema 已存在")
        
        # 創建 wallet_token_state 表
        logger.info("檢查 wallet_token_state 表是否存在...")
        async with engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dex_query_v1' AND table_name = 'wallet_token_state')"
            ))
            table_exists = result.scalar()
            
            if not table_exists:
                logger.info("wallet_token_state 表不存在，創建中...")
                create_table_sql = """
                CREATE TABLE dex_query_v1.wallet_token_state (
                    id SERIAL PRIMARY KEY,
                    wallet_address VARCHAR(100) NOT NULL,
                    token_address VARCHAR(100) NOT NULL,
                    chain VARCHAR(50) NOT NULL,
                    chain_id INTEGER NOT NULL,
                    current_amount FLOAT NOT NULL DEFAULT 0.0,
                    current_total_cost FLOAT NOT NULL DEFAULT 0.0,
                    current_avg_buy_price FLOAT NOT NULL DEFAULT 0.0,
                    position_opened_at BIGINT,
                    historical_buy_amount FLOAT NOT NULL DEFAULT 0.0,
                    historical_sell_amount FLOAT NOT NULL DEFAULT 0.0,
                    historical_buy_cost FLOAT NOT NULL DEFAULT 0.0,
                    historical_sell_value FLOAT NOT NULL DEFAULT 0.0,
                    historical_realized_pnl FLOAT NOT NULL DEFAULT 0.0,
                    historical_buy_count INTEGER NOT NULL DEFAULT 0,
                    historical_sell_count INTEGER NOT NULL DEFAULT 0,
                    last_transaction_time BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_wallet_token_chain UNIQUE (wallet_address, token_address, chain)
                )
                """
                async with engine.begin() as conn:
                    await conn.execute(text(create_table_sql))
                logger.info("wallet_token_state 表創建成功")
                
                # 添加索引
                logger.info("創建 wallet_token_state 表的索引...")
                index_sql = [
                    "CREATE INDEX idx_wallet_token_state_wallet ON dex_query_v1.wallet_token_state(wallet_address)",
                    "CREATE INDEX idx_wallet_token_state_token ON dex_query_v1.wallet_token_state(token_address)",
                    "CREATE INDEX idx_wallet_token_state_chain ON dex_query_v1.wallet_token_state(chain)"
                ]
                async with engine.begin() as conn:
                    for sql in index_sql:
                        try:
                            await conn.execute(text(sql))
                            logger.info(f"執行成功: {sql[:50]}...")
                        except Exception as e:
                            logger.error(f"創建索引失敗: {e}")
                
                logger.info("wallet_token_state 表索引創建完成")
            else:
                logger.info("wallet_token_state 表已存在")
        
        logger.info("資料庫初始化成功！")
        await engine.dispose()
    except Exception as e:
        logger.error(f"資料庫初始化過程中發生錯誤: {e}")
        logger.error(traceback.format_exc())
        return False
        
    return True

if __name__ == "__main__":
    logger.info("執行資料庫初始化腳本...")
    asyncio.run(init_database())
    logger.info("初始化腳本執行完畢") 