import logging
import sys
import os
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# 確保可以引入其他模組
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.core.config import settings
from app.models.models import Base

logger = logging.getLogger(__name__)

async def check_missing_tables():
    """檢查缺失的表並輸出創建腳本"""
    logger.info("檢查缺失的表...")
    
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        future=True
    )
    
    try:
        # 獲取solana schema中的所有表
        async with engine.connect() as conn:
            result = await conn.execute(text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'solana'"
            ))
            existing_tables = [row[0] for row in result.fetchall()]
            logger.info(f"現有表: {existing_tables}")
        
        # 獲取模型定義的所有表
        model_tables = [
            table.name for table in Base.metadata.tables.values() 
            if table.schema == 'solana'
        ]
        logger.info(f"模型定義的表: {model_tables}")
        
        # 找出缺失的表
        missing_tables = [
            table for table in model_tables 
            if table.lower() not in [t.lower() for t in existing_tables]
        ]
        
        if missing_tables:
            logger.info(f"缺失的表: {missing_tables}")
            
            # 為每個缺失的表生成SQL並創建
            for table_name in missing_tables:
                table = next((t for t in Base.metadata.tables.values() 
                              if t.schema == 'solana' and t.name == table_name), None)
                if table:
                    try:
                        logger.info(f"準備創建表: {table_name}")
                        # 取得CREATE TABLE語句
                        create_stmt = table.compile(dialect=engine.dialect)
                        create_sql = str(create_stmt).replace('\n', ' ')
                        logger.info(f"創建表SQL: {create_sql}")
                        
                        # 直接執行SQL語句創建表
                        async with engine.begin() as conn:
                            await conn.execute(text(create_sql))
                            
                        logger.info(f"成功創建表: {table_name}")
                    except Exception as e:
                        logger.error(f"創建表 {table_name} 時出錯: {e}")
                        
                        # 嘗試使用更直接的方法創建
                        try:
                            logger.info(f"嘗試使用替代方法創建表: {table_name}")
                            # 簡單的創建表SQL
                            if table_name == 'transaction_records':
                                # 為transaction_records表提供特定SQL
                                await create_transaction_records_table(engine)
                            else:
                                # 對其他表使用標準方法
                                async with engine.begin() as conn:
                                    await conn.run_sync(lambda sync_conn: table.create(sync_conn))
                            logger.info(f"成功使用替代方法創建表: {table_name}")
                        except Exception as e2:
                            logger.error(f"使用替代方法創建表 {table_name} 時也失敗: {e2}")
        else:
            logger.info("沒有缺失的表")
    except Exception as e:
        logger.error(f"檢查缺失表時發生錯誤: {e}")
    finally:
        await engine.dispose()

async def create_transaction_records_table(engine):
    """使用直接SQL創建transaction_records表"""
    create_sql = """
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
    );
    
    CREATE INDEX idx_tx_hash_wallet ON solana.transaction_records(transaction_hash, wallet_address);
    CREATE INDEX idx_wallet_token_rec ON solana.transaction_records(wallet_address, token_address);
    """
    
    async with engine.begin() as conn:
        # 確保一行一行執行，不讓一個錯誤影響其他語句
        sql_statements = [stmt.strip() for stmt in create_sql.split(';') if stmt.strip()]
        for stmt in sql_statements:
            try:
                await conn.execute(text(stmt))
                logger.info(f"執行SQL: {stmt[:50]}...")
            except Exception as e:
                logger.error(f"執行SQL失敗: {stmt[:50]}..., 錯誤: {e}")
                # 繼續執行其他語句
    
    logger.info("transaction_records表創建完成")

async def check_table_columns(table_name):
    """檢查表的列是否與模型定義一致"""
    logger.info(f"檢查表 {table_name} 的列...")
    
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        future=True
    )
    
    try:
        async with engine.begin() as conn:
            # 獲取表的現有列
            result = await conn.execute(text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'solana' AND table_name = '{table_name}'
            """))
            
            existing_columns = {row[0]: (row[1], row[2]) for row in result.fetchall()}
            logger.info(f"表 {table_name} 的現有列: {existing_columns.keys()}")
            
            # 獲取模型定義的列
            table = next((t for t in Base.metadata.tables.values() 
                          if t.schema == 'solana' and t.name == table_name), None)
            
            if not table:
                logger.error(f"找不到表 {table_name} 的模型定義")
                return
                
            model_columns = {col.name: (col.type, col.nullable) for col in table.columns}
            logger.info(f"模型定義的列: {model_columns.keys()}")
            
            # 找出缺失的列
            missing_columns = set(model_columns.keys()) - set(existing_columns.keys())
            
            if missing_columns:
                logger.info(f"缺失的列: {missing_columns}")
                for col_name in missing_columns:
                    logger.info(f"需要添加列: {col_name}")
            else:
                logger.info("表的列與模型定義一致")
    finally:
        await engine.dispose()

async def verify_db_setup():
    """驗證數據庫設置"""
    logger.info("驗證數據庫設置...")
    
    # 檢查資料庫連接
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        future=True
    )
    
    try:
        async with engine.connect() as conn:
            # 檢查數據庫連接
            result = await conn.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logger.info("數據庫連接成功")
            else:
                logger.error("數據庫連接失敗")
                return False
            
            # 檢查 schema 是否存在
            result = await conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'solana'"
            ))
            if result.scalar():
                logger.info("solana schema 存在")
            else:
                logger.error("solana schema 不存在")
                return False
            
            # 檢查缺失的表
            await check_missing_tables()
            
            # 檢查 transaction_records 表的列
            await check_table_columns('transaction_records')
            
            logger.info("驗證完成")
            return True
    except Exception as e:
        logger.error(f"驗證過程中發生錯誤: {e}")
        return False
    finally:
        await engine.dispose()

if __name__ == "__main__":
    # 配置日誌
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    logger.info("執行數據庫工具...")
    asyncio.run(verify_db_setup())
    logger.info("數據庫工具執行完畢") 