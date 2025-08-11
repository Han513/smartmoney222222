import os
import re
import time
import asyncio
import base58
import logging
import requests
import sqlalchemy
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import create_engine, select, update, and_, exists, text, func, delete, or_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.dialects.postgresql import insert 
from app.core.config import settings
from app.models.models import WalletSummary, Transaction, Holding, TokenBuyData, WalletTokenState
from app.services.token_repository import token_repository
from app.services.solscan import solscan_client
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from app.services.cache_service import cache_service
from app.services.wallet_summary_service import wallet_summary_service
import aiohttp
import pandas as pd
import traceback
import hashlib
import json

# 設置 Decimal 精度
getcontext().prec = 28
tz_utc8 = timezone(timedelta(hours=8))

# 移除重複的 logging.basicConfig，使用 run.py 中已配置的 logger
logger = logging.getLogger(__name__)

def now_utc8():
    return datetime.now(tz_utc8)

class TransactionProcessor:
    """
    交易處理器 - 負責創建和處理交易記錄
    """
    
    def __init__(self):
        self.engine = None
        self.session_factory = None
        self.db_enabled = settings.DB_ENABLED
        self._activated = False
        self._connection_tested_successfully = False
        logger.info("TransactionProcessor 實例已創建")
        logger.info(f"資料庫功能狀態: {'啟用' if self.db_enabled else '停用'}")
        logger.info("引擎和工廠將在首次需要時創建")
        logger.info(f"資料庫設定來源: {settings.DATABASE_URL}")
        
        # 記錄環境變數
        # 檢查有哪些環境變數
        import os
        logger.info("檢查環境變數...")
        db_uri_solana = os.getenv('DATABASE_URI_Solana', '未設定')
        db_url_sync = os.getenv('DATABASE_URL_SYNC', '未設定')
        db_url_from_settings = settings.DATABASE_URL
        all_env_keys = [k for k in os.environ.keys() if 'DATABASE' in k]
        
        logger.info(f"環境變數 DATABASE_URI_Solana: {db_uri_solana}")
        logger.info(f"環境變數 DATABASE_URL_SYNC: {db_url_sync}")
        logger.info(f"從 settings 獲取的 DATABASE_URL: {db_url_from_settings}")
        logger.info(f"所有與DATABASE相關的環境變數: {all_env_keys}")
        
        # Ian 資料庫連接（用於查詢 trades 表）
        self.ian_engine = None
        self.ian_session_factory = None
        self._ian_connection_tested_successfully = False
        db_uri_ian = os.getenv('DATABASE_URI_Ian', '未設定')
        logger.info(f"環境變數 DATABASE_URI_Ian: {db_uri_ian}")
        
        self.token_info_cache = {
        "So11111111111111111111111111111111111111112": {
            "symbol": "SOL",
            "name": "Solana",
            "decimals": 9,
            "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
        },
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
            "symbol": "USDC",
            "name": "USD Coin",
            "decimals": 6,
            "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
        },
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
            "symbol": "USDT",
            "name": "Tether",
            "decimals": 6,
            "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB/logo.svg"
        }
    }
        self.event_queue_key = "smart_token_events_queue"
        self.BACKEND_HOST = os.getenv('BACKEND_HOST', "http://172.25.183.205")
        self.BACKEND_PORT = os.getenv('BACKEND_PORT', "4200")
        self.api_endpoint = f"http://{self.BACKEND_HOST}:{self.BACKEND_PORT}/internal/smart_token_event"
        logger.info(f"Using API endpoint: {self.api_endpoint}")
        self.event_batch_size = 50 
        self.event_process_interval = 5.0
        self.event_processor_task = None
        self.running = False
        self.pending_events = []
        self.last_send_time = time.time()
        # 新增事件推送去重緩存
        self._event_push_cache = {}  # key: event_key, value: last_push_time
        self._event_push_cache_expiry = 30  # 秒
        self._last_cache_cleanup = time.time()  # 上次清理時間
        self._cache_cleanup_interval = 300
        
        # 添加調試統計
        self._event_stats = {
            'total_added': 0,
            'total_sent': 0,
            'total_duplicates': 0,
            'last_reset_time': time.time()
        }  # 每5分鐘清理一次緩存
        
        # 添加穩定幣和 WSOL 地址常量
        self.STABLES = {
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  # USDC
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',  # USDT
            '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',  # RAY
            'So11111111111111111111111111111111111111112',   # WSOL
            'So11111111111111111111111111111111111111111',   # SOL
        }
        self.WSOL = 'So11111111111111111111111111111111111111112'
        self.SOL = 'So11111111111111111111111111111111111111111'
    
        self.token_buy_cache = {}  # 緩存字典
        self.cache_update_threshold = 100  # 緩存更新閾值
        self.cache_update_interval = 300  # 緩存更新間隔（秒）
        self.last_cache_update = time.time()  # 上次緩存更新時間
        self.cache_lock = None  # 緩存鎖 - 延遲初始化
        self.pending_updates = 0  # 待更新的交易數量
        self.processed_tokens = set()  # 已處理過的代幣集合
    
        # WalletTokenState 緩存機制
        self.wallet_token_state_cache = {}  # 錢包代幣狀態緩存
        self.state_cache_lock = None  # 狀態緩存鎖 - 延遲初始化
        self.state_cache_update_threshold = 50  # 狀態緩存更新閾值
        
        # 添加交易處理去重緩存
        self._processed_transactions = set()
        self._transaction_cache_expiry = 3600  # 1小時
        self._last_transaction_cleanup = time.time()
        self.state_cache_update_interval = 180  # 狀態緩存更新間隔（秒）
        self.last_state_cache_update = time.time()  # 上次狀態緩存更新時間
        self.pending_state_updates = 0  # 待更新的狀態數量
        self.max_signature_cache_size = 2000
        self._pending_lock = None  # 延遲初始化
    
    def activate(self):
        """激活交易處理器，啟動事件處理循環"""
        if self._activated:
            logger.info("TransactionProcessor 已經被激活")
            return True
        
        logger.info("激活 TransactionProcessor...")
        self._activated = True
        self._init_db_connection()
        
        # 初始化 Ian 資料庫連接
        self._init_ian_db_connection()
        
        # 初始化異步鎖 - 在事件循環中延遲初始化
        self._init_async_locks()
        
        # 加載 WalletTokenState 數據到緩存
        try:
            # 獲取當前事件循環，如果不存在則跳過
            loop = asyncio.get_running_loop()
            if loop and not loop.is_closed():
                asyncio.create_task(self._load_wallet_token_states_to_cache())
        except RuntimeError:
            # 沒有運行中的事件循環，跳過異步任務創建
            pass
        
        # 啟動事件處理循環
        self.running = True
        # self.event_processor_task = asyncio.create_task(self._process_events_loop())
        
        if not self.db_enabled:
            logger.warning("数据库功能未启用")
        return True

    def _init_async_locks(self):
        """初始化異步鎖，確保它們在正確的事件循環中創建"""
        try:
            # 獲取當前事件循環，如果不存在則跳過
            loop = asyncio.get_running_loop()
            if loop and not loop.is_closed():
                if self.cache_lock is None:
                    self.cache_lock = asyncio.Lock()
                if self.state_cache_lock is None:
                    self.state_cache_lock = asyncio.Lock()
                if self._pending_lock is None:
                    self._pending_lock = asyncio.Lock()
                logger.info("異步鎖已初始化")
        except RuntimeError:
            # 沒有運行中的事件循環，鎖將在使用時動態創建
            logger.info("沒有運行中的事件循環，異步鎖將在使用時動態創建")

    def _ensure_async_locks(self):
        """確保異步鎖已初始化"""
        try:
            loop = asyncio.get_running_loop()
            if loop and not loop.is_closed():
                if self.cache_lock is None:
                    self.cache_lock = asyncio.Lock()
                if self.state_cache_lock is None:
                    self.state_cache_lock = asyncio.Lock()
                if self._pending_lock is None:
                    self._pending_lock = asyncio.Lock()
        except RuntimeError:
            # 沒有運行中的事件循環，無法創建鎖
            pass

    async def stop(self):
        """停止事件處理循環"""
        self.running = False
        if self.event_processor_task:
            self.event_processor_task.cancel()
            try:
                await self.event_processor_task
            except asyncio.CancelledError:
                pass
        # 發送剩餘的事件
        if self.pending_events:
            await self._send_events_batch(self.pending_events)
            self.pending_events = []
        # 服務關閉時推送剩餘事件
        # 確保異步鎖已初始化
        self._ensure_async_locks()
        
        if self._pending_lock is not None:
            async with self._pending_lock:
                if self.pending_events:
                    logger.info(f"服務關閉，推送剩餘 {len(self.pending_events)} 個事件")
                    await self._send_events_batch(self.pending_events)
                    self.pending_events.clear()
        else:
            # 如果沒有鎖，直接清空事件（避免阻塞）
            if self.pending_events:
                logger.warning("沒有事件循環，直接清空剩餘事件")
                self.pending_events.clear()

    # ======= 以下為已註解的定時推送相關邏輯 =======
    # async def _process_events_loop(self):
    #     """事件處理循環 - 每X秒批量發送事件到 API"""
    #     try:
    #         while self.running:
    #             try:
    #                 # 每X秒檢查一次隊列
    #                 await asyncio.sleep(self.event_process_interval)
    #                 # ...原有批量推送邏輯...
    #             except asyncio.CancelledError:
    #                 raise
    #             except Exception as e:
    #                 logger.exception(f"事件處理循環發生錯誤: {e}")
    #                 await asyncio.sleep(5)
    #     except asyncio.CancelledError:
    #         logger.info("事件處理循環被取消")
    #         raise
    #     except Exception as e:
    #         logger.exception(f"事件處理循環發生未預期的錯誤: {e}")
    # ======= 以上為已註解的定時推送相關邏輯 =======

    def _make_event_key(self, event):
        """生成事件唯一key，防止短時間重複推送"""
        # 使用更謹慎的複合鍵：signature + wallet_address + tokenAddress + poolAddress
        signature = event.get('signature', '')
        wallet_address = event.get('wallet_address', '')
        token_address = event.get('tokenAddress', '')
        pool_address = event.get('poolAddress', '')
        
        # 如果所有關鍵字段都存在，使用複合鍵
        if signature and wallet_address and token_address and pool_address:
            return f"sig_{signature}_wallet_{wallet_address}_token_{token_address}_pool_{pool_address}"
        # 如果只有 signature，使用 signature
        elif signature:
            return f"sig_{signature}"
        # 如果沒有 signature，使用其他字段組合作為備用
        else:
            return f"{token_address}_{event.get('transactionType')}_{event.get('transactionTime')}"

    def _insert_event_sorted(self, event):
        """按 transactionTime 排序插入事件到 pending_events"""
        try:
            event_time = int(event.get('transactionTime', 0))
            
            # 如果隊列為空，直接添加
            if not self.pending_events:
                self.pending_events.append(event)
                return
            
            # 二分查找插入位置
            left, right = 0, len(self.pending_events)
            while left < right:
                mid = (left + right) // 2
                mid_time = int(self.pending_events[mid].get('transactionTime', 0))
                
                if mid_time <= event_time:
                    left = mid + 1
                else:
                    right = mid
            
            # 在正確位置插入事件（按時間升序）
            self.pending_events.insert(left, event)
            
        except (ValueError, TypeError) as e:
            # 如果時間解析失敗，直接添加到末尾
            logger.warning(f"事件時間解析失敗，添加到隊列末尾: {e}")
            self.pending_events.append(event)

    def _cleanup_expired_cache(self, current_time):
        """清理過期的緩存數據"""
        try:
            # 清理事件推送緩存
            expired_keys = [k for k, v in self._event_push_cache.items() 
                          if current_time - v > self._event_push_cache_expiry]
            for k in expired_keys:
                del self._event_push_cache[k]
            
            # 定期清理已處理的 signatures（避免內存洩漏）
            if hasattr(self, '_processed_signatures') and len(self._processed_signatures) > 10000:
                # 如果已處理的 signatures 超過 10000 個，清空緩存
                logger.info("清理已處理的 signatures 緩存")
                self._processed_signatures.clear()
                
        except Exception as e:
            logger.error(f"清理緩存時發生錯誤: {e}")

    async def _send_events_batch(self, events):
        if not events:
            return True
        now = time.time()
        
        # 清理過期的緩存
        self._cleanup_expired_cache(now)
        filtered_events = []
        sent_signatures = set()
        processed_keys = set()
        
        for e in events:
            event_key = self._make_event_key(e)
            sig = e.get('signature')
            
            # 檢查是否已經處理過相同的 key
            if event_key in processed_keys:
                logger.debug(f"[API推送去重] 事件 key {event_key} 已在本批次中處理過，跳過")
                continue
                
            # signature級別去重，確保同一signature只推送一次
            if sig and sig in self._processed_signatures:
                logger.debug(f"[API推送去重] signature {sig} 已推送過，跳過")
                continue
                
            # 檢查緩存中的去重
            last_push = self._event_push_cache.get(event_key, 0)
            if now - last_push < self._event_push_cache_expiry:
                logger.debug(f"[API推送去重] 事件 {event_key} 在{self._event_push_cache_expiry}秒內已推送過，跳過")
                continue
                
            filtered_events.append(e)
            self._event_push_cache[event_key] = now
            processed_keys.add(event_key)
            if sig:
                sent_signatures.add(sig)
        if not filtered_events:
            logger.info("[API推送去重] 本批次無需推送事件")
            return True
        try:
            logger.info(f"開始批量發送 {len(filtered_events)} 個事件")
            print(filtered_events)
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_endpoint,
                    json=filtered_events,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        logger.info(f"成功發送 {len(filtered_events)} 個事件")
                        if hasattr(self, '_processed_signatures'):
                            for sig in sent_signatures:
                                self._processed_signatures.add(sig)
                        self._event_stats['total_sent'] += len(filtered_events)
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"批量發送事件失敗: {response.status}, {response_text}")
                        return False
        except Exception as e:
            logger.error(f"批量發送事件時發生錯誤: {str(e)}")
            return False

    def _init_db_connection(self) -> bool:
        """
        确保DB连接就绪: 检查激活状态，首次需要时创建引擎/工厂并测试连接
        增强容错能力，确保数据库连接始终可用
        """
        if not self._activated:
            logger.warning("尝试使用未激活的 TransactionProcessor，自动激活")
            self._activated = True
            
        if not self.db_enabled:
            logger.warning("数据库功能未启用，无法初始化连接")
            return False
        
        if self._connection_tested_successfully:
            return True
        
        if self.engine is None or self.session_factory is None:
            logger.info("引擎/工厂尚未创建，尝试在首次需要时创建...")
            try:
                # 優先使用 DATABASE_URL_SYNC 環境變數
                db_url = os.getenv('DATABASE_URL_SYNC')
                
                # 如果 DATABASE_URL_SYNC 未設定，則使用 settings.DATABASE_URL 並轉換
                if not db_url:
                    db_url = settings.DATABASE_URL
                    if not db_url:
                        logger.error("資料庫 URL 未設定，無法建立連線")
                        return False
                        
                    if db_url.startswith('postgresql+asyncpg://'):
                        db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://')
                        logger.info(f"轉換異步數據庫 URL 為同步 URL")
                else:
                    logger.info(f"使用環境變數 DATABASE_URL_SYNC")
                
                # 檢查 URL 格式
                if not db_url or not isinstance(db_url, str):
                    logger.error(f"資料庫 URL 無效")
                    return False
                
                if not (db_url.startswith('postgresql://') or db_url.startswith('mysql://') or db_url.startswith('sqlite:///')):
                    logger.error(f"資料庫 URL 格式無效，需要以 postgresql:// 或其他支援的資料庫前綴開始: {db_url}")
                    return False
                
                # logger.info(f"使用資料庫 URL: {db_url}")
                
                # 增强设置，加大连接池和超时时间
                self.engine = create_engine(
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
                self.session_factory = sessionmaker(
                    bind=self.engine,
                    expire_on_commit=False
                )
                logger.info("成功创建数据库引擎和会话工厂")
            except Exception as e:
                logger.error(f"创建数据库引擎/工厂时发生错误: {e}")
                self.engine = None
                self.session_factory = None
                self._connection_tested_successfully = False
                return False
        
        try:
            logger.info("测试数据库连接...")
            with self.session_factory() as session:
                result = session.execute(select(1))
                if result.scalar() == 1:
                    logger.info("数据库连接测试成功")
                    self._connection_tested_successfully = True
                    return True
                else:
                    logger.error("数据库连接测试失败 (查询未返回 1)")
                    self._connection_tested_successfully = False
                    return False
            
        except Exception as e:
            logger.error(f"测试数据库连接时发生错误: {e}")
            self._connection_tested_successfully = False
            return False
    
    def close_db_connection(self):
        """關閉資料庫連接"""
        if self.engine:
            logger.info("關閉資料庫連接")
            self.engine.dispose()
        self.engine = None
        self.session_factory = None
        self._connection_tested_successfully = False
        
                # 關閉 Ian 資料庫連接
        if self.ian_engine:
            logger.info("關閉 Ian 資料庫連接")
            self.ian_engine.dispose()
        self.ian_engine = None
        self.ian_session_factory = None
        self._ian_connection_tested_successfully = False
        
    def _init_ian_db_connection(self) -> bool:
        """
        初始化 Ian 資料庫連接（用於查詢 trades 表）
        """
        if self._ian_connection_tested_successfully:
            return True
            
        if not settings.DATABASE_URI_Ian:
            logger.warning("DATABASE_URI_Ian 未設定，無法查詢 trades 表")
            return False
            
        if self.ian_engine is None or self.ian_session_factory is None:
            try:
                db_url = settings.DATABASE_URI_Ian
                logger.info(f"初始化 Ian 資料庫連接: {db_url}")
                
                # 如果是異步 URL，轉換為同步 URL
                if db_url.startswith('postgresql+asyncpg://'):
                    db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://')
                    logger.info(f"轉換異步數據庫 URL 為同步 URL: {db_url}")
                
                self.ian_engine = create_engine(
                    db_url,
                    echo=False,
                    future=True,
                    pool_size=5,
                    max_overflow=10,
                    pool_timeout=30,
                    pool_recycle=1800,
                    pool_pre_ping=True
                )
                
                self.ian_session_factory = sessionmaker(bind=self.ian_engine)
                logger.info("Ian 資料庫引擎和會話工廠創建成功")
                
                # 測試連接
                with self.ian_session_factory() as session:
                    session.execute(text("SELECT 1"))
                    session.commit()
                    
                self._ian_connection_tested_successfully = True
                logger.info("Ian 資料庫連接測試成功")
                return True
                
            except Exception as e:
                logger.error(f"初始化 Ian 資料庫連接失敗: {str(e)}")
                self.ian_engine = None
                self.ian_session_factory = None
                return False
                
        return self._ian_connection_tested_successfully
    
    def get_wallet_balance(self, wallet_address: str) -> Dict[str, Any]:
        """獲取錢包餘額數據"""
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法獲取錢包 {wallet_address} 餘額，因數據庫未就緒")
            return {"balance": 0, "balance_usd": 0}

        try:
            with self.session_factory() as session:
                wallet_query = session.execute(
                    select(WalletSummary).where(WalletSummary.wallet_address == wallet_address)
                )
                wallet = wallet_query.scalars().first()

                if wallet:
                    balance = float(wallet.balance) if wallet.balance is not None else 0.0
                    balance_usd = float(wallet.balance_usd) if wallet.balance_usd is not None else 0.0
                    return {"balance": balance, "balance_usd": balance_usd}

            return {"balance": 0, "balance_usd": 0}
        except Exception as e:
            logger.exception(f"獲取錢包 {wallet_address} 餘額時發生錯誤: {e}")
            return {"balance": 0, "balance_usd": 0}

    def get_token_buy_data(self, wallet_address: str, token_address: str) -> Dict[str, Any]:
        """獲取代幣購買數據"""
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法獲取 {wallet_address}/{token_address} 購買數據，因數據庫未就緒")
            return {}

        try:
            with self.session_factory() as session:
                query = session.execute(
                    select(TokenBuyData).where(
                        (TokenBuyData.wallet_address == wallet_address) &
                        (TokenBuyData.token_address == token_address)
                    )
                )
                buy_record = query.scalars().first()

                if buy_record:
                    return {
                        "total_amount": float(buy_record.total_amount or 0.0),
                        "total_cost": float(buy_record.total_cost or 0.0),
                        "avg_buy_price": float(buy_record.avg_buy_price or 0.0),
                        "historical_total_sell_value": float(buy_record.historical_total_sell_value or 0.0),
                        "updated_at": buy_record.updated_at.timestamp() if buy_record.updated_at else 0
                    }
            return {}
        except Exception as e:
            logger.exception(f"獲取 {wallet_address}/{token_address} 購買數據時發生錯誤: {e}")
            return {}

    async def process_wallet_activities(self, wallet_address: str, activities: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """處理錢包活動 - 優先從資料庫查詢，如果沒有數據再使用 Solscan"""
        logger.info(f"開始處理錢包 {wallet_address} 的交易記錄")
        
        # 優先從資料庫查詢交易記錄
        try:
            logger.info(f"嘗試從資料庫查詢錢包 {wallet_address} 的交易記錄")
            trades_from_db = await self.fetch_trades_from_db(wallet_address)
            
            if trades_from_db:
                logger.info(f"從資料庫查詢到 {len(trades_from_db)} 筆交易記錄")
                
                # 標準化交易記錄
                normalized_trades = []
                for trade in trades_from_db:
                    normalized_trade = self.normalize_trade(trade)
                    if normalized_trade:
                        normalized_trades.append(normalized_trade)
                
                # 轉換為 Transaction 格式
                transaction_records = await self.convert_trades_to_transactions(normalized_trades, wallet_address)
                
                if transaction_records:
                    logger.info(f"成功從資料庫獲取並轉換了 {len(transaction_records)} 筆交易記錄")
                    
                    # 保存到 wallet_transaction 表
                    try:
                        save_result = await self.save_transactions_batch(transaction_records)
                        logger.info(f"批量保存交易記錄結果: {save_result}")
                    except Exception as e:
                        logger.error(f"保存交易記錄失敗: {str(e)}")
                    
                    return transaction_records
                
                # 如果有查詢到交易記錄但轉換後為空，仍然返回空列表而不使用 Solscan
                if trades_from_db:
                    logger.info(f"從資料庫查詢到交易記錄但轉換後為空，不使用 Solscan")
                    return []
                
        except Exception as e:
            logger.warning(f"從資料庫查詢交易記錄失敗: {str(e)}，將使用 Solscan 作為備選方案")
        
        # 只有在資料庫完全沒有數據時才使用 Solscan
        if not activities:
            logger.info(f"錢包 {wallet_address} 沒有活動記錄，創建空摘要")
            from app.services.wallet_summary_service import wallet_summary_service
            await wallet_summary_service.create_empty_wallet_summary(wallet_address)
            return []
        
        logger.info(f"資料庫中沒有交易記錄，使用 Solscan 數據處理錢包 {wallet_address} 的 {len(activities)} 筆活動")
        
        start_time = time.time()
        max_processing_time = 120  # 設置處理超時時間
        
        # 先去重、排序並預處理
        unique_activities = {}
        for activity in activities:
            tx_hash = activity.get("tx_hash", "unknown")
            if tx_hash not in unique_activities:
                unique_activities[tx_hash] = activity
        
        sorted_activities = sorted(unique_activities.values(), key=lambda x: x.get("block_time", 0))
        
        if len(sorted_activities) < len(activities):
            logger.info(f"移除了 {len(activities) - len(sorted_activities)} 筆重複活動")
        
        # 獲取錢包餘額
        wallet_balance = await self._get_wallet_balance_safely(wallet_address)
        
        # 預載入所有相關代幣資訊
        token_cache = {}
        token_addresses = self._extract_all_token_addresses(sorted_activities)
        
        if token_addresses:
            try:
                token_info_dict = await token_repository.get_multiple_token_info(list(token_addresses))
                token_cache.update(token_info_dict)
                logger.info(f"預載入 {len(token_info_dict)}/{len(token_addresses)} 個代幣資訊")
            except Exception as e:
                logger.warning(f"預載入代幣資訊失敗: {str(e)}")
        
        # 創建代幣買入資訊緩存
        token_buy_cache = {}
        
        # 從資料庫預載入現有的代幣買入資訊到緩存
        try:
            if self.db_enabled and self._connection_tested_successfully:
                with self.session_factory() as session:
                    existing_records = session.execute(
                        select(TokenBuyData).where(
                            TokenBuyData.wallet_address == wallet_address
                        )
                    ).scalars().all()
                    
                    for record in existing_records:
                        token_buy_cache[record.token_address] = {
                            "total_amount": Decimal(str(record.total_amount or 0)),
                            "total_cost": Decimal(str(record.total_cost or 0)),
                            "avg_buy_price": Decimal(str(record.avg_buy_price or 0)),
                            "historical_buy_amount": Decimal(str(record.historical_total_buy_amount or 0)),
                            "historical_buy_cost": Decimal(str(record.historical_total_buy_cost or 0)),
                            "historical_sell_amount": Decimal(str(record.historical_total_sell_amount or 0)),
                            "historical_sell_value": Decimal(str(record.historical_total_sell_value or 0)),
                            "realized_profit": Decimal(str(record.realized_profit or 0)),
                            "last_transaction_time": record.last_transaction_time or 0
                        }
                    
                    logger.info(f"從資料庫預載入 {len(token_buy_cache)} 個代幣買入資訊到緩存")
        except Exception as e:
            logger.warning(f"預載入代幣買入資訊失敗: {str(e)}")
        
        # 單次遍歷處理所有活動
        processed_transactions = []
        transaction_data_batch = []
        token_updates = {}
        
        # 添加進度追蹤
        activities_count = len(sorted_activities)
        progress_interval = max(1, activities_count // 10)
        
        for i, activity in enumerate(sorted_activities):
            # 處理超時保護
            if time.time() - start_time > max_processing_time:
                logger.warning(f"處理時間超出限制，已處理 {i}/{activities_count} 筆活動")
                break
            
            # 進度報告
            if i % progress_interval == 0:
                logger.info(f"處理進度: {i}/{activities_count} ({i/activities_count*100:.1f}%)")
            
            try:
                # 處理不同類型的活動
                activity_type = activity.get("activity_type", "").lower()
                
                if activity_type == 'token_swap':
                    # 使用緩存的代幣買入資訊處理交易
                    result = self._process_token_swap_with_cache(
                        wallet_address, activity, wallet_balance, token_cache, token_buy_cache
                    )
                    
                    if result and result.get("success"):
                        processed_transactions.append(result)
                        
                        # 收集交易數據以批量保存
                        if "transaction_data" in result:
                            transaction_data_batch.append(result["transaction_data"])
                        
                        # 收集代幣更新資訊
                        self._collect_token_update(result, token_updates)
                        
                        # 批量保存檢查
                        if len(transaction_data_batch) >= 50:
                            await self._safe_batch_save(transaction_data_batch)
                            transaction_data_batch = []
                
                elif activity_type == 'token_transfer':
                    # 處理代幣轉帳
                    result = self._process_token_transfer(wallet_address, activity)
                    
                    if result and result.get("success"):
                        processed_transactions.append(result)
                        # 收集交易數據以批量保存
                        if "transaction_data" in result:
                            transaction_data_batch.append(result["transaction_data"])
                        self._collect_token_transfer_update(result, token_updates)
                
                # 其他活動類型...
                
            except Exception as e:
                logger.exception(f"處理活動時出錯: {str(e)}")
        
        # 處理剩餘交易
        if transaction_data_batch:
            await self._safe_batch_save(transaction_data_batch)
        
        # 從緩存更新TokenBuyData表
        if token_buy_cache:  # 如果有資料要更新
            logger.info(f"準備從緩存更新錢包 {wallet_address} 的 {len(token_buy_cache)} 個代幣購買數據")
            try:
                update_result = await self.update_token_buy_data_from_cache(wallet_address, token_buy_cache)
                logger.info(f"TokenBuyData 從緩存更新結果: {'成功' if update_result else '失敗'}")
            except Exception as e:
                logger.error(f"從緩存更新代幣購買數據失敗: {str(e)}")
        else:
            logger.info(f"錢包 {wallet_address} 沒有需要更新的代幣購買數據")

        try:
            await self.update_wallet_summary(wallet_address)
        except Exception as e:
            logger.error(f"更新錢包摘要時出錯: {e}")
        
        total_time = time.time() - start_time
        logger.info(f"完成處理 {wallet_address} 錢包的 {len(processed_transactions)} 筆交易，總耗時: {total_time:.2f}秒")
        return processed_transactions

    # 輔助方法：帶錯誤處理的批量保存
    async def _safe_batch_save(self, batch):
        """安全地批量保存交易數據，錯誤時不中斷處理"""
        try:
            await self.save_transactions_batch(batch)
        except Exception as e:
            logger.error(f"批量保存交易時出錯: {str(e)}")
            # 嘗試單個保存，最大程度避免資料丟失
            for tx_data in batch:
                try:
                    await self.save_single_transaction(tx_data)
                except Exception as single_err:
                    logger.warning(f"保存單個交易時出錯 (簽名: {tx_data.get('signature')}): {str(single_err)}")

    async def save_single_transaction(self, tx_data: Dict[str, Any]) -> bool:
        """保存單個交易到資料庫"""
        try:
            return await self.save_transactions_batch([tx_data])
        except Exception as e:
            logger.error(f"保存單個交易失敗: {str(e)}")
            return False

    # 帶緩存的代幣交換處理方法
    def _process_token_swap_with_cache(self, wallet_address, activity_data, wallet_balance, token_cache, token_buy_cache):
        """處理代幣交換活動 - 使用本地緩存計算利潤"""
        tx_hash = activity_data.get('tx_hash', 'N/A')
        
        try:
            # 基本資料提取
            value = activity_data.get('value', 0)
            from_token = activity_data.get('from_token', {})
            to_token = activity_data.get('to_token', {})
            from_token_address = from_token.get('address', '')
            to_token_address = to_token.get('address', '')
            
            # 安全轉換數值
            try:
                from_amount = Decimal(str(from_token.get('amount', 0)))
                to_amount = Decimal(str(to_token.get('amount', 0)))
                value_decimal = Decimal(str(value))
            except (ValueError, TypeError):
                from_amount = Decimal('0')
                to_amount = Decimal('0')
                value_decimal = Decimal('0')
                
            timestamp = activity_data.get("block_time", None)
            
            # 基本檢查
            if not from_token_address or not to_token_address:
                return {"success": False, "transaction_hash": tx_hash, "message": "Missing token address"}
            
            # 數值檢查
            if from_amount <= 0 or to_amount <= 0:
                return {"success": False, "transaction_hash": tx_hash, "message": "Swap involves zero amount"}
            
            # 判斷交易類型
            is_buy = self._is_token_buy(from_token_address, to_token_address)
            
            # 先設置 token_address 和 amount
            if is_buy:
                token_address = to_token_address
                amount = to_amount
                cost = value_decimal
                price = value_decimal / to_amount if to_amount != 0 else Decimal('0')
            else:
                token_address = from_token_address
                amount = from_amount
                cost = value_decimal
                price = value_decimal / from_amount if from_amount != 0 else Decimal('0')
            
            # 使用新的交易類型判斷邏輯
            transaction_type = self.determine_transaction_type(wallet_address, token_address, is_buy, float(amount))
            
            # 使用緩存獲取代幣資訊
            from_token_symbol = self._get_token_symbol_cached(from_token_address, token_cache)
            to_token_symbol = self._get_token_symbol_cached(to_token_address, token_cache)
            token_name = self._get_token_symbol_cached(token_address, token_cache)
            token_icon = self._get_token_icon_cached(token_address, token_cache)
            token_supply = self._get_token_supply_cached(token_address, token_cache)
            
            # 計算 marketcap
            marketcap = 0
            if float(price) > 0:  # 只要有價格就計算marketcap
                marketcap = self._convert_to_float(price) * self._convert_to_float(token_supply)
            
            # 計算 holding_percentage
            holding_percentage = 0
            if is_buy and self._convert_to_float(value_decimal) > 0:
                wallet_balance_float = self._convert_to_float(wallet_balance)
                holding_pct = self._convert_to_float(value_decimal) / (wallet_balance_float + self._convert_to_float(value_decimal)) * 100
                holding_percentage = min(100, max(-100, holding_pct))
            elif not is_buy and self._convert_to_float(amount) > 0:
                token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                current_amount = self._convert_to_float(token_buy_data.get("total_amount", 0))
                if current_amount > 0:
                    holding_pct = (self._convert_to_float(amount) / current_amount) * 100
                    holding_percentage = min(100, max(-100, holding_pct))
                else:
                    holding_percentage = 100
            
            # 計算 realized_profit 和 realized_profit_percentage (只有賣出才有)
            realized_profit = 0
            realized_profit_percentage = 0
            
            if not is_buy:  # 賣出交易
                # 使用新的統一計算方法
                realized_profit, realized_profit_percentage = self.calculate_realized_profit_with_cache(
                    wallet_address, token_address, float(amount), float(price)
                )
            
            # 創建交易資料
            transaction_data = {
                "wallet_address": wallet_address,
                "wallet_balance": wallet_balance,
                "signature": tx_hash,
                "transaction_time": timestamp,
                "transaction_type": transaction_type,  # 使用新的交易類型
                "from_token_address": from_token_address,
                "from_token_symbol": from_token_symbol,
                "from_token_amount": float(from_amount),
                "dest_token_address": to_token_address,
                "dest_token_symbol": to_token_symbol,
                "dest_token_amount": float(to_amount),
                "token_address": token_address,
                "token_name": token_name,
                "token_icon": token_icon,
                "amount": float(amount),
                "value": float(value_decimal),
                "price": float(price),
                "marketcap": marketcap,
                "holding_percentage": holding_percentage,
                "realized_profit": realized_profit,
                "realized_profit_percentage": realized_profit_percentage,
                "chain": "SOLANA"
            }
            
            # 更新WalletTokenState
            self._update_wallet_token_state_after_transaction(wallet_address, token_address, transaction_type, float(amount), float(value_decimal), float(price), timestamp)
            
            return {
                "success": True,
                "transaction_hash": tx_hash,
                "transaction_data": transaction_data,
                "token_address": token_address,
                "amount": float(amount),
                "value": float(value_decimal),
                "transaction_type": transaction_type  # 使用新的交易類型
            }
            
        except Exception as e:
            logger.exception(f"處理代幣交換交易 {tx_hash} 時發生錯誤: {e}")
            return {
                "success": False,
                "transaction_hash": tx_hash,
                "message": f"處理代幣交換交易時發生錯誤: {str(e)}"
            }

    # 使用緩存獲取代幣符號
    def _get_token_symbol_cached(self, token_address, token_cache):
        """從緩存中獲取代幣符號，如果不存在則回退到原方法"""
        if token_address in token_cache:
            token_info = token_cache[token_address]
            return token_info.get("symbol", None)
        
        # 回退到原方法
        return self.get_token_symbol(token_address)
    
    def _get_token_icon_cached(self, token_address, token_cache):
        """從緩存中獲取代幣圖標，如果不存在則回退到原方法"""
        if token_address in token_cache:
            token_info = token_cache[token_address]
            return token_info.get("icon", None)
        return None
    
    def _get_token_supply_cached(self, token_address, token_cache):
        """從緩存中獲取代幣供應量，如果不存在則回退到原方法"""
        default_supply = 1000000000  # 預設供應量為10億
        
        if token_address in token_cache:
            token_info = token_cache[token_address]
            return token_info.get("supply_float", default_supply)
        
        return default_supply
    
    def _extract_all_token_addresses(self, activities: List[Dict]) -> set:
        """從活動記錄中提取所有的代幣地址"""
        token_addresses = set()
        
        for activity in activities:
            activity_type = activity.get("activity_type", "")
            
            if activity_type == "token_swap":
                # 提取交換中涉及的代幣
                from_token = activity.get("from_token", {})
                to_token = activity.get("to_token", {})
                
                if from_token and "address" in from_token:
                    token_addresses.add(from_token["address"])
                if to_token and "address" in to_token:
                    token_addresses.add(to_token["address"])
                    
            elif activity_type == "token_transfer":
                # 提取轉帳中涉及的代幣
                token_address = activity.get("token_address", "")
                if token_address:
                    token_addresses.add(token_address)
        
        return token_addresses

    async def update_token_buy_data_from_cache(self, wallet_address: str, token_buy_cache: Dict[str, Dict]) -> bool:
        """從緩存更新TokenBuyData表"""
        if not token_buy_cache:
            logger.info(f"沒有 {wallet_address} 的代幣購買數據需要更新")
            return True
        
        logger.info(f"開始從緩存更新錢包 {wallet_address} 的 {len(token_buy_cache)} 個代幣購買數據")
        # 打印前3個要更新的代幣地址，以便於調試
        sample_tokens = list(token_buy_cache.keys())[:3]
        logger.info(f"樣本代幣地址: {sample_tokens}")
            
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法更新代幣購買數據，因數據庫未就緒")
            return False

        start_time = time.time()
        updates_processed = 0
        
        try:
            # 確保我們有可用的會話工廠
            if self.session_factory is None:
                logger.error("session_factory為None，無法更新代幣購買數據")
                return False
                
            with self.session_factory() as session:
                try:
                    # 查詢現有記錄
                    token_addresses = list(token_buy_cache.keys())
                    existing_records_query = session.execute(
                        select(TokenBuyData).where(
                            (TokenBuyData.wallet_address == wallet_address) &
                            (TokenBuyData.token_address.in_(token_addresses))
                        )
                    )
                    
                    existing_records = existing_records_query.scalars().all()
                    record_map = {record.token_address: record for record in existing_records}
                    
                    # 記錄查詢結果
                    logger.info(f"從緩存更新代幣買入資訊: 找到 {len(record_map)}/{len(token_addresses)} 個現有記錄")
                    
                    # 更新或創建記錄
                    new_records = []
                    updated_records = []
                    
                    for token_address, cache_data in token_buy_cache.items():
                        # 僅對有餘額或有交易記錄的代幣進行處理
                        has_transactions = (
                            cache_data.get("historical_buy_amount", Decimal('0')) > 0 or
                            cache_data.get("historical_sell_amount", Decimal('0')) > 0
                        )
                        
                        if not has_transactions:
                            continue
                        
                        # 從緩存獲取數據
                        total_amount = float(cache_data.get("total_amount", 0))
                        total_cost = float(cache_data.get("total_cost", 0))
                        avg_buy_price = float(cache_data.get("avg_buy_price", 0))
                        historical_buy_amount = float(cache_data.get("historical_buy_amount", 0))
                        historical_buy_cost = float(cache_data.get("historical_buy_cost", 0))
                        historical_sell_amount = float(cache_data.get("historical_sell_amount", 0))
                        historical_sell_value = float(cache_data.get("historical_sell_value", 0))
                        realized_profit = float(cache_data.get("realized_profit", 0))
                        last_transaction_time = cache_data.get("last_transaction_time", int(time.time()))
                        
                        if token_address in record_map:
                            # 更新現有記錄
                            record = record_map[token_address]
                            record.chain = "SOLANA"
                            record.total_amount = total_amount
                            record.total_cost = total_cost
                            record.avg_buy_price = avg_buy_price
                            record.historical_total_buy_amount = historical_buy_amount
                            record.historical_total_buy_cost = historical_buy_cost
                            record.historical_total_sell_amount = historical_sell_amount
                            record.historical_total_sell_value = historical_sell_value
                            record.realized_profit = realized_profit
                            record.last_transaction_time = last_transaction_time
                            record.updated_at = now_utc8()
                            
                            # Recalculate historical average prices
                            record.historical_avg_buy_price = historical_buy_cost / historical_buy_amount if historical_buy_amount > 0 else 0.0
                            record.historical_avg_sell_price = historical_sell_value / historical_sell_amount if historical_sell_amount > 0 else 0.0
                            
                            # 特殊邏輯: 第一次交易和最後一次交易時間
                            if not record.position_opened_at and historical_buy_amount > 0:
                                record.position_opened_at = now_utc8().timestamp()
                            
                            if total_amount == 0 and historical_sell_amount > 0:
                                record.last_active_position_closed_at = now_utc8().timestamp()
                            
                            updated_records.append(token_address)
                        else:
                            # 創建新記錄
                            new_record = TokenBuyData(
                                wallet_address=wallet_address,
                                chain="SOLANA",
                                chain_id=501,
                                token_address=token_address,
                                total_amount=total_amount,
                                total_cost=total_cost,
                                avg_buy_price=avg_buy_price,
                                position_opened_at=now_utc8().timestamp() if total_amount > 0 else None,
                                historical_total_buy_amount=historical_buy_amount,
                                historical_total_buy_cost=historical_buy_cost,
                                historical_total_sell_amount=historical_sell_amount,
                                historical_total_sell_value=historical_sell_value,
                                historical_avg_buy_price=avg_buy_price if historical_buy_amount > 0 else 0,
                                historical_avg_sell_price=historical_sell_value / historical_sell_amount if historical_sell_amount > 0 else 0,
                                last_active_position_closed_at=now_utc8().timestamp() if total_amount == 0 and historical_sell_amount > 0 else None,
                                last_transaction_time=last_transaction_time,
                                realized_profit=realized_profit,
                                updated_at=now_utc8(),
                                date=now_utc8().date()
                            )
                            session.add(new_record)
                            new_records.append(token_address)
                        
                        updates_processed += 1
                    
                    # 提交所有更改
                    session.commit()
                    
                    # 驗證更新結果
                    if new_records or updated_records:
                        # 記錄處理結果
                        logger.info(f"代幣買入資訊更新結果: 新增 {len(new_records)}, 更新 {len(updated_records)}")
                        
                        # 驗證部分新記錄
                        if new_records:
                            validation_sample = new_records[:min(3, len(new_records))]
                            validation_query = session.execute(
                                select(TokenBuyData).where(
                                    (TokenBuyData.wallet_address == wallet_address) &
                                    (TokenBuyData.token_address.in_(validation_sample))
                                )
                            )
                            validation_results = validation_query.scalars().all()
                            logger.info(f"驗證結果: 找到 {len(validation_results)}/{len(validation_sample)} 個新插入的記錄")
                    
                    elapsed = time.time() - start_time
                    logger.info(f"從緩存更新 {updates_processed} 個代幣購買數據完成，耗時: {elapsed:.2f}秒")
                    return True
                    
                except Exception as inner_e:
                    session.rollback()
                    logger.exception(f"在會話中更新代幣購買數據時出錯: {inner_e}")
                    return False
                    
        except Exception as e:
            logger.exception(f"從緩存更新代幣購買數據時發生錯誤: {e}")
            return False

    async def save_transactions_batch(self, transactions: List[Dict[str, Any]]) -> bool:
        """批量保存交易到數據庫 - 針對分區表優化版本"""
        if not self.db_enabled or not transactions:
            return False
        
        logger.info(f"嘗試批量保存 {len(transactions)} 筆交易")
        
        # 确保数据库连接始终正常
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法批量保存交易，因數據庫未就緒")
            return False
        
        # 修正所有交易的 transaction_time 毫秒問題
        for tx in transactions:
            transaction_time = tx.get("transaction_time")
            if transaction_time and transaction_time > 10**12:
                tx["transaction_time"] = transaction_time // 1000
        
        start_time = time.time()
        first_error_logged = False  # 新增 flag
        try:
            with self.session_factory() as session:
                inserted_count = 0
                updated_count = 0
                
                # 使用原始SQL批量處理，讓PostgreSQL自動處理分區路由
                for idx, tx_data in enumerate(transactions):
                    signature = tx_data.get("signature")
                    wallet_address = tx_data.get("wallet_address")
                    token_address = tx_data.get("token_address")
                    transaction_time = tx_data.get("transaction_time")
                    
                    if not all([signature, wallet_address, token_address, transaction_time]):
                        logger.warning(f"跳過缺少必要欄位的交易: {signature}")
                        continue
                    
                    # 檢查代幣名稱是否為空
                    token_name = tx_data.get("token_name")
                    if not token_name or token_name.strip() == "":
                        logger.warning(f"代幣名稱為空，跳過保存交易: {token_address}, signature: {signature}")
                        continue
                    
                    # 設置時間字段
                    if "time" not in tx_data:
                        time_now = now_utc8()
                        tx_data["time"] = time_now.strftime('%Y-%m-%d %H:%M:%S.%f')
                    
                    # 確保 chain 欄位不為空
                    if not tx_data.get("chain"):
                        tx_data["chain"] = "SOLANA"
                    
                    try:
                        # 使用原始SQL進行UPSERT，包含分區鍵
                        upsert_sql = text("""
                            INSERT INTO dex_query_v1.wallet_transaction (
                                wallet_address, wallet_balance, signature, transaction_time, 
                                transaction_type, token_address, token_name, token_icon, 
                                marketcap, amount, value, price, holding_percentage, 
                                realized_profit, realized_profit_percentage, chain,
                                from_token_address, from_token_symbol, from_token_amount,
                                dest_token_address, dest_token_symbol, dest_token_amount, time
                            ) VALUES (
                                :wallet_address, :wallet_balance, :signature, :transaction_time,
                                :transaction_type, :token_address, :token_name, :token_icon,
                                :marketcap, :amount, :value, :price, :holding_percentage,
                                :realized_profit, :realized_profit_percentage, :chain,
                                :from_token_address, :from_token_symbol, :from_token_amount,
                                :dest_token_address, :dest_token_symbol, :dest_token_amount, :time
                            )
                            ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                            DO UPDATE SET
                                wallet_balance = EXCLUDED.wallet_balance,
                                transaction_type = EXCLUDED.transaction_type,
                                token_name = EXCLUDED.token_name,
                                token_icon = EXCLUDED.token_icon,
                                marketcap = EXCLUDED.marketcap,
                                amount = EXCLUDED.amount,
                                value = EXCLUDED.value,
                                price = EXCLUDED.price,
                                holding_percentage = EXCLUDED.holding_percentage,
                                realized_profit = EXCLUDED.realized_profit,
                                realized_profit_percentage = EXCLUDED.realized_profit_percentage,
                                chain = EXCLUDED.chain,
                                from_token_address = EXCLUDED.from_token_address,
                                from_token_symbol = EXCLUDED.from_token_symbol,
                                from_token_amount = EXCLUDED.from_token_amount,
                                dest_token_address = EXCLUDED.dest_token_address,
                                dest_token_symbol = EXCLUDED.dest_token_symbol,
                                dest_token_amount = EXCLUDED.dest_token_amount,
                                time = EXCLUDED.time
                        """)
                        
                        session.execute(upsert_sql, tx_data)
                        inserted_count += 1
                        
                    except Exception as e:
                        if not first_error_logged:
                            logger.error(f"[FIRST ERROR] 第{idx+1}筆交易寫入失敗: {str(e)}\nSQL: {upsert_sql}\n參數: {tx_data}", exc_info=True)
                            first_error_logged = True
                        else:
                            logger.warning(f"第{idx+1}筆交易寫入失敗: {str(e)}")
                        
                        # 如果UPSERT失敗，使用查詢-插入/更新方法
                        try:
                            check_sql = text("""
                                SELECT id FROM dex_query_v1.wallet_transaction 
                                WHERE signature = :signature 
                                AND wallet_address = :wallet_address 
                                AND token_address = :token_address
                            """)
                            
                            existing = session.execute(check_sql, {
                                'signature': signature,
                                'wallet_address': wallet_address,
                                'token_address': token_address
                            }).fetchone()
                            
                            if existing:
                                update_sql = text("""
                                    UPDATE dex_query_v1.wallet_transaction SET
                                        wallet_balance = :wallet_balance,
                                        transaction_type = :transaction_type,
                                        token_name = :token_name,
                                        token_icon = :token_icon,
                                        marketcap = :marketcap,
                                        amount = :amount,
                                        value = :value,
                                        price = :price,
                                        holding_percentage = :holding_percentage,
                                        realized_profit = :realized_profit,
                                        realized_profit_percentage = :realized_profit_percentage,
                                        time = :time
                                    WHERE signature = :signature 
                                    AND wallet_address = :wallet_address 
                                    AND token_address = :token_address
                                """)
                                session.execute(update_sql, tx_data)
                                updated_count += 1
                            else:
                                # 插入
                                insert_sql = text("""
                                    INSERT INTO dex_query_v1.wallet_transaction (
                                        wallet_address, wallet_balance, signature, transaction_time, 
                                        transaction_type, token_address, token_name, token_icon, 
                                        marketcap, amount, value, price, holding_percentage, 
                                        realized_profit, realized_profit_percentage, chain,
                                        from_token_address, from_token_symbol, from_token_amount,
                                        dest_token_address, dest_token_symbol, dest_token_amount, time
                                    ) VALUES (
                                        :wallet_address, :wallet_balance, :signature, :transaction_time,
                                        :transaction_type, :token_address, :token_name, :token_icon,
                                        :marketcap, :amount, :value, :price, :holding_percentage,
                                        :realized_profit, :realized_profit_percentage, :chain,
                                        :from_token_address, :from_token_symbol, :from_token_amount,
                                        :dest_token_address, :dest_token_symbol, :dest_token_amount, :time
                                    )
                                """)
                                session.execute(insert_sql, tx_data)
                                inserted_count += 1
                                
                        except Exception as inner_e:
                            if not first_error_logged:
                                logger.error(f"[FIRST ERROR][FALLBACK] 替代方法處理交易 {signature} 失敗: {str(inner_e)}", exc_info=True)
                                first_error_logged = True
                            else:
                                logger.error(f"替代方法處理交易 {signature} 失敗: {str(inner_e)}")
                            # 記錄錯誤但不回滾，讓其他交易繼續處理
                            continue
                # 提交所有變更
                session.commit()
                
                elapsed = time.time() - start_time
                logger.info(f"批量保存 {len(transactions)} 筆交易完成（插入: {inserted_count}, 更新: {updated_count}），耗時: {elapsed:.2f}秒")
                return True
                
        except Exception as e:
            logger.error(f"批量UPSERT交易失敗，錯誤: {str(e)}", exc_info=True)
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"回滾交易時發生錯誤: {rollback_error}")
            return False

    # 刪除重複的方法定義 - 此方法與第825行的方法重複
    
    def _is_token_buy(self, from_token: str, to_token: str) -> bool:
        """
        判斷交易是買入還是賣出
        
        Args:
            from_token: 源代幣地址
            to_token: 目標代幣地址
            
        Returns:
            如果是買入交易則返回True，否則返回False
        """
        # 定義常用代幣地址
        stablecoins = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        ]
        sol_addresses = [
            "So11111111111111111111111111111111111111112",  # SOL
            "So11111111111111111111111111111111111111111"   # 另一種 SOL 地址表示
        ]
        
        # 檢查代幣地址是否為基礎代幣(SOL或穩定幣)
        from_is_base = from_token in stablecoins or from_token in sol_addresses
        to_is_base = to_token in stablecoins or to_token in sol_addresses
        
        # 邏輯1: 從SOL換成穩定幣 -> 賣出(SOL)
        if from_token in sol_addresses and to_token in stablecoins:
            logger.debug(f"從SOL換成穩定幣: {from_token} -> {to_token}, 判定為賣出")
            return False
        
        # 邏輯2: 從穩定幣換成SOL -> 買入(SOL)
        if from_token in stablecoins and to_token in sol_addresses:
            logger.debug(f"從穩定幣換成SOL: {from_token} -> {to_token}, 判定為買入")
            return True
        
        # 邏輯3: 從基礎代幣換成其他代幣 -> 買入
        if from_is_base and not to_is_base:
            logger.debug(f"從基礎代幣換成其他代幣: {from_token} -> {to_token}, 判定為買入")
            return True
        
        # 邏輯4: 從其他代幣換成基礎代幣 -> 賣出
        if not from_is_base and to_is_base:
            logger.debug(f"從其他代幣換成基礎代幣: {from_token} -> {to_token}, 判定為賣出")
            return False
        
        # 邏輯5: 都不是基礎代幣 -> 視為買入目標代幣(token2)
        logger.debug(f"都不是基礎代幣的交換: {from_token} -> {to_token}, 默認判定為買入")
        return True
    
    def _process_token_transfer(self, wallet_address: str, activity_data: Dict) -> Dict:
        """處理代幣轉帳活動"""
        tx_hash = activity_data.get('tx_hash', 'N/A')
        transaction_hash = activity_data.get('transaction_hash', tx_hash)
        
        try:
            # 提取基本數據
            token_address = activity_data.get('token_address', '')
            amount = Decimal(str(activity_data.get('amount', 0)))
            is_incoming = activity_data.get('is_incoming', False)
            timestamp = activity_data.get('block_time', int(time.time()))
            block_number = activity_data.get('slot', 0)
            from_address = activity_data.get('from_address', '')
            to_address = activity_data.get('to_address', '')
            
            # 基本檢查
            if not token_address:
                logger.warning(f"轉帳交易 {tx_hash} 缺少代幣地址")
                return {"success": False, "transaction_hash": tx_hash, "message": "Missing token address"}
            
            if amount <= 0:
                logger.warning(f"轉帳交易 {tx_hash} 金額為零，跳過處理")
                return {"success": False, "transaction_hash": tx_hash, "message": "Transfer amount is zero"}
            
            # 設置活動類型
            activity_type = 'receive' if is_incoming else 'send'
            
            # 獲取代幣資訊
            token_name = self.get_token_symbol(token_address)
            token_icon = self.get_token_icon(token_address)
            
            # 創建交易資料
            transaction_data = {
                "wallet_address": wallet_address,
                "wallet_balance": 0,  # 轉帳交易不涉及錢包餘額變化
                "signature": transaction_hash,
                "transaction_time": timestamp,
                "transaction_type": activity_type,
                "from_token_address": from_address if not is_incoming else token_address,
                "from_token_symbol": token_name if not is_incoming else token_name,
                "from_token_amount": float(amount) if not is_incoming else 0,
                "dest_token_address": to_address if is_incoming else token_address,
                "dest_token_symbol": token_name if is_incoming else token_name,
                "dest_token_amount": float(amount) if is_incoming else 0,
                "token_address": token_address,
                "token_name": token_name,
                "token_icon": token_icon,
                "amount": float(amount),
                "value": 0,  # 轉帳交易沒有價值
                "price": 0,  # 轉帳交易沒有價格
                "marketcap": 0,
                "holding_percentage": 0,
                "realized_profit": 0,
                "realized_profit_percentage": 0,
                "chain": "SOLANA"
            }
            
            # 創建交易記錄
            return {
                "success": True,
                "transaction_hash": transaction_hash,
                "transaction_data": transaction_data,
                "token_address": token_address,
                "amount": float(amount),
                "type": activity_type,
                "is_incoming": is_incoming,
                "from_address": from_address,
                "to_address": to_address,
                "timestamp": timestamp,
                "block_number": block_number
            }
            
        except Exception as e:
            logger.exception(f"處理代幣轉帳 {tx_hash} 時發生不可預期的錯誤: {e}")
            return {
                "success": False,
                "transaction_hash": tx_hash,
                "message": f"Error processing transfer: {str(e)}"
            }
    
    def _process_bulk_token_activities(self, wallet_address: str, bulk_activity_data: dict) -> Dict:
        """
        处理包含多个子活动的 'token_activities' 类型
        增强错误处理和批处理效率
        """
        tx_hash_main = bulk_activity_data.get("tx_hash", "N/A")
        # logger.info(f"處理批量代幣活動 (主 Tx: {tx_hash_main}) for {wallet_address}")
        
        try:
            activities = bulk_activity_data.get('activities', [])
            if not activities:
                logger.warning(f"批量活动 {tx_hash_main} 没有子活动数据")
                return {
                    "success": False,
                    "transaction_hash": tx_hash_main,
                    "message": "No sub-activities found"
                }
                
            # 基本数据准备
            processed_count = 0
            total_count = len(activities)
            batch_results = []
            
            # 获取主活动信息
            timestamp_main = bulk_activity_data.get("block_time", int(time.time()))
            block_number_main = bulk_activity_data.get("slot", 0)
            
            # 批量处理子活动记录
            # 提前识别重复签名，避免后续插入冲突
            seen_signatures = set()
            
            # 处理每个子活动
            for sub_activity in activities:
                try:
                    sub_tx_hash = sub_activity.get('tx_hash', tx_hash_main)
                    
                    # 跳过重复的子活动
                    if sub_tx_hash in seen_signatures:
                        logger.debug(f"跳过重复子活动签名: {sub_tx_hash}")
                        continue
                    seen_signatures.add(sub_tx_hash)
                    
                    # 提取基本数据
                    token_address = sub_activity.get('token_address', '')
                    amount_str = sub_activity.get('amount', '0')
                    value_str = sub_activity.get('value', '0')
                    is_buy = sub_activity.get('is_buy', False)
                    
                    # 基本检查
                    if not token_address:
                        logger.warning(f"跳過處理無效的子代幣活動 (缺少 token_address)")
                        continue
                    
                    # 安全转换数值
                    try:
                        amount = Decimal(str(amount_str))
                        value = Decimal(str(value_str))
                    except (ValueError, TypeError):
                        logger.warning(f"子活动 {sub_tx_hash} 数值转换失败")
                        continue
                    
                    # 跳过零金额活动
                    if amount <= 0 and value <= 0:
                        logger.debug(f"跳過处理零金額的子代幣活動")
                        continue
                    
                    # 安全计算价格
                    price = Decimal(0)
                    if amount > 0:
                        price = value / amount
                    
                    # 创建交易记录
                    transaction_record = {
                        "success": True,
                        "transaction_hash": sub_tx_hash,
                        "token_address": token_address,
                        "amount": float(amount),
                        "value": float(value),
                        "price": float(price),
                        "activity_type": 'buy' if is_buy else 'sell',
                        "timestamp": timestamp_main,
                        "block_number": block_number_main
                    }
                    
                    batch_results.append(transaction_record)
                    processed_count += 1
                    
                except Exception as e:
                    logger.warning(f"處理單個子代幣活動時出錯: {e}")
                    # 不将错误信息加入结果，继续处理其他子活动
            
            # 处理结果
            if processed_count == 0:
                logger.warning(f"批量活动 {tx_hash_main} 未能成功处理任何子活动")
                return {
                    "success": False,
                    "transaction_hash": tx_hash_main,
                    "message": "No valid sub-activities processed"
                }
            
            # logger.info(f"處理批量代幣活動 {tx_hash_main} 完成: {processed_count}/{total_count} 筆活動")
            return {
                "success": True,
                "transaction_hash": tx_hash_main,
                "processed_count": processed_count,
                "total_count": total_count,
                "details": batch_results
            }
            
        except Exception as e:
            logger.exception(f"處理批量代幣活動 {tx_hash_main} 時發生錯誤: {e}")
            return {
                "success": False,
                "transaction_hash": tx_hash_main,
                "message": f"處理批量代幣活動時發生錯誤: {str(e)}"
            }
        
    async def _safe_retry_async(self, coro, retries=3, delay=1.0, backoff=2.0, error_msg="操作失败"):
        """
        安全地重试异步操作，使用指数退避策略
        
        Args:
            coro: 要执行的异步协程
            retries: 最大重试次数
            delay: 初始延迟时间（秒）
            backoff: 延迟增长倍数
            error_msg: 记录错误日志时的前缀信息
            
        Returns:
            操作的结果，或者在所有重试都失败时引发最后一个异常
        """
        last_exception = None
        current_delay = delay
        
        for attempt in range(retries):
            try:
                return await coro
            except Exception as e:
                last_exception = e
                if attempt < retries - 1:  # 还有重试机会
                    logger.warning(f"{error_msg} - 尝试 {attempt+1}/{retries} 失败: {str(e)}，等待 {current_delay:.1f}s 后重试")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff  # 指数增长延迟
                else:
                    logger.error(f"{error_msg} - 所有 {retries} 次尝试均失败。最后错误: {str(e)}")
        
        # 所有重试都失败
        if last_exception:
            raise last_exception
        raise Exception(f"{error_msg} - 未知原因的重试失败")
    
    def get_token_info_safely(self, address: str) -> Dict[str, Any]:
        """安全獲取代幣信息的統一方法"""
        if not address:
            return {}
        
        # 硬編碼常見代幣
        common_tokens = {
            "So11111111111111111111111111111111111111112": {
                "symbol": "WSOL",
                "name": "Solana",
                "decimals": 9,
                "supply_float": 15803159.03,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
            },
            "So11111111111111111111111111111111111111111": {
                "symbol": "SOL",
                "name": "Solana",
                "decimals": 9,
                "supply_float": 15803159.03,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
            },
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
                "symbol": "USDC",
                "name": "USD Coin",
                "decimals": 6,
                "supply_float": 8456027186.13,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
            },
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
                "symbol": "USDT",
                "name": "Tether USD",
                "decimals": 6,
                "supply_float": 2389927599.54,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB/logo.png"
            },
            "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs": {
                "symbol": "ETH",
                "name": "Ether (Portal)",
                "decimals": 8,
                "supply_float": 72471.25,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs/logo.png"
            }
        }
        
        # 檢查是否為常見代幣
        if address in common_tokens:
            return common_tokens[address]
        
        # 檢查快取
        if address in self.token_info_cache:
            return self.token_info_cache[address]
        
        # 嘗試從token_repository獲取
        try:
            info = token_repository.get_token(address)
            if info:
                self.token_info_cache[address] = info
                return info
        except Exception as e:
            logger.error(f"獲取代幣 {address} 信息時出錯: {e}")
        
        return {"symbol": None, "name": None, "icon": None, "supply_float": None}

    def get_token_symbol(self, token_address: str) -> str:
        """獲取代幣符號"""
        token_info = self.get_token_info_safely(token_address)
        return token_info.get("symbol", None)

    def get_token_name(self, token_address: str) -> str:
        """獲取代幣名稱"""
        token_info = self.get_token_info_safely(token_address)
        return token_info.get("name", None)
    
    def get_token_icon(self, token_address: str) -> str:
        """獲取代幣名稱"""
        token_info = self.get_token_info_safely(token_address)
        return token_info.get("icon", None)
    
    def get_token_supply(self, token_address: str) -> float:
        """獲取代幣供應量"""
        token_info = self.get_token_info_safely(token_address)
        return token_info.get("supply_float", None)
    
    async def _get_wallet_balance_safely(self, wallet_address: str) -> float:
        """安全地獲取錢包餘額，如果失敗則返回0"""
        try:
            balance_data = await self.get_sol_balance(wallet_address)
            wallet_balance = balance_data.get("balance", {}).get("float", 0)
            logger.info(f"成功獲取錢包 {wallet_address} 餘額: {wallet_balance}")
            return wallet_balance
        except Exception as e:
            logger.warning(f"獲取錢包 {wallet_address} 餘額失敗: {e}，使用預設值 0")
            return 0
        
    def _collect_token_update(self, result, token_updates):
        """收集代幣交換的更新信息"""
        if not result or not result.get("success"):
            return
        
        token_address = result.get("token_address")
        if not token_address:
            return
        
        transaction_type = result.get("transaction_type")
        amount = result.get("amount", 0)
        value = result.get("value", 0)
        
        is_buy = transaction_type == "buy" or transaction_type == "build"
        
        if token_address not in token_updates:
            token_updates[token_address] = {
                "amount": 0,
                "cost": 0,
                "is_buy": is_buy
            }
        
        # 更新數量和成本
        if is_buy:
            token_updates[token_address]["amount"] += amount
            token_updates[token_address]["cost"] += value
        else:
            token_updates[token_address]["amount"] -= amount
        
        token_updates[token_address]["is_buy"] = is_buy

    def _collect_token_transfer_update(self, result, token_updates):
        """收集代幣轉帳的更新信息"""
        if not result or not result.get("success"):
            return
        
        token_address = result.get("token_address")
        if not token_address:
            return
        
        is_incoming = result.get("is_incoming", False)
        amount = result.get("amount", 0)
        
        if token_address not in token_updates:
            token_updates[token_address] = {
                "amount": 0,
                "cost": 0,
                "is_buy": False
            }
        
        # 更新數量
        if is_incoming:
            token_updates[token_address]["amount"] += amount
        else:
            token_updates[token_address]["amount"] -= amount

    async def update_missing_profits(self):
        """更新缺失的交易利潤數據"""
        # logger.info("開始更新缺失的交易利潤數據")
        
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error("數據庫未就緒，無法更新交易利潤")
            return False
        
        try:
            with self.session_factory() as session:
                # 查詢所有缺失利潤的賣出交易
                query = select(Transaction).where(
                    (Transaction.transaction_type == 'sell') &
                    (Transaction.realized_profit == 0)
                )
                transactions = session.execute(query).scalars().all()
                
                updated_count = 0
                for tx in transactions:
                    # 使用新的 WalletTokenState 緩存機制獲取平均買入價格
                    wallet_token_state = self.get_wallet_token_state(tx.wallet_address, tx.token_address)
                    avg_buy_price = wallet_token_state["current_avg_buy_price"]
                    
                    # 如果緩存中沒有有效的平均買入價格，嘗試從數據庫獲取
                    if avg_buy_price <= 0:
                        token_buy_data = self.get_token_buy_data(tx.wallet_address, tx.token_address)
                        historical_buy_amount = self._convert_to_float(token_buy_data.get("historical_total_buy_amount", 0))
                        historical_buy_cost = self._convert_to_float(token_buy_data.get("historical_total_buy_cost", 0))
                        
                        # 計算平均買入價格
                        if historical_buy_amount > 0:
                            avg_buy_price = historical_buy_cost / historical_buy_amount
                    
                    # 計算利潤
                    amount = tx.amount
                    value = tx.value
                    
                    if avg_buy_price > 0 and amount > 0:
                        cost_basis = amount * avg_buy_price
                        realized_profit = value - cost_basis
                        
                        if cost_basis > 0:
                            realized_profit_percentage = (realized_profit / cost_basis) * 100
                        else:
                            realized_profit_percentage = 0
                        
                        # 更新交易記錄
                        tx.realized_profit = realized_profit
                        tx.realized_profit_percentage = realized_profit_percentage
                        updated_count += 1
                        
                        logger.info(f"更新交易 {tx.signature} 的利潤: {realized_profit}, 比例: {realized_profit_percentage}%")
                
                if updated_count > 0:
                    session.commit()
                    logger.info(f"成功更新 {updated_count} 筆交易的利潤數據")
                else:
                    logger.info("沒有需要更新的交易")
                    
                return updated_count > 0
        except Exception as e:
            logger.exception(f"更新交易利潤數據時出錯: {e}")
            return False
    
    async def save_transaction(self, transaction: Dict[str, Any]) -> bool:
        try:
            signature = transaction.get("signature")
            transaction_time = transaction.get("transaction_time")
            pool_address = transaction.get("pool_address")
            wallet_address = transaction.get("wallet_address")
            token_address = transaction.get("token_address")
            
            if not signature:
                logger.error("保存交易失敗: 缺少簽名")
                return False
            
            # 交易去重檢查
            current_time = time.time()
            transaction_key = f"{signature}_{wallet_address}_{token_address}_{pool_address}"
            
            # 定期清理過期的交易緩存
            if current_time - self._last_transaction_cleanup > 3600:  # 每小時清理一次
                self._processed_transactions.clear()
                self._last_transaction_cleanup = current_time
                logger.debug("已清理交易處理緩存")
            
            # 檢查是否已處理過相同的交易
            if transaction_key in self._processed_transactions:
                logger.debug(f"跳過重複交易: {transaction_key}")
                return True  # 返回True表示"處理成功"，但實際上是跳過了
            
            # 添加到已處理集合
            self._processed_transactions.add(transaction_key)
        
            # 修正 transaction_time 的毫秒問題 (轉換為秒)
            if transaction_time and transaction_time > 10**12:  # 判斷是否為毫秒時間戳 (13位數)
                transaction["transaction_time"] = transaction_time // 1000
                logger.info(f"轉換毫秒時間戳 {transaction_time} 為秒 {transaction['transaction_time']}")
        
            with self.session_factory() as session:
                try:
                    # 獲取所有需要的代幣信息
                    token_address = transaction.get("token_address")
                    from_token_address = transaction.get("from_token_address")
                    dest_token_address = transaction.get("dest_token_address")
                    wallet_address = transaction.get("wallet_address")
                    
                    # 先檢查錢包餘額緩存
                    cache_key = f"wallet_sol_balance:{wallet_address}"
                    cached_balance = await cache_service.get(cache_key)
                    
                    if cached_balance is not None:
                        wallet_balance = self._convert_to_float(cached_balance)
                        logger.debug(f"使用緩存的錢包餘額: {wallet_address} = {wallet_balance} SOL")
                    else:
                        # 緩存未命中，調用RPC獲取餘額
                        try:
                            sol_balance_result = await self.get_sol_balance(wallet_address)
                            if sol_balance_result and sol_balance_result.get("balance"):
                                wallet_balance = sol_balance_result.get("balance", {}).get("float", 0)
                                # 將餘額緩存 5 分鐘 (300秒)
                                await cache_service.set(cache_key, wallet_balance, expiry=300)
                                logger.debug(f"已緩存錢包餘額: {wallet_address} = {wallet_balance} SOL")
                            else:
                                logger.warning(f"無法獲取 {wallet_address} 的餘額，使用預設值 0")
                                wallet_balance = 0
                        except Exception as e:
                            logger.error(f"獲取餘額時出錯: {e}")
                            wallet_balance = transaction.get("wallet_balance", 0) or 0  # 嘗試使用傳入的值，否則使用0
                    
                    # 更新交易的錢包餘額
                    transaction["wallet_balance"] = wallet_balance
                    
                    # 獲取代幣信息
                    token_info = self.get_token_info_safely(token_address)
                    from_token_info = self.get_token_info_safely(from_token_address)
                    dest_token_info = self.get_token_info_safely(dest_token_address)
                    
                    # 檢查代幣名稱和符號是否為空
                    token_name = transaction.get("token_name") or token_info.get("symbol") or token_info.get("name")
                    token_symbol = token_info.get("symbol")
                    
                    # 如果代幣名稱和符號都為空，跳過保存交易
                    if not token_name and not token_symbol:
                        logger.warning(f"代幣信息為空，跳過保存交易: {token_address}")
                        return True  # 返回True表示"處理成功"，但實際上是跳過了
                    
                    # 更新交易數據
                    if token_info:
                        if "token_name" not in transaction or not transaction["token_name"]:
                            transaction["token_name"] = token_name
                        
                        if "token_icon" not in transaction or not transaction["token_icon"]:
                            transaction["token_icon"] = token_info.get("icon") or token_info.get("url")
                        
                        # 計算marketcap (如果有supply_float)
                        if "marketcap" not in transaction or not transaction["marketcap"]:
                            price = self._convert_to_float(transaction.get("price", 0))
                            # 從token_info獲取supply_float，如果沒有則使用預設值
                            supply_float = token_info.get("supply_float", 1000000000)  # 預設供應量為10億
                            
                            # 添加安全性檢查，避免異常大的市值
                            if price and supply_float:
                                marketcap = price * supply_float
                                # 設置一個合理的上限，例如10萬億美元(10^13)
                                if marketcap > 10e13:
                                    logger.warning(f"檢測到異常市值: {marketcap}，代幣: {token_address}, 價格: {price}, 供應量: {supply_float}")
                                    marketcap = 0
                                transaction["marketcap"] = marketcap
                            else:
                                transaction["marketcap"] = 0  # 如果沒有價格，marketcap設為0而不是null
                    
                    # 更新代幣符號
                    if from_token_info and "from_token_symbol" not in transaction:
                        transaction["from_token_symbol"] = from_token_info.get("symbol")
                    
                    if dest_token_info and "dest_token_symbol" not in transaction:
                        transaction["dest_token_symbol"] = dest_token_info.get("symbol")

                    transaction_type = transaction.get("transaction_type")
                    amount = self._convert_to_float(transaction.get("amount", 0))
                    price = self._convert_to_float(transaction.get("price", 0))
                    value = self._convert_to_float(transaction.get("value", 0))
                    
                    if transaction_type == "buy" or transaction_type == "build" and self._convert_to_float(value) > 0:
                        # 使用剛取得的wallet_balance計算持倉百分比
                        value_float = self._convert_to_float(value)
                        wallet_balance_float = self._convert_to_float(wallet_balance)
                        holding_pct = value_float / (wallet_balance_float + value_float) * 100 if wallet_balance_float + value_float > 0 else 0
                        transaction["holding_percentage"] = min(100, max(-100, holding_pct))
                    elif transaction_type == "sell" or transaction_type == "clean" and self._convert_to_float(amount) > 0:
                        token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                        current_amount = self._convert_to_float(token_buy_data.get("total_amount", 0))
                        amount_float = self._convert_to_float(amount)
                        if current_amount > 0:
                            holding_pct = (amount_float / current_amount) * 100
                            transaction["holding_percentage"] = min(100, max(-100, holding_pct))
                        else:
                            transaction["holding_percentage"] = 100
                    
                    if transaction_type == "sell" or transaction_type == "clean":
                        # 使用新的 WalletTokenState 緩存機制獲取平均買入價格
                        wallet_token_state = self.get_wallet_token_state(wallet_address, token_address)
                        avg_buy_price = wallet_token_state["current_avg_buy_price"]
                        
                        # 如果緩存中沒有有效的平均買入價格，嘗試從數據庫獲取
                        if avg_buy_price <= 0:
                            token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                            avg_buy_price = self._convert_to_float(token_buy_data.get("avg_buy_price", 0))
                        
                        sell_price = self._convert_to_float(transaction.get("price", 0))
                        
                        if avg_buy_price > 0 and amount > 0:
                            realized_profit, realized_profit_percentage = self.calculate_realized_profit(token_address, amount, sell_price, avg_buy_price)
                            realized_profit_percentage = min(1000, max(-100, realized_profit_percentage))
                            transaction["realized_profit"] = realized_profit
                            transaction["realized_profit_percentage"] = realized_profit_percentage
                        else:
                            # 降低日志级别，避免重复警告
                            logger.debug(f"無法計算利潤: avg_buy_price={avg_buy_price}, amount={amount}, wallet={wallet_address}, token={token_address}")
                            transaction["realized_profit"] = 0
                            transaction["realized_profit_percentage"] = 0
                    else:
                        # 買入時這些值為0
                        transaction["realized_profit"] = 0
                        transaction["realized_profit_percentage"] = 0

                    time_now = now_utc8()
                    formatted_time = time_now.strftime('%Y-%m-%d %H:%M:%S.%f')

                    if transaction_type == "buy" or transaction_type == "build" and amount > 0 and price > 0:
                        token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                        current_amount = self._convert_to_float(token_buy_data.get("total_amount", 0))
                        current_value = self._convert_to_float(token_buy_data.get("total_value", 0))
                        
                        # 計算新的總數量和總值
                        new_amount = current_amount + amount
                        new_value = current_value + (amount * price)
                        
                        # 計算新的平均買入價格
                        if new_amount > 0:
                            avg_buy_price = new_value / new_amount
                        else:
                            avg_buy_price = 0
                            
                        buy_data = {
                            "total_amount": new_amount,
                            "total_value": new_value,
                            "avg_buy_price": avg_buy_price,
                            "last_buy_time": transaction.get("transaction_time"),
                            "last_price": price,
                            "is_buy": True
                        }
                        self.update_token_buy_data(wallet_address, token_address, buy_data)
                    
                    elif transaction_type == "sell" or transaction_type == "clean" and amount > 0 and price > 0:
                        token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                        current_amount = self._convert_to_float(token_buy_data.get("total_amount", 0))
                        current_value = self._convert_to_float(token_buy_data.get("total_value", 0))
                        avg_buy_price = self._convert_to_float(token_buy_data.get("avg_buy_price", 0))
                        
                        # 確保有買入記錄
                        if current_amount > 0 and avg_buy_price > 0:
                            # 計算賣出後剩餘數量和價值
                            remaining_amount = max(0, current_amount - amount)
                            
                            if current_amount > 0:
                                # 按比例減少價值
                                value_reduction = (amount / current_amount) * current_value
                                remaining_value = max(0, current_value - value_reduction)
                            else:
                                remaining_value = 0
                            
                            # 更新平均買入價格（如果還有剩餘代幣）
                            if remaining_amount > 0:
                                new_avg_buy_price = remaining_value / remaining_amount
                            else:
                                new_avg_buy_price = 0
                            
                            # 更新 TokenBuyData
                            buy_data = {
                                "total_amount": remaining_amount,
                                "total_value": remaining_value,
                                "avg_buy_price": new_avg_buy_price,
                                "last_price": price,
                                "is_buy": False
                            }
                            self.update_token_buy_data(wallet_address, token_address, buy_data)
                            
                    # 最終檢查代幣名稱是否為空
                    final_token_name = transaction.get("token_name")
                    if not final_token_name or final_token_name.strip() == "":
                        logger.warning(f"代幣名稱為空，跳過保存交易: {token_address}, signature: {signature}")
                        return True  # 返回True表示"處理成功"，但實際上是跳過了
                    
                    # 準備交易資料
                    transaction_data = {
                        "wallet_address": wallet_address,
                        "wallet_balance": wallet_balance,
                        "signature": signature,
                        "transaction_time": transaction.get("transaction_time"),
                        "transaction_type": transaction_type,
                        "token_address": token_address,
                        "token_name": final_token_name,
                        "token_icon": transaction.get("token_icon"),
                        "marketcap": transaction.get("marketcap"),
                        "amount": self._convert_to_float(transaction.get("amount")),
                        "value": self._convert_to_float(transaction.get("value")),
                        "price": self._convert_to_float(transaction.get("price")),
                        "holding_percentage": self._convert_to_float(transaction.get("holding_percentage", 0)),
                        "realized_profit": self._convert_to_float(transaction.get("realized_profit", 0)),
                        "realized_profit_percentage": self._convert_to_float(transaction.get("realized_profit_percentage", 0)),
                        "chain": transaction.get("chain", "SOLANA"),
                        "chain_id": transaction.get("chain_id", 501),
                        "from_token_address": from_token_address,
                        "from_token_symbol": transaction.get("from_token_symbol"),
                        "from_token_amount": self._convert_to_float(transaction.get("from_token_amount")),
                        "dest_token_address": dest_token_address,
                        "dest_token_symbol": transaction.get("dest_token_symbol"),
                        "dest_token_amount": self._convert_to_float(transaction.get("dest_token_amount")),
                        "time": formatted_time
                    }
                    
                    # 使用UPSERT模式處理分區表插入
                    # 注意：插入到主表Transaction，PostgreSQL會自動路由到正確的分區
                    try:
                        # 使用ON CONFLICT進行UPSERT操作
                        insert_stmt = insert(Transaction).values(**transaction_data)
                        
                        # 使用ON CONFLICT DO UPDATE，明確指定衝突的列
                        insert_stmt = insert_stmt.on_conflict_do_update(
                            index_elements=['signature', 'wallet_address', 'token_address', 'transaction_time'],
                            set_=transaction_data
                        )
                        
                        session.execute(insert_stmt)
                        session.commit()
                        logger.info(f"成功保存/更新交易: {signature}, 錢包: {wallet_address}, 代幣: {token_address}")

                        # 構建 API 發送數據
                        smart_token_event_data = {
                            "network": "SOLANA",
                            "tokenAddress": token_address,
                            "poolAddress": pool_address,
                            "smartAddress": wallet_address,
                            "transactionType": transaction_type,
                            "transactionFromAmount": str(transaction.get("from_token_amount", "0")),
                            "transactionFromToken": transaction.get("from_token_address", ""),
                            "transactionToAmount": str(transaction.get("dest_token_amount", "0")),
                            "transactionToToken": transaction.get("dest_token_address", ""),
                            "transactionPrice": str(price),
                            "totalPnl": str(transaction.get("realized_profit", "0")),
                            "transactionTime": str(transaction.get("transaction_time")),
                            "signature": signature,
                            "brand": "BYD"
                        }
                        await self.add_event_to_pending(smart_token_event_data, signature)

                        return True
                        
                    except sqlalchemy.exc.IntegrityError as e:
                        session.rollback()
                        error_msg = str(e)
                        # 處理特定的錯誤類型
                        if "duplicate key value violates unique constraint" in error_msg:
                            logger.info(f"交易已存在，使用替代方法更新: {signature}")
                            
                            # 如果UPSERT失敗，嘗試使用先查詢後更新的方式
                            try:
                                # 使用月份信息構建可能的分區表名稱格式
                                tx_time = transaction.get("transaction_time")
                                tx_date = datetime.fromtimestamp(tx_time) if tx_time else now_utc8()
                                
                                # 嘗試更新操作
                                update_stmt = update(Transaction).where(
                                    and_(
                                        Transaction.signature == signature,
                                        Transaction.wallet_address == wallet_address,
                                        Transaction.token_address == token_address
                                    )
                                ).values(**transaction_data)
                                
                                result = session.execute(update_stmt)
                                
                                if result.rowcount == 0:
                                    # 如果更新影響0行，可能是因為記錄不存在，嘗試插入
                                    logger.info(f"無法更新交易，嘗試直接插入: {signature}")
                                    # 避免主鍵衝突，先檢查記錄是否存在
                                    check_stmt = select(exists().where(
                                        and_(
                                            Transaction.signature == signature,
                                            Transaction.wallet_address == wallet_address,
                                            Transaction.token_address == token_address
                                        )
                                    ))
                                    exists_record = session.execute(check_stmt).scalar()
                                    
                                    if not exists_record:
                                        session.add(Transaction(**transaction_data))
                                
                                session.commit()
                                logger.info(f"成功使用替代方法保存/更新交易: {signature}")
                                return True
                                
                            except Exception as inner_e:
                                session.rollback()
                                logger.error(f"使用替代方法更新交易時出錯: {inner_e}")
                                return False
                        else:
                            logger.error(f"保存交易時出現完整性錯誤: {error_msg}")
                            return False
                
                except Exception as e:
                    session.rollback()
                    logger.error(f"保存交易時出現錯誤: {e}")
                    return False
                    
        except Exception as e:
            logger.exception(f"保存/更新交易 {signature} 時發生錯誤: {e}")
            return False
        
    async def update_wallet_holdings(self, wallet_address: str, token_stats: Dict[str, Any]) -> bool:
        """
        根据钱包分析结果更新 Holding 表 - 增强容错和批量处理
        """
        logger.info(f"更新錢包 {wallet_address} 的持倉記錄")
        
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法更新持倉記錄，因數據庫未就緒")
            return False
        
        try:
            # 获取所有代币信息
            tokens = token_stats.get("tokens", {})
            current_time = int(time.time())
            
            # 如果没有代币数据，只需标记所有现有持仓为已清算
            if not tokens:
                with self.session_factory() as session:
                    try:
                        session.execute(
                            update(Holding).where(
                                Holding.wallet_address == wallet_address
                            ).values(is_cleared=True)
                        )
                        session.commit()
                        logger.info(f"已标记 {wallet_address} 的所有持仓为已清算 (没有活跃代币)")
                        return True
                    except Exception as e:
                        logger.error(f"标记持仓已清算时出错: {str(e)}")
                        session.rollback()
                        return False
            
            # 优化: 批量查询所有现有持仓
            with self.session_factory() as session:
                try:
                    # 先将所有持仓标记为已清算
                    session.execute(
                        update(Holding).where(
                            Holding.wallet_address == wallet_address
                        ).values(is_cleared=True)
                    )
                    
                    # 查询现有持仓记录
                    existing_holdings = session.execute(
                        select(Holding).where(
                            Holding.wallet_address == wallet_address
                        )
                    ).scalars().all()
                    
                    # 建立地址到持仓的映射
                    holding_map = {holding.token_address: holding for holding in existing_holdings}
                    
                    # 收集要新增的持仓记录
                    new_holdings = []
                    updated_count = 0
                    
                    # 处理每个代币
                    for token_address, token_data in tokens.items():
                        # 只处理有正余额的代币 (实际持有的代币)
                        balance = token_data.get("balance", 0)
                        if balance <= 0:
                            continue
                        
                        # 获取代币购买数据
                        buy_data = self.get_token_buy_data(wallet_address, token_address)
                        avg_buy_price = buy_data.get("avg_buy_price", 0)
                        total_cost = buy_data.get("total_cost", 0)
                        total_sell = buy_data.get("historical_total_sell_value", 0)
                        
                        # 获取代币信息
                        token_info = self.get_token_info_safely(token_address)
                        current_price = 0.0  # 可以在此添加获取实时价格的逻辑
                        
                        # 计算各种指标
                        value = balance * current_price if current_price > 0 else 0.0
                        value_USDT = value
                        unrealized_profits = 0.0
                        
                        if avg_buy_price > 0 and balance > 0:
                            unrealized_profits = value - (avg_buy_price * balance)
                        
                        # 计算总盈亏
                        pnl = unrealized_profits
                        pnl_percentage = 0.0
                        if total_cost > 0:
                            pnl_percentage = (pnl / total_cost) * 100
                            pnl_percentage = max(-100, pnl_percentage)  # 最小为-100%
                        
                        # 更新或创建持仓记录
                        if token_address in holding_map:
                            # 更新现有记录
                            holding = holding_map[token_address]
                            holding.amount = float(balance)
                            holding.value = float(value)
                            holding.value_usdt = float(value_USDT)
                            holding.unrealized_profits = float(unrealized_profits)
                            holding.pnl = float(pnl)
                            holding.pnl_percentage = float(pnl_percentage)
                            holding.avg_price = float(avg_buy_price) if avg_buy_price else 0.0
                            holding.is_cleared = False
                            holding.cumulative_cost = float(total_cost) if total_cost else 0.0
                            holding.cumulative_profit = float(total_sell) if total_sell else 0.0
                            holding.last_transaction_time = token_data.get("last_transaction_time", current_time)
                            holding.time = now_utc8()
                            
                            # 更新代币信息
                            holding.token_name = token_data.get("symbol") or token_info.get("symbol", None)
                            holding.token_icon = token_info.get("icon", "")
                            holding.marketcap = token_info.get("marketcap", 0.0)
                            
                            updated_count += 1
                        else:
                            # 创建新记录
                            new_holding = Holding(
                                wallet_address=wallet_address,
                                token_address=token_address,
                                token_name=token_data.get("symbol") or token_info.get("symbol", "Unknown"),
                                token_icon=token_info.get("icon", ""),
                                chain="SOLANA",
                                amount=float(balance),
                                value=float(value),
                                value_usdt=float(value_USDT),
                                unrealized_profits=float(unrealized_profits),
                                pnl=float(pnl),
                                pnl_percentage=float(pnl_percentage),
                                avg_price=float(avg_buy_price) if avg_buy_price else 0.0,
                                marketcap=token_info.get("marketcap", 0.0),
                                is_cleared=False,
                                cumulative_cost=float(total_cost) if total_cost else 0.0,
                                cumulative_profit=float(total_sell) if total_sell else 0.0,
                                last_transaction_time=token_data.get("last_transaction_time", current_time),
                                time=now_utc8()
                            )
                            new_holdings.append(new_holding)
                    
                    # 批量添加新持仓
                    if new_holdings:
                        session.add_all(new_holdings)
                    
                    # 提交变更
                    session.commit()
                    logger.info(f"成功更新錢包 {wallet_address} 的持倉記錄: {updated_count} 更新, {len(new_holdings)} 新增")
                    return True
                
                except Exception as e:
                    logger.exception(f"更新持倉記錄時發生錯誤: {str(e)}")
                    session.rollback()
                    
                    # 在完全失败的情况下，尝试更简单的更新方式
                    try:
                        # 只标记为已清算，不进行其他更新
                        session.execute(
                            update(Holding).where(
                                Holding.wallet_address == wallet_address
                            ).values(is_cleared=True)
                        )
                        session.commit()
                        logger.warning(f"回退到简单更新: 已标记 {wallet_address} 的所有持仓为已清算")
                        return False
                    except:
                        logger.error("回退更新也失败")
                        return False
        
        except Exception as e:
            logger.exception(f"更新持倉記錄時發生錯誤: {str(e)}")
            return False
        
    def update_token_buy_data(self, wallet_address: str, token_address: str, buy_data: Dict) -> bool:
        """更新代幣購買數據"""
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法更新 {wallet_address}/{token_address} 購買數據，因數據庫未就緒")
            return False

        try:
            # 調試日誌，記錄詳細信息
            logger.info(f"更新代幣購買數據: 地址={wallet_address}, 代幣={token_address}, 數據={buy_data}")
            
            # These are the new absolute values for the current holding state, pre-calculated by save_transaction
            new_total_amount = Decimal(str(buy_data.get("total_amount", 0)))
            new_total_cost = Decimal(str(buy_data.get("total_value", 0))) # 'total_value' from buy_data is the new total_cost
            new_avg_buy_price = Decimal(str(buy_data.get("avg_buy_price", 0)))
            is_buy_event = buy_data.get("is_buy", True) # To determine if it's a buy for historicals on new record

            logger.info(f"解析後的新狀態: total_amount={new_total_amount}, total_cost={new_total_cost}, avg_buy_price={new_avg_buy_price}")

            with self.session_factory() as session:
                # 修復：在查詢條件中包含date字段，避免重複記錄
                current_date = now_utc8().date()
                
                # 使用 upsert 操作來避免唯一約束違反
                from sqlalchemy.dialects.postgresql import insert
                
                # 準備 upsert 數據
                upsert_data = {
                    'wallet_address': wallet_address,
                    'token_address': token_address,
                    'chain': "SOLANA",
                    'chain_id': 501,
                    'total_amount': float(new_total_amount),
                    'total_cost': float(new_total_cost),
                    'avg_buy_price': float(new_avg_buy_price),
                    'updated_at': now_utc8(),
                    'date': current_date
                }
                
                # 如果是買入事件且數量大於0，設置歷史數據
                if is_buy_event and new_total_amount > 0:
                    upsert_data.update({
                        'position_opened_at': int(now_utc8().timestamp()),
                        'historical_total_buy_amount': float(new_total_amount),
                        'historical_total_buy_cost': float(new_total_cost)
                    })
                
                # 嘗試使用 upsert 操作
                try:
                    stmt = insert(TokenBuyData).values(upsert_data)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['wallet_address', 'token_address', 'chain', 'date'],
                        set_={
                            'total_amount': stmt.excluded.total_amount,
                            'total_cost': stmt.excluded.total_cost,
                            'avg_buy_price': stmt.excluded.avg_buy_price,
                            'updated_at': stmt.excluded.updated_at
                        }
                    )
                    
                    session.execute(stmt)
                    session.commit()
                    
                    logger.info(f"代幣購買數據已成功更新/插入")
                    return True
                    
                except sqlalchemy.exc.IntegrityError as e:
                    session.rollback()
                    error_msg = str(e)
                    
                    # 如果是唯一約束衝突，嘗試使用替代方法
                    if "duplicate key value violates unique constraint" in error_msg:
                        logger.info(f"檢測到唯一約束衝突，使用替代方法更新: {wallet_address}/{token_address}")
                        
                        try:
                            # 先查詢現有記錄
                            existing_record = session.execute(
                                select(TokenBuyData).where(
                                    and_(
                                        TokenBuyData.wallet_address == wallet_address,
                                        TokenBuyData.token_address == token_address,
                                        TokenBuyData.chain == "SOLANA",
                                        TokenBuyData.date == current_date
                                    )
                                )
                            ).scalar_one_or_none()
                            
                            if existing_record:
                                # 更新現有記錄
                                existing_record.total_amount = float(new_total_amount)
                                existing_record.total_cost = float(new_total_cost)
                                existing_record.avg_buy_price = float(new_avg_buy_price)
                                existing_record.updated_at = now_utc8()
                                
                                if is_buy_event and new_total_amount > 0:
                                    existing_record.position_opened_at = int(now_utc8().timestamp())
                                    existing_record.historical_total_buy_amount = float(new_total_amount)
                                    existing_record.historical_total_buy_cost = float(new_total_cost)
                                
                                session.commit()
                                logger.info(f"成功更新現有記錄: {wallet_address}/{token_address}")
                                return True
                            else:
                                # 如果記錄不存在，嘗試插入（可能是分區表問題）
                                logger.info(f"記錄不存在，嘗試直接插入: {wallet_address}/{token_address}")
                                
                                # 創建新記錄
                                new_record = TokenBuyData(**upsert_data)
                                session.add(new_record)
                                session.commit()
                                
                                logger.info(f"成功插入新記錄: {wallet_address}/{token_address}")
                                return True
                                
                        except Exception as inner_e:
                            session.rollback()
                            logger.error(f"使用替代方法更新時出錯: {inner_e}")
                            return False
                    else:
                        logger.error(f"數據庫完整性錯誤: {error_msg}")
                        return False

        except Exception as e:
            logger.exception(f"更新 {wallet_address}/{token_address} 購買數據時發生錯誤: {e}")
            return False
        
    async def update_wallet_summary(
        self, 
        wallet_address: str,
        twitter_name: Optional[str] = None,
        twitter_username: Optional[str] = None,
        is_smart_wallet: bool = False
    ) -> bool:
        """
        更新錢包摘要信息
        
        Args:
            wallet_address: 錢包地址
            twitter_name: Twitter 名稱（可選）
            twitter_username: Twitter 用戶名（可選）
            is_smart_wallet: 是否為智能錢包（可選）
            
        Returns:
            bool: 更新是否成功
        """
        try:
            from app.services.wallet_summary_service import wallet_summary_service
            
            # 計算交易統計數據
            with self.session_factory() as session:
                tx_stats = await self._calculate_wallet_transaction_stats(wallet_address, session)
            # print(f"tx_stats: {tx_stats}")
            # 更新錢包摘要
            success = await wallet_summary_service.update_full_summary(
                wallet_address=wallet_address,
                tx_stats=tx_stats,
                twitter_name=twitter_name,
                twitter_username=twitter_username,
                is_smart_wallet=is_smart_wallet
            )
            return success
        except Exception as e:
            logger.exception(f"更新錢包摘要時發生錯誤: {e}")
            return False
        
    async def _calculate_wallet_transaction_stats(self, wallet_address: str, session) -> Dict[str, Any]:
        """計算錢包交易統計資訊"""
        try:
            # 獲取當前時間
            current_time = int(time.time())
            
            # 定義時間範圍
            time_ranges = {
                "1d": current_time - 86400,      # 1天
                "7d": current_time - 86400 * 7,  # 7天
                "30d": current_time - 86400 * 30 # 30天
            }
            
            # 結果統計
            stats = {}
            
            # 查詢該錢包的所有交易記錄
            transactions = session.execute(
                select(Transaction).where(
                    Transaction.wallet_address == wallet_address
                ).order_by(Transaction.transaction_time.desc())
            ).scalars().all()
            
            if not transactions:
                logger.info(f"錢包 {wallet_address} 沒有交易記錄")
                # 返回預設值而不是空字典
                return {
                    "last_transaction_time": None,
                    "total_transaction_num_30d": 0,
                    "total_transaction_num_7d": 0,
                    "total_transaction_num_1d": 0,
                    "buy_num_30d": 0,
                    "buy_num_7d": 0,
                    "buy_num_1d": 0,
                    "sell_num_30d": 0,
                    "sell_num_7d": 0,
                    "sell_num_1d": 0,
                    "pnl_30d": 0,
                    "pnl_7d": 0,
                    "pnl_1d": 0,
                    "pnl_percentage_30d": 0,
                    "pnl_percentage_7d": 0,
                    "pnl_percentage_1d": 0,
                    "unrealized_profit_30d": 0,
                    "unrealized_profit_7d": 0,
                    "unrealized_profit_1d": 0,
                    "total_cost_30d": 0,
                    "total_cost_7d": 0,
                    "total_cost_1d": 0,
                    "avg_cost_30d": 0,
                    "avg_cost_7d": 0,
                    "avg_cost_1d": 0,
                    "avg_realized_profit_30d": 0,
                    "avg_realized_profit_7d": 0,
                    "avg_realized_profit_1d": 0,
                    "win_rate_30d": 0,
                    "win_rate_7d": 0,
                    "win_rate_1d": 0,
                    "distribution_lt50_30d": 0,
                    "distribution_0to50_30d": 0,
                    "distribution_0to200_30d": 0,
                    "distribution_200to500_30d": 0,
                    "distribution_gt500_30d": 0,
                    "distribution_lt50_percentage_30d": 0,
                    "distribution_0to50_percentage_30d": 0,
                    "distribution_0to200_percentage_30d": 0,
                    "distribution_200to500_percentage_30d": 0,
                    "distribution_gt500_percentage_30d": 0,
                    "distribution_lt50_7d": 0,
                    "distribution_0to50_7d": 0,
                    "distribution_0to200_7d": 0,
                    "distribution_200to500_7d": 0,
                    "distribution_gt500_7d": 0,
                    "distribution_lt50_percentage_7d": 0,
                    "distribution_0to50_percentage_7d": 0,
                    "distribution_0to200_percentage_7d": 0,
                    "distribution_200to500_percentage_7d": 0,
                    "distribution_gt500_percentage_7d": 0,
                    "asset_multiple": 0,
                    "pnl_pic_30d": "",
                    "pnl_pic_7d": "",
                    "pnl_pic_1d": "",
                    "token_list": None
                }
            
            # 最後交易時間
            stats["last_transaction_time"] = transactions[0].transaction_time
            
            # 將交易按時間範圍分組
            period_transactions = {
                "1d": [],
                "7d": [],
                "30d": []
            }
            
            for tx in transactions:
                for period, start_time in time_ranges.items():
                    if tx.transaction_time >= start_time:
                        period_transactions[period].append(tx)
            
            # 獲取TokenBuyData記錄
            token_buy_records = session.execute(
                select(TokenBuyData).where(
                    TokenBuyData.wallet_address == wallet_address
                )
            ).scalars().all()
            
            # 計算交易統計資料
            for period in ["1d", "7d", "30d"]:
                # 交易總數
                stats[f"total_transaction_num_{period}"] = len(period_transactions[period])
                
                # 買入/賣出交易數（包括build和clean）
                buy_txs = [tx for tx in period_transactions[period] if tx.transaction_type in ['buy', 'build']]
                sell_txs = [tx for tx in period_transactions[period] if tx.transaction_type in ['sell', 'clean']]
                stats[f"buy_num_{period}"] = len(buy_txs)
                stats[f"sell_num_{period}"] = len(sell_txs)
                
                # 計算PNL（包括clean交易）
                period_realized_profit = sum(tx.realized_profit or 0 for tx in sell_txs)
                stats[f"pnl_{period}"] = period_realized_profit
                
                # 計算未實現收益
                unrealized_profit = 0
                for record in token_buy_records:
                    if record.total_amount > 0 and record.avg_buy_price > 0:
                        # 獲取當前價格 (這裡可能需要額外的函數來獲取)
                        current_price = await self._get_token_current_price(record.token_address)
                        token_unrealized_profit = (current_price - record.avg_buy_price) * record.total_amount
                        unrealized_profit += token_unrealized_profit
                
                stats[f"unrealized_profit_{period}"] = unrealized_profit
            
            # 計算成本和利潤比例
            unique_tokens = set(tx.token_address for tx in transactions)
            total_tokens = len(unique_tokens)
            
            for period in ["1d", "7d", "30d"]:
                period_txs = period_transactions[period]
                period_tokens = set(tx.token_address for tx in period_txs)
                
                # 計算平均成本（包括build和buy交易）
                if period_tokens:
                    # 計算該時期所有buy和build交易的總成本
                    total_cost = sum(tx.value or 0 for tx in period_txs if tx.transaction_type in ['buy', 'build'])
                    stats[f"total_cost_{period}"] = total_cost
                    stats[f"avg_cost_{period}"] = total_cost / len(period_tokens) if period_tokens else 0
                else:
                    stats[f"total_cost_{period}"] = 0
                    stats[f"avg_cost_{period}"] = 0
                
                # 計算PNL百分比
                if stats[f"total_cost_{period}"] > 0:
                    stats[f"pnl_percentage_{period}"] = (stats[f"pnl_{period}"] / stats[f"total_cost_{period}"]) * 100
                else:
                    stats[f"pnl_percentage_{period}"] = 0
                
                # 計算平均已實現利潤
                if period_tokens:
                    stats[f"avg_realized_profit_{period}"] = stats[f"pnl_{period}"] / len(period_tokens)
                else:
                    stats[f"avg_realized_profit_{period}"] = 0
                
                # 計算勝率（考慮clean交易）
                profitable_tokens = 0
                for token in period_tokens:
                    token_txs = [tx for tx in period_txs if tx.token_address == token]
                    token_profit = sum(tx.realized_profit or 0 for tx in token_txs if tx.transaction_type in ['sell', 'clean'])
                    if token_profit > 0:
                        profitable_tokens += 1
                
                stats[f"win_rate_{period}"] = (profitable_tokens / len(period_tokens)) * 100 if period_tokens else 0
                
                # 計算PNL分佈
                # 需要先獲取每個代幣的PNL百分比
                token_pnl_percentages = []
                for token in period_tokens:
                    token_txs = [tx for tx in period_txs if tx.token_address == token]
                    buy_value = sum(tx.value or 0 for tx in token_txs if tx.transaction_type in ['buy', 'build'])
                    token_profit = sum(tx.realized_profit or 0 for tx in token_txs if tx.transaction_type in ['sell', 'clean'])
                    
                    if buy_value > 0:
                        pnl_percentage = (token_profit / buy_value) * 100
                        token_pnl_percentages.append(pnl_percentage)
                
                # 計算PNL分佈
                lt50_count = sum(1 for p in token_pnl_percentages if p < -50)  # 小於0的（虧損）
                from0to50_count = sum(1 for p in token_pnl_percentages if -50 <= p <= 0)  # 0-50%
                from50to200_count = sum(1 for p in token_pnl_percentages if 0 < p <= 200)  # 50-200%
                from200to500_count = sum(1 for p in token_pnl_percentages if 200 < p <= 500)  # 200-500%
                gt500_count = sum(1 for p in token_pnl_percentages if p > 500)  # 大於500%

                stats[f"distribution_lt50_{period}"] = lt50_count
                stats[f"distribution_0to50_{period}"] = from0to50_count
                stats[f"distribution_0to200_{period}"] = from50to200_count
                stats[f"distribution_200to500_{period}"] = from200to500_count
                stats[f"distribution_gt500_{period}"] = gt500_count

                total_count = len(token_pnl_percentages)
                # 計算分佈百分比
                if total_count > 0:
                    stats[f"distribution_lt50_percentage_{period}"] = (lt50_count / total_count) * 100
                    stats[f"distribution_0to50_percentage_{period}"] = (from0to50_count / total_count) * 100
                    stats[f"distribution_0to200_percentage_{period}"] = (from50to200_count / total_count) * 100
                    stats[f"distribution_200to500_percentage_{period}"] = (from200to500_count / total_count) * 100
                    stats[f"distribution_gt500_percentage_{period}"] = (gt500_count / total_count) * 100
                else:
                    stats[f"distribution_lt50_percentage_{period}"] = 0
                    stats[f"distribution_0to50_percentage_{period}"] = 0
                    stats[f"distribution_0to200_percentage_{period}"] = 0
                    stats[f"distribution_200to500_percentage_{period}"] = 0
                    stats[f"distribution_gt500_percentage_{period}"] = 0
            
            # 計算資產翻倍倍數 (使用30天PNL百分比)
            if "pnl_percentage_30d" in stats:
                stats["asset_multiple"] = stats["pnl_percentage_30d"] / 100
            else:
                stats["asset_multiple"] = 0
            
            # 生成每日PNL圖
            # 需要查詢近30天的每日PNL數據
            # 這裡需要額外的邏輯來統計每天的PNL
            for period, days in [("1d", 1), ("7d", 7), ("30d", 30)]:
                pnl_chart = await self._generate_daily_pnl_chart(wallet_address, days, session)
                stats[f"pnl_pic_{period}"] = pnl_chart
            
            # 生成最近交易的三個代幣列表
            recent_tokens = []
            if transactions:
                # 按交易時間排序
                sorted_txs = sorted(transactions, key=lambda tx: tx.transaction_time, reverse=True)
                # 獲取不重複的最近三個代幣
                for tx in sorted_txs:
                    if tx.token_address not in recent_tokens:
                        recent_tokens.append(tx.token_address)
                    if len(recent_tokens) == 3:
                        break
                
                stats["token_list"] = ",".join(recent_tokens) if recent_tokens else None
            else:
                stats["token_list"] = None
            
            return stats
        
        except Exception as e:
            logger.exception(f"計算錢包交易統計時發生錯誤: {e}")
            return {}
        
    async def _generate_daily_pnl_chart(self, wallet_address: str, days: int, session) -> str:
        """生成特定天數的每日PNL圖資料"""
        try:
            # 計算日期範圍
            end_date = now_utc8().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            start_date = end_date - timedelta(days=days-1)
            
            # 初始化每日PNL字典
            daily_pnl = {}
            current_date = start_date
            while current_date <= end_date:
                daily_pnl[current_date.strftime('%Y-%m-%d')] = 0
                current_date += timedelta(days=1)
            
            # 查詢該時間範圍內的所有賣出交易
            start_timestamp = int(start_date.timestamp())
            end_timestamp = int((end_date + timedelta(days=1)).timestamp())
            
            sell_txs = session.execute(
                select(Transaction).where(
                    (Transaction.wallet_address == wallet_address) &
                    (Transaction.transaction_type == 'sell' or Transaction.transaction_type == 'clean') &
                    (Transaction.transaction_time >= start_timestamp) &
                    (Transaction.transaction_time < end_timestamp)
                )
            ).scalars().all()
            
            # 按日期累計PNL
            for tx in sell_txs:
                tx_date = datetime.fromtimestamp(tx.transaction_time).strftime('%Y-%m-%d')
                if tx_date in daily_pnl:
                    daily_pnl[tx_date] += tx.realized_profit or 0
            
            # 轉換為逗號分隔的字符串，順序為從近到遠
            pnl_values = []
            current_date = end_date
            for _ in range(days):
                date_str = current_date.strftime('%Y-%m-%d')
                pnl_values.append(str(round(daily_pnl.get(date_str, 0), 2)))
                current_date -= timedelta(days=1)
            
            return ",".join(pnl_values)
        
        except Exception as e:
            logger.exception(f"生成每日PNL圖時發生錯誤: {e}")
            return ""
        
    async def _get_token_current_price(self, token_address: str) -> float:
        """獲取代幣當前價格，如無法獲取則使用平均賣出價格"""
        try:
            # 嘗試從API獲取價格
            token_info = await token_repository.get_token_info_async(token_address)
            if token_info and "price" in token_info and token_info["price"]:
                return float(token_info["price"])
            
            # 如果API無法獲取，則嘗試使用最近的賣出價格作為近似
            with self.session_factory() as session:
                latest_sell = session.execute(
                    select(Transaction)
                    .where(
                        (Transaction.token_address == token_address) &
                        (Transaction.transaction_type == 'sell' or Transaction.transaction_type == 'clean')
                    )
                    .order_by(Transaction.transaction_time.desc())
                    .limit(1)
                ).scalar_one_or_none()
                
                if latest_sell and latest_sell.price:
                    return latest_sell.price
            
            return 0.0
            
        except Exception as e:
            logger.error(f"獲取代幣 {token_address} 價格時出錯: {e}")
            return 0.0

    def calculate_realized_profit(self, token_address, amount, sell_price, avg_buy_price):
        """計算已實現利潤和利潤百分比"""
        if avg_buy_price <= 0 or amount <= 0:
            return 0, 0
        
        realized_profit = amount * (sell_price - avg_buy_price)
        buy_cost = amount * avg_buy_price
        
        realized_profit_percentage = 0
        if buy_cost > 0:
            realized_profit_percentage = (realized_profit / buy_cost) * 100
        
        logger.debug(f"計算已實現利潤: token={token_address}, amount={amount}, sell_price={sell_price}, avg_buy_price={avg_buy_price}")
        logger.debug(f"計算結果: realized_profit={realized_profit}, realized_profit_percentage={realized_profit_percentage}")
        
        return realized_profit, realized_profit_percentage

    def _remove_emoji(self, text: str) -> str:
        """移除文本中的表情符號"""
        if not text: return ""
        try:
            emoji_pattern = re.compile(
                "["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                u"\U00002702-\U000027B0"
                u"\U000024C2-\U0001F251"
                u"\U0001f926-\U0001f937"
                u"\U00010000-\U0010ffff"
                u"\u2640-\u2642"
                u"\u2600-\u2B55"
                u"\u200d"
                u"\u23cf"
                u"\u23e9"
                u"\u231a"
                u"\ufe0f"  # dingbats
                u"\u3030"
                "]+", flags=re.UNICODE)
            return emoji_pattern.sub(r'', text)
        except Exception: # Catch potential errors during regex
            return text # Return original text on error

    def _convert_to_float(self, value: Any) -> float:
        """安全轉換為 float，處理 None 和轉換錯誤"""
        if value is None:
            return 0.0
        try:
            if isinstance(value, Decimal):
                return float(value)
            return float(str(value))  # 先轉為字串再轉為浮點數，避免精度問題
        except (ValueError, TypeError):
            logger.warning(f"無法將值 '{value}' (類型: {type(value)}) 轉換為 float，返回 0.0")
            return 0.0

    async def get_sol_balance_async(self, wallet_address: str) -> dict:
        """
        非同步獲取 SOL 餘額
        :param wallet_address: 錢包地址
        :return: 包含 SOL 餘額的字典
        """
        try:            
            # 檢查是否有 RPC URL 設定
            rpc_url = getattr(settings, "SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
            logger.info(f"使用Solana RPC URL: {rpc_url}")
            
            # 確保URL格式正確
            # if not rpc_url.startswith(("http://", "https://")):
            #     rpc_url = f"https://{rpc_url}"
            #     logger.info(f"修正後的URL: {rpc_url}")
            # 創建客戶端
            client = AsyncClient(rpc_url)
            
            pubkey = Pubkey(base58.b58decode(wallet_address))
            balance_response = await client.get_balance(pubkey=pubkey)
            await client.close()
            sol_price = TransactionProcessor.get_sol_info("So11111111111111111111111111111111111111112").get("priceUsd", 0)
            balance = {
                'decimals': 9,
                'balance': {
                    'int': balance_response.value,
                    'float': float((balance_response.value / 10**9)* sol_price)
                }
            }
            return balance
        except Exception as e:
            logger.exception(f"獲取SOL餘額時出錯: {e}")
            return {"decimals": 9, "balance": {"int": 0, "float": 0.0}}

    # 同步版本，可以在非異步環境中使用
    async def get_sol_balance(self, wallet_address: str) -> dict:
        """
        获取 SOL 余额及其美元价值 - 快速失败，不重试
        """
        # 设定主 RPC URL
        primary_rpc = getattr(settings, "SOLANA_RPC_URL", "https://jadeite-greasily-xykrmnpgkw-dedicated.helius-rpc.com?api-key=90e3661e-4c5f-4e6b-af81-7aed2fd0f5be")
        default_balance = {"decimals": 9, "balance": {"int": 0, "float": 0.0}, "lamports": 0}

        # 将pubkey转换移到try块内，避免因无效钱包地址导致崩溃
        try:
            pubkey = Pubkey(base58.b58decode(wallet_address))
        except Exception as e:
            logger.error(f"转换钱包地址到Pubkey时失败: {str(e)}")
            return default_balance

        client = None
        try:
            # 确保URL格式正确
            if not primary_rpc.startswith(("http://", "https://")):
                primary_rpc = f"https://{primary_rpc}"

            # 创建客户端，设置较短的超时时间
            client = AsyncClient(primary_rpc, timeout=5)

            # 获取余额
            balance_response = await client.get_balance(pubkey=pubkey)

            # 获取 SOL 价格 (如果发生错误，使用默认值)
            sol_price = 125.0  # 默认值
            try:
                sol_info = self.get_sol_info("So11111111111111111111111111111111111111112")
                if sol_info and "priceUsd" in sol_info:
                    sol_price = sol_info.get("priceUsd", sol_price)
            except Exception as e:
                logger.warning(f"获取 SOL 价格时出错: {e}, 使用默认价格 {sol_price}")

            # 计算 SOL 余额和美元价值
            lamports = balance_response.value
            sol_balance = lamports / 10**9
            usd_value = sol_balance * sol_price

            logger.info(f"钱包 {wallet_address} 的 SOL 余额: {sol_balance:.6f} (${usd_value:.2f})")

            return {
                'decimals': 9,
                'balance': {
                    'int': sol_balance,
                    'float': float(usd_value)
                },
                'lamports': lamports
            }

        except Exception as e:
            logger.warning(f"获取钱包 {wallet_address} 的 SOL 余额失败: {str(e)}，直接返回0")
            return default_balance
        finally:
            # 关闭客户端连接
            if client:
                try:
                    await client.close()
                except:
                    pass

    @staticmethod
    def get_sol_info(token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，返回包括價格的數據。
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    return {
                        "symbol": data['pairs'][0].get('baseToken', {}).get('symbol', None),
                        "url": data['pairs'][0].get('url', "no url"),
                        "marketcap": data['pairs'][0].get('marketCap', 0),
                        "priceNative": float(data['pairs'][0].get('priceNative', 0)),
                        "priceUsd": float(data['pairs'][0].get('priceUsd', 0)),
                        "volume": data['pairs'][0].get('volume', 0),
                        "liquidity": data['pairs'][0].get('liquidity', 0)
                    }
            else:
                data = response.json()
        except Exception as e:
            return {"priceUsd": 125}
        return {"priceUsd": 125}

    def run_async_safely(self, coro):
        """安全運行異步協程，避免事件循環嵌套問題"""
        try:
            import asyncio
            import concurrent.futures
            
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 如果循環正在運行，使用線程池執行
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        future = pool.submit(asyncio.run, coro)
                        return future.result()
                else:
                    # 如果沒有循環運行，直接運行
                    return loop.run_until_complete(coro)
            except RuntimeError:
                # 當前線程沒有事件循環
                return asyncio.run(coro)
        except Exception as e:
            logger.error(f"運行異步協程時出錯: {e}")
            return None

    async def fetch_trades_from_db(self, wallet_address: str, min_timestamp: int = None) -> List[Dict[str, Any]]:
        """
        從 dex_query_v1.trades 表查詢錢包交易記錄
        
        Args:
            wallet_address: 錢包地址
            min_timestamp: 最小時間戳，用於過濾交易記錄
            
        Returns:
            交易記錄列表
        """
        try:
            # 初始化 Ian 資料庫連接
            if not self._init_ian_db_connection():
                logger.error("Ian 資料庫連接未初始化，無法查詢 trades 表")
                return []
            
            # 計算30天前的時間戳
            if min_timestamp is None:
                min_timestamp = int(time.time()) - (30 * 24 * 60 * 60)
            
            # 構建查詢 SQL
            trades_sql = text("""
                SELECT 
                    signer,
                    token_in,
                    token_out,
                    CASE WHEN side = 0 THEN token_in ELSE token_out END AS token_address,
                    CASE WHEN side = 0 THEN 'buy' ELSE 'sell' END AS side,
                    amount_in,
                    amount_out,
                    price,
                    price_usd,
                    decimals_in,
                    decimals_out,
                    timestamp,
                    tx_hash,
                    chain_id
                FROM dex_query_v1.trades
                WHERE signer = :wallet_address 
                AND timestamp >= :min_timestamp
                AND chain_id = 501
                ORDER BY timestamp DESC
            """)
            
            with self.ian_session_factory() as session:
                result = session.execute(trades_sql, {
                    'wallet_address': wallet_address,
                    'min_timestamp': min_timestamp
                })
                
                trades_data = []
                for row in result:
                    trades_data.append({
                        'signer': row.signer,
                        'token_in': row.token_in,
                        'token_out': row.token_out,
                        'token_address': row.token_address,
                        'side': row.side,
                        'amount_in': row.amount_in,
                        'amount_out': row.amount_out,
                        'price': row.price,
                        'price_usd': row.price_usd,
                        'decimals_in': row.decimals_in or 0,
                        'decimals_out': row.decimals_out or 0,
                        'timestamp': row.timestamp,
                        'tx_hash': row.tx_hash,
                        'chain_id': row.chain_id
                    })
                
                logger.info(f"從 Ian 資料庫查詢到 {len(trades_data)} 筆交易記錄 for {wallet_address}")
                return trades_data
                
        except Exception as e:
            logger.error(f"從 Ian 資料庫查詢交易記錄失敗: {str(e)}")
            return []
    
    def normalize_trade(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        標準化交易記錄，轉換為統一格式
        
        Args:
            row: 原始交易記錄
            
        Returns:
            標準化後的交易記錄
        """
        try:
            tx_hash = row['tx_hash']
            token_in = row['token_in']
            token_out = row['token_out']
            decimals_in = self._convert_to_float(row.get('decimals_in', 0) or 0)
            decimals_out = self._convert_to_float(row.get('decimals_out', 0) or 0)
            side = row['side']
            price = self._convert_to_float(row['price_usd'] if row.get('price_usd') and row['price_usd'] > 0 else row['price'])
            signer = row['signer']
            chain_id = row['chain_id']
            timestamp = row['timestamp']

            in_is_stable_or_wsol = token_in in self.STABLES or token_in == self.WSOL or token_in == self.SOL
            out_is_stable_or_wsol = token_out in self.STABLES or token_out == self.WSOL or token_out == self.SOL
            in_is_non_stable = not in_is_stable_or_wsol
            out_is_non_stable = not out_is_stable_or_wsol

            # 確保數值計算使用 float 類型
            amount_in = self._convert_to_float(row['amount_in']) / (10 ** decimals_in)
            amount_out = self._convert_to_float(row['amount_out']) / (10 ** decimals_out)

            if (in_is_stable_or_wsol and out_is_non_stable):
                token_address = token_out
                direction = 'buy'
                amount = amount_out
            elif (out_is_stable_or_wsol and in_is_non_stable):
                token_address = token_in
                direction = 'sell'
                amount = amount_in
            elif in_is_non_stable and out_is_non_stable:
                if side == 'buy':
                    token_address = token_out
                    direction = 'buy'
                    amount = amount_out
                else:
                    token_address = token_in
                    direction = 'sell'
                    amount = amount_in
            else:
                token_address = token_out
                direction = 'buy' if side == 'buy' else 'sell'
                amount = amount_out if direction == 'buy' else amount_in

            return {
                'tx_hash': tx_hash,
                'signer': signer,
                'token_address': token_address,
                'side': direction,
                'token_in': token_in,
                'token_out': token_out,
                'amount_in': amount_in,
                'amount_out': amount_out,
                'amount': amount,
                'price': price,
                'timestamp': timestamp,
                'chain_id': chain_id,
            }
        except Exception as e:
            logger.error(f"標準化交易記錄失敗: {str(e)}")
            return None
    
    def get_chain_name(self, chain_id: int) -> str:
        """根據 chain_id 獲取鏈名稱"""
        chain_mapping = {
            501: 'SOLANA',
            56: 'BSC',
            1: 'ETH',
            8453: 'BASE',
            195: 'TRON'
        }
        return chain_mapping.get(chain_id, 'UNKNOWN')
    
    async def convert_trades_to_transactions(self, trades: List[Dict[str, Any]], wallet_address: str) -> List[Dict[str, Any]]:
        """
        將交易記錄轉換為 Transaction 表格式
        
        Args:
            trades: 標準化後的交易記錄列表
            wallet_address: 錢包地址
            
        Returns:
            Transaction 格式的交易記錄列表
        """
        if not trades:
            return []
        
        try:
            # 獲取所有涉及的代幣地址
            all_token_addresses = set()
            for trade in trades:
                all_token_addresses.add(trade['token_address'])
                all_token_addresses.add(trade['token_in'])
                all_token_addresses.add(trade['token_out'])
            
            # 批量獲取代幣資訊
            token_info_dict = {}
            try:
                token_info_dict = await token_repository.get_multiple_token_info(list(all_token_addresses))
            except Exception as e:
                logger.warning(f"批量獲取代幣資訊失敗: {str(e)}")
            
            # 獲取錢包餘額
            wallet_balance = 0
            try:
                balance_info = await self.get_sol_balance_async(wallet_address)
                wallet_balance = balance_info.get('balance', {}).get('int', 0)
            except Exception as e:
                logger.warning(f"獲取錢包餘額失敗: {str(e)}")
            
            # 按錢包地址和代幣地址分組處理交易
            transaction_rows = []
            trades_df = pd.DataFrame(trades)
            
            for (signer, token_address), group in trades_df.groupby(['signer', 'token_address']):
                group = group.sort_values('timestamp')
                holding_amount = 0
                holding_cost = 0
                
                for _, trade_row in group.iterrows():
                    # 確保所有數值都是 float 類型
                    amount = self._convert_to_float(trade_row['amount'])
                    price = self._convert_to_float(trade_row['price'])
                    original_holding_amount = holding_amount
                    realized_profit = 0
                    realized_profit_percentage = 0
                    
                    if trade_row['side'] == 'sell':
                        sell_amount = amount
                        avg_cost = (holding_cost / holding_amount) if holding_amount > 0 else 0
                        realized_profit = (price - avg_cost) * sell_amount if avg_cost > 0 else 0
                        realized_profit_percentage = ((price / avg_cost - 1) * 100) if avg_cost > 0 else 0
                        holding_amount -= sell_amount
                        holding_cost -= avg_cost * sell_amount
                        if holding_amount < 0:
                            holding_amount = 0
                            holding_cost = 0
                    elif trade_row['side'] == 'buy':
                        holding_amount += amount
                        holding_cost += amount * price
                    
                    # 處理時間戳
                    ts = int(trade_row['timestamp'])
                    if ts > 1e12:
                        ts = int(ts / 1000)
                    
                    # 獲取代幣資訊
                    token_info = token_info_dict.get(trade_row['token_address'], {})
                    # 使用 supply_float 而不是 supply，因為 supply_float 已經考慮了 decimals
                    supply_float = self._convert_to_float(token_info.get('supply_float', 0) or 0)
                    
                    try:
                        marketcap = price * supply_float if supply_float else 0
                    except (ValueError, TypeError):
                        marketcap = 0
                    
                    value = price * amount
                    
                    # 計算持倉百分比
                    holding_percentage = None
                    wallet_balance_float = self._convert_to_float(wallet_balance)
                    if trade_row['side'] == 'buy' or trade_row['side'] == 'build':
                        if wallet_balance_float > 0:
                            holding_percentage = min(100, (value / wallet_balance_float) * 100)
                    elif trade_row['side'] == 'sell' or trade_row['side'] == 'clean':
                        if original_holding_amount > 0:
                            holding_percentage = min(100, (amount / original_holding_amount) * 100)
                    
                    # 獲取代幣詳細資訊
                    token_icon = token_info.get('image', '')
                    token_name = token_info.get('symbol', token_info.get('name', ''))
                    from_token_info = token_info_dict.get(trade_row['token_in'], {})
                    dest_token_info = token_info_dict.get(trade_row['token_out'], {})
                    from_token_symbol = from_token_info.get('symbol', '')
                    dest_token_symbol = dest_token_info.get('symbol', '')
                    
                    # 使用新的交易類型判斷邏輯
                    is_buy = trade_row['side'] == 'buy' or trade_row['side'] == 'build'
                    transaction_type = self.determine_transaction_type(
                        trade_row['signer'], trade_row['token_address'], 
                        is_buy, trade_row['amount']
                    )
                    
                    # 構建交易記錄
                    tx_data = {
                        'wallet_address': trade_row['signer'],
                        'wallet_balance': wallet_balance,
                        'token_address': trade_row['token_address'],
                        'token_icon': token_icon,
                        'token_name': token_name,
                        'price': price,
                        'amount': trade_row['amount'],
                        'marketcap': marketcap,
                        'value': value,
                        'holding_percentage': holding_percentage,
                        'chain': self.get_chain_name(trade_row['chain_id']),
                        'realized_profit': realized_profit,
                        'realized_profit_percentage': realized_profit_percentage,
                        'transaction_type': transaction_type,
                        'transaction_time': ts,
                        'time': now_utc8(),
                        'signature': trade_row['tx_hash'],
                        'from_token_address': trade_row['token_in'],
                        'from_token_symbol': from_token_symbol,
                        'from_token_amount': trade_row['amount_in'],
                        'dest_token_address': trade_row['token_out'],
                        'dest_token_symbol': dest_token_symbol,
                        'dest_token_amount': trade_row['amount_out'],
                    }
                    transaction_rows.append(tx_data)
                    
                    # 更新 WalletTokenState 緩存
                    self._update_wallet_token_state_after_transaction(
                        trade_row['signer'], trade_row['token_address'], transaction_type,
                        trade_row['amount'], value, price, ts
                    )
            
            logger.info(f"轉換了 {len(transaction_rows)} 筆交易記錄")
            return transaction_rows
            
        except Exception as e:
            logger.error(f"轉換交易記錄失敗: {str(e)}")
            return []

    async def _check_cache_update_needed(self, wallet_address: str):
        """檢查是否需要將緩存更新到數據庫"""
        current_time = time.time()
        
        # 確保異步鎖已初始化
        self._ensure_async_locks()
        
        # 如果鎖仍然為 None（沒有事件循環），跳過緩存更新
        if self.cache_lock is None:
            logger.warning("沒有事件循環，跳過緩存更新")
            return
            
        async with self.cache_lock:
            # 檢查是否達到更新條件
            if (self.pending_updates >= self.cache_update_threshold or 
                current_time - self.last_cache_update >= self.cache_update_interval):
                
                # 重置計數器和時間戳
                self.pending_updates = 0
                self.last_cache_update = current_time

                # 在背景執行更新
                asyncio.create_task(self._update_cache_to_db(wallet_address))

    async def _update_cache_to_db(self, wallet_address: str):
        """將緩存數據更新到數據庫"""
        try:
            if not self.token_buy_cache:
                return

            logger.info(f"開始將緩存數據更新到數據庫，錢包: {wallet_address}")
            await self.update_token_buy_data_from_cache(wallet_address, self.token_buy_cache)
            
            # 清空已更新代幣的緩存數據
            for token_address in list(self.token_buy_cache.keys()):
                if token_address in self.processed_tokens:
                    del self.token_buy_cache[token_address]
            
            # 清空已處理代幣集合
            self.processed_tokens.clear()
            
        except Exception as e:
            logger.error(f"更新緩存到數據庫時發生錯誤: {str(e)}")

    def _increment_pending_updates(self):
        """增加待處理交易計數"""
        self.pending_updates += 1

    async def _load_token_data_from_db(self, wallet_address: str, token_address: str) -> Optional[Dict]:
        """從數據庫加載代幣數據"""
        try:
            async with async_session() as session:
                query = select(TokenBuyData).where(
                    and_(
                        TokenBuyData.wallet_address == wallet_address,
                        TokenBuyData.token_address == token_address
                    )
                )
                result = await session.execute(query)
                record = result.scalar_one_or_none()

                if record:
                    return {
                        "total_amount": Decimal(str(record.total_amount or 0)),
                        "total_cost": Decimal(str(record.total_cost or 0)),
                        "avg_buy_price": Decimal(str(record.avg_buy_price or 0)),
                        "historical_buy_amount": Decimal(str(record.historical_total_buy_amount or 0)),
                        "historical_buy_cost": Decimal(str(record.historical_total_buy_cost or 0)),
                        "historical_sell_amount": Decimal(str(record.historical_total_sell_amount or 0)),
                        "historical_sell_value": Decimal(str(record.historical_total_sell_value or 0)),
                        "realized_profit": Decimal(str(record.realized_profit or 0)),
                        "last_transaction_time": record.last_transaction_time or 0
                    }
                return None
        except Exception as e:
            logger.error(f"從數據庫加載代幣數據時發生錯誤: {str(e)}")
            return None

    def _load_token_data_sync(self, wallet_address: str, token_address: str) -> Optional[Dict]:
        """從數據庫同步加載代幣數據"""
        try:
            with self.session_factory() as session:
                query = session.execute(
                    select(TokenBuyData).where(
                        and_(
                            TokenBuyData.wallet_address == wallet_address,
                            TokenBuyData.token_address == token_address
                        )
                    )
                )
                record = query.scalar_one_or_none()

                if record:
                    return {
                        "total_amount": Decimal(str(record.total_amount or 0)),
                        "total_cost": Decimal(str(record.total_cost or 0)),
                        "avg_buy_price": Decimal(str(record.avg_buy_price or 0)),
                        "historical_buy_amount": Decimal(str(record.historical_total_buy_amount or 0)),
                        "historical_buy_cost": Decimal(str(record.historical_total_buy_cost or 0)),
                        "historical_sell_amount": Decimal(str(record.historical_total_sell_amount or 0)),
                        "historical_sell_value": Decimal(str(record.historical_total_sell_value or 0)),
                        "realized_profit": Decimal(str(record.realized_profit or 0)),
                        "last_transaction_time": record.last_transaction_time or 0
                    }
                return None
        except Exception as e:
            logger.error(f"從數據庫加載代幣數據時發生錯誤: {str(e)}")
            return None

    async def _load_wallet_token_states_to_cache(self):
        """從數據庫加載 WalletTokenState 數據到緩存"""
        try:
            logger.info("開始從數據庫加載 WalletTokenState 數據到緩存...")
            
            # 計算30天前的時間戳
            thirty_days_ago = int(time.time()) - (30 * 24 * 60 * 60)
            
            db_ready = self._init_db_connection()
            if not db_ready:
                logger.error("數據庫未就緒，無法加載 WalletTokenState 數據")
                return
            
            with self.session_factory() as session:
                # 查詢SOLANA鏈且最近30天有交易的記錄
                query = session.execute(
                    select(WalletTokenState).where(
                        (WalletTokenState.chain == "SOLANA") &
                        (WalletTokenState.last_transaction_time >= thirty_days_ago)
                    )
                )
                records = query.scalars().all()
                
                loaded_count = 0
                for record in records:
                    cache_key = f"{record.wallet_address}:{record.token_address}"
                    self.wallet_token_state_cache[cache_key] = {
                        "current_amount": float(record.current_amount or 0),
                        "current_total_cost": float(record.current_total_cost or 0),
                        "current_avg_buy_price": float(record.current_avg_buy_price or 0),
                        "position_opened_at": record.position_opened_at,
                        "historical_buy_amount": float(record.historical_buy_amount or 0),
                        "historical_sell_amount": float(record.historical_sell_amount or 0),
                        "historical_buy_cost": float(record.historical_buy_cost or 0),
                        "historical_sell_value": float(record.historical_sell_value or 0),
                        "historical_realized_pnl": float(record.historical_realized_pnl or 0),
                        "historical_buy_count": int(record.historical_buy_count or 0),
                        "historical_sell_count": int(record.historical_sell_count or 0),
                        "last_transaction_time": record.last_transaction_time or 0
                    }
                    loaded_count += 1
                
                logger.info(f"成功加載 {loaded_count} 條 WalletTokenState 記錄到緩存")
                
        except Exception as e:
            logger.error(f"從數據庫加載 WalletTokenState 數據到緩存時發生錯誤: {str(e)}")

    def get_wallet_token_state(self, wallet_address: str, token_address: str) -> Dict[str, Any]:
        """獲取錢包代幣狀態"""
        cache_key = f"{wallet_address}:{token_address}"
        
        if cache_key in self.wallet_token_state_cache:
            return self.wallet_token_state_cache[cache_key]
        
        # 如果緩存中沒有，從數據庫加載
        return self._load_wallet_token_state_from_db(wallet_address, token_address)

    def _load_wallet_token_state_from_db(self, wallet_address: str, token_address: str) -> Dict[str, Any]:
        """從數據庫加載錢包代幣狀態"""
        try:
            db_ready = self._init_db_connection()
            if not db_ready:
                return self._get_default_wallet_token_state()
            
            with self.session_factory() as session:
                query = session.execute(
                    select(WalletTokenState).where(
                        and_(
                            WalletTokenState.wallet_address == wallet_address,
                            WalletTokenState.token_address == token_address,
                            WalletTokenState.chain == "SOLANA"
                        )
                    )
                )
                record = query.scalar_one_or_none()
                
                if record:
                    state_data = {
                        "current_amount": float(record.current_amount or 0),
                        "current_total_cost": float(record.current_total_cost or 0),
                        "current_avg_buy_price": float(record.current_avg_buy_price or 0),
                        "position_opened_at": record.position_opened_at,
                        "historical_buy_amount": float(record.historical_buy_amount or 0),
                        "historical_sell_amount": float(record.historical_sell_amount or 0),
                        "historical_buy_cost": float(record.historical_buy_cost or 0),
                        "historical_sell_value": float(record.historical_sell_value or 0),
                        "historical_realized_pnl": float(record.historical_realized_pnl or 0),
                        "historical_buy_count": int(record.historical_buy_count or 0),
                        "historical_sell_count": int(record.historical_sell_count or 0),
                        "last_transaction_time": record.last_transaction_time or 0
                    }
                    
                    # 更新緩存
                    cache_key = f"{wallet_address}:{token_address}"
                    self.wallet_token_state_cache[cache_key] = state_data
                    
                    return state_data
                
                return self._get_default_wallet_token_state()
                
        except Exception as e:
            logger.error(f"從數據庫加載錢包代幣狀態時發生錯誤: {str(e)}")
            return self._get_default_wallet_token_state()

    def _get_default_wallet_token_state(self) -> Dict[str, Any]:
        """獲取預設的錢包代幣狀態"""
        return {
            "current_amount": 0.0,
            "current_total_cost": 0.0,
            "current_avg_buy_price": 0.0,
            "position_opened_at": None,
            "historical_buy_amount": 0.0,
            "historical_sell_amount": 0.0,
            "historical_buy_cost": 0.0,
            "historical_sell_value": 0.0,
            "historical_realized_pnl": 0.0,
            "historical_buy_count": 0,
            "historical_sell_count": 0,
            "last_transaction_time": 0
        }

    def update_wallet_token_state(self, wallet_address: str, token_address: str, state_data: Dict[str, Any]) -> None:
        """更新錢包代幣狀態緩存"""
        cache_key = f"{wallet_address}:{token_address}"
        self.wallet_token_state_cache[cache_key] = state_data
        
        # 增加待更新計數
        self.pending_state_updates += 1
        
        # 檢查是否需要更新到數據庫 - 修復事件循環問題
        try:
            # 獲取當前事件循環，如果不存在則跳過
            loop = asyncio.get_running_loop()
            if loop and not loop.is_closed():
                asyncio.create_task(self._check_state_cache_update_needed(wallet_address, token_address))
        except RuntimeError:
            # 沒有運行中的事件循環，跳過異步任務創建
            pass

    async def _check_state_cache_update_needed(self, wallet_address: str, token_address: str):
        """檢查是否需要將狀態緩存更新到數據庫"""
        current_time = time.time()
        
        # 確保異步鎖已初始化
        self._ensure_async_locks()
        
        # 如果鎖仍然為 None（沒有事件循環），跳過狀態緩存更新
        if self.state_cache_lock is None:
            logger.warning("沒有事件循環，跳過狀態緩存更新")
            return
            
        async with self.state_cache_lock:
            # 檢查是否達到更新條件
            if (self.pending_state_updates >= self.state_cache_update_threshold or 
                current_time - self.last_state_cache_update >= self.state_cache_update_interval):
                
                # 重置計數器和時間戳
                self.pending_state_updates = 0
                self.last_state_cache_update = current_time

                # 在背景執行更新
                asyncio.create_task(self._update_state_cache_to_db(wallet_address, token_address))

    async def _update_state_cache_to_db(self, wallet_address: str, token_address: str):
        """將狀態緩存數據更新到數據庫"""
        try:
            cache_key = f"{wallet_address}:{token_address}"
            if cache_key not in self.wallet_token_state_cache:
                return

            logger.info(f"開始將狀態緩存數據更新到數據庫，錢包: {wallet_address}, 代幣: {token_address}")
            await self._save_wallet_token_state_to_db(wallet_address, token_address, self.wallet_token_state_cache[cache_key])
            
        except Exception as e:
            logger.error(f"更新狀態緩存到數據庫時發生錯誤: {str(e)}")

    async def _save_wallet_token_state_to_db(self, wallet_address: str, token_address: str, state_data: Dict[str, Any]) -> bool:
        """保存錢包代幣狀態到數據庫 - 使用UPSERT避免唯一約束衝突"""
        try:
            db_ready = self._init_db_connection()
            if not db_ready:
                logger.error("數據庫未就緒，無法保存錢包代幣狀態")
                return False
            
            with self.session_factory() as session:
                # 使用 PostgreSQL 的 UPSERT 功能
                stmt = insert(WalletTokenState).values(
                    wallet_address=wallet_address,
                    token_address=token_address,
                    chain="SOLANA",
                    chain_id=501,
                    current_amount=state_data["current_amount"],
                    current_total_cost=state_data["current_total_cost"],
                    current_avg_buy_price=state_data["current_avg_buy_price"],
                    position_opened_at=state_data["position_opened_at"],
                    historical_buy_amount=state_data["historical_buy_amount"],
                    historical_sell_amount=state_data["historical_sell_amount"],
                    historical_buy_cost=state_data["historical_buy_cost"],
                    historical_sell_value=state_data["historical_sell_value"],
                    historical_realized_pnl=state_data["historical_realized_pnl"],
                    historical_buy_count=state_data["historical_buy_count"],
                    historical_sell_count=state_data["historical_sell_count"],
                    last_transaction_time=state_data["last_transaction_time"],
                    updated_at=now_utc8()
                )
                
                # 設置衝突處理：如果唯一約束衝突，則更新現有記錄
                stmt = stmt.on_conflict_do_update(
                    index_elements=['wallet_address', 'token_address', 'chain'],
                    set_={
                        'current_amount': state_data["current_amount"],
                        'current_total_cost': state_data["current_total_cost"],
                        'current_avg_buy_price': state_data["current_avg_buy_price"],
                        'position_opened_at': state_data["position_opened_at"],
                        'historical_buy_amount': state_data["historical_buy_amount"],
                        'historical_sell_amount': state_data["historical_sell_amount"],
                        'historical_buy_cost': state_data["historical_buy_cost"],
                        'historical_sell_value': state_data["historical_sell_value"],
                        'historical_realized_pnl': state_data["historical_realized_pnl"],
                        'historical_buy_count': state_data["historical_buy_count"],
                        'historical_sell_count': state_data["historical_sell_count"],
                        'last_transaction_time': state_data["last_transaction_time"],
                        'updated_at': now_utc8()
                    }
                )
                
                session.execute(stmt)
                session.commit()
                logger.info(f"成功保存錢包代幣狀態到數據庫: {wallet_address}:{token_address}")
                return True
                
        except Exception as e:
            logger.error(f"保存錢包代幣狀態到數據庫時發生錯誤: {str(e)}")
            return False

    def determine_transaction_type(self, wallet_address: str, token_address: str, is_buy: bool, amount: float) -> str:
        """判斷交易類型：build, buy, sell, clean"""
        try:
            # 獲取當前錢包代幣狀態
            state = self.get_wallet_token_state(wallet_address, token_address)
            current_amount = state["current_amount"]
            
            if is_buy:
                # 買入交易
                if current_amount <= 0:
                    # 如果當前沒有持倉，視為建倉
                    return "build"
                else:
                    # 如果已有持倉，視為加倉
                    return "buy"
            else:
                # 賣出交易
                if current_amount <= 0:
                    # 如果當前沒有持倉，視為清倉（可能是之前的清倉操作）
                    return "clean"
                elif amount >= current_amount:
                    # 如果賣出數量等於或大於當前持倉，視為清倉
                    return "clean"
                else:
                    # 如果賣出數量小於當前持倉，視為減倉
                    return "sell"
                    
        except Exception as e:
            logger.error(f"判斷交易類型時發生錯誤: {str(e)}")
            # 發生錯誤時，回退到原來的邏輯
            return "buy" if is_buy else "sell"

    def _update_wallet_token_state_after_transaction(self, wallet_address, token_address, transaction_type, amount, value, price, timestamp):
        """更新錢包代幣狀態緩存"""
        cache_key = f"{wallet_address}:{token_address}"
        
        # 獲取當前狀態，如果不存在則創建預設狀態
        if cache_key not in self.wallet_token_state_cache:
            self.wallet_token_state_cache[cache_key] = self._get_default_wallet_token_state()
        
        state = self.wallet_token_state_cache[cache_key]
        current_amount = state["current_amount"]
        current_cost = state["current_total_cost"]
        current_avg_buy_price = state["current_avg_buy_price"]
        position_opened_at = state["position_opened_at"]
        historical_buy_amount = state["historical_buy_amount"]
        historical_sell_amount = state["historical_sell_amount"]
        historical_buy_cost = state["historical_buy_cost"]
        historical_sell_value = state["historical_sell_value"]
        historical_realized_pnl = state["historical_realized_pnl"]
        historical_buy_count = state["historical_buy_count"]
        historical_sell_count = state["historical_sell_count"]

        if transaction_type in ["build", "buy"]:
            # 買入交易（建倉或加倉）
            if transaction_type == "build" and position_opened_at is None:
                # 建倉時設置首次建倉時間
                position_opened_at = timestamp
            
            # 更新當前持倉
            current_amount += amount
            current_cost += value
            current_avg_buy_price = current_cost / current_amount if current_amount > 0 else 0
            
            # 更新歷史數據
            historical_buy_amount += amount
            historical_buy_cost += value
            historical_buy_count += 1
            
        elif transaction_type in ["sell", "clean"]:
            # 賣出交易（減倉或清倉）
            if current_amount > 0:
                # 計算賣出比例
                sell_ratio = min(1.0, amount / current_amount)
                
                # 計算對應的成本
                cost_basis = current_cost * sell_ratio
                
                # 計算已實現盈虧
                realized_pnl = value - cost_basis
                historical_realized_pnl += realized_pnl
                
                # 更新當前持倉
                current_amount -= amount
                if current_amount <= 0:
                    # 完全清倉
                    current_amount = 0
                    current_cost = 0
                    current_avg_buy_price = 0
                    position_opened_at = None
                else:
                    # 部分減倉
                    current_cost -= cost_basis
                    current_avg_buy_price = current_cost / current_amount if current_amount > 0 else 0
            
            # 更新歷史數據
            historical_sell_amount += amount
            historical_sell_value += value
            historical_sell_count += 1

        # 更新最後交易時間
        state["last_transaction_time"] = timestamp
        
        # 更新狀態數據
        state["current_amount"] = current_amount
        state["current_total_cost"] = current_cost
        state["current_avg_buy_price"] = current_avg_buy_price
        state["position_opened_at"] = position_opened_at
        state["historical_buy_amount"] = historical_buy_amount
        state["historical_sell_amount"] = historical_sell_amount
        state["historical_buy_cost"] = historical_buy_cost
        state["historical_sell_value"] = historical_sell_value
        state["historical_realized_pnl"] = historical_realized_pnl
        state["historical_buy_count"] = historical_buy_count
        state["historical_sell_count"] = historical_sell_count
        
        # 更新緩存
        self.wallet_token_state_cache[cache_key] = state
        
        # 觸發數據庫更新
        self.update_wallet_token_state(wallet_address, token_address, state)

    def calculate_realized_profit_with_cache(self, wallet_address: str, token_address: str, amount: float, sell_price: float) -> Tuple[float, float]:
        """
        使用 WalletTokenState 緩存機制計算已實現利潤和利潤百分比
        
        Args:
            wallet_address: 錢包地址
            token_address: 代幣地址
            amount: 賣出數量
            sell_price: 賣出價格
            
        Returns:
            Tuple[float, float]: (realized_profit, realized_profit_percentage)
        """
        try:
            # [PNL排查] 計算realized_profit前日誌
            logger.warning(f"[PNL排查] 計算realized_profit: wallet={wallet_address}, token={token_address}, amount={amount}, sell_price={sell_price}")

            wallet_token_state = self.get_wallet_token_state(wallet_address, token_address)
            avg_buy_price = wallet_token_state["current_avg_buy_price"]

            # [PNL排查] 當前狀態日誌
            logger.warning(f"[PNL排查] 當前狀態: current_amount={wallet_token_state['current_amount']}, current_total_cost={wallet_token_state['current_total_cost']}, avg_buy_price={avg_buy_price}")

            # 註解掉本方法內所有info/debug日誌
            # logger.debug(f"賣出計算 - 代幣: {token_address}, 平均買入價: {avg_buy_price}, 賣出價: {sell_price}, 數量: {amount}")

            if avg_buy_price > 0 and amount > 0:
                cost_basis = amount * avg_buy_price
                sell_value = amount * sell_price
                realized_profit = sell_value - cost_basis
                if cost_basis > 0:
                    realized_profit_percentage = (realized_profit / cost_basis) * 100
                    realized_profit_percentage = max(-100.0, realized_profit_percentage)
                else:
                    realized_profit_percentage = 0
                # [PNL排查] 計算結果日誌
                logger.warning(f"[PNL排查] 計算結果: cost_basis={cost_basis}, sell_value={sell_value}, realized_profit={realized_profit}, realized_profit_percentage={realized_profit_percentage}")
            else:
                realized_profit = 0
                realized_profit_percentage = 0
                # 降低日志级别，避免重复警告
                logger.debug(f"[PNL排查] 無法計算利潤，avg_buy_price={avg_buy_price}, amount={amount}")
            return realized_profit, realized_profit_percentage
        except Exception as e:
            logger.error(f"[PNL排查] 計算已實現利潤時出錯: {e}")
            return 0.0, 0.0

    def _create_event_content_hash(self, event_data):
        exclude_fields = {'signature', 'transactionTime'}
        content = {k: v for k, v in event_data.items() if k not in exclude_fields}
        sorted_content = json.dumps(content, sort_keys=True)
        return hashlib.md5(sorted_content.encode()).hexdigest()

    async def add_event_to_pending(self, event, signature=None):
        # 確保異步鎖已初始化
        self._ensure_async_locks()
        
        # 如果鎖仍然為 None（沒有事件循環），跳過事件推送
        if self._pending_lock is None:
            logger.warning("沒有事件循環，跳過事件推送")
            return True
            
        async with self._pending_lock:
            if not signature:
                signature = event.get('signature')
            if not signature:
                logger.error("缺少 signature，無法推送事件")
                return False
            if not hasattr(self, '_processed_signatures'):
                self._processed_signatures = set()
            if not hasattr(self, '_pending_signatures'):
                self._pending_signatures = set()
            if signature in self._processed_signatures or signature in self._pending_signatures:
                logger.debug(f"Signature 已處理或已在 pending 中，跳過: {signature}")
                self._event_stats['total_duplicates'] += 1
                return True
            # 按 transactionTime 排序插入事件
            self._insert_event_sorted(event)
            self._pending_signatures.add(signature)
            self._event_stats['total_added'] += 1
            if len(self.pending_events) >= 10:
                to_send = self.pending_events[:]
                # 不要先清空，等推送成功後再清空
                success = await self._send_events_batch(to_send)
                if success:
                    self.pending_events.clear()
                    self._pending_signatures.clear()

transaction_processor = TransactionProcessor()