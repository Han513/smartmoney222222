import os
import re
import time
import base58
import logging
import requests
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from app.core.config import settings
from app.models.models import WalletSummary, Transaction, Holding, TokenBuyData
from app.services.token_repository import token_repository
from app.services.solscan import solscan_client
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
import asyncio

# 設置 Decimal 精度
getcontext().prec = 28
tz_utc8 = timezone(timedelta(hours=8))


logger = logging.getLogger(__name__)

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
        # 嘗試取得環境變數
        db_uri_solana = os.getenv('DATABASE_URI_Solana', '未設定')
        db_url_sync = os.getenv('DATABASE_URL_SYNC', '未設定')
        logger.info(f"環境變數 DATABASE_URI_Solana: {db_uri_solana}")
        logger.info(f"環境變數 DATABASE_URL_SYNC: {db_url_sync}")
        self.token_info_cache = {}
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
            "icon": "..."
        },
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
            "symbol": "USDT",
            "name": "Tether",
            "decimals": 6,
            "icon": "..."
        }
    }
    
    def activate(self):
        """
        激活交易处理器，标记为已激活
        """
        if self._activated:
            logger.info("TransactionProcessor 已经被激活")
            return True
        
        logger.info("激活 TransactionProcessor...")
        self._activated = True
        self._init_db_connection()
        
        if not self.db_enabled:
            logger.warning("数据库功能未启用")
        return True
    
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
                        logger.info(f"轉換異步數據庫 URL 為同步 URL: {db_url}")
                else:
                    logger.info(f"使用環境變數 DATABASE_URL_SYNC: {db_url}")
                
                # 檢查 URL 格式
                if not db_url or not isinstance(db_url, str):
                    logger.error(f"資料庫 URL 無效: {db_url}")
                    return False
                
                if not (db_url.startswith('postgresql://') or db_url.startswith('mysql://') or db_url.startswith('sqlite:///')):
                    logger.error(f"資料庫 URL 格式無效，需要以 postgresql:// 或其他支援的資料庫前綴開始: {db_url}")
                    return False
                
                logger.info(f"使用資料庫 URL: {db_url}")
                
                # 增强设置，加大连接池和超时时间
                self.engine = create_engine(
                    db_url,
                    echo=False,
                    future=True,
                    pool_size=10,           # 增加连接池大小
                    max_overflow=20,        # 增加最大溢出连接数
                    pool_timeout=60,        # 增加获取连接超时时间
                    pool_recycle=1800,      # 每30分钟回收连接
                    pool_pre_ping=True      # 使用前测试连接活跃性
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
    
    def get_wallet_balance(self, wallet_address: str) -> Dict[str, Any]:
        """獲取錢包餘額數據"""
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法獲取錢包 {wallet_address} 餘額，因數據庫未就緒")
            return {"balance": 0, "balance_usd": 0}

        try:
            with self.session_factory() as session:
                wallet_query = session.execute(
                    select(WalletSummary).where(WalletSummary.address == wallet_address)
                )
                wallet = wallet_query.scalars().first()

                if wallet:
                    balance = float(wallet.balance) if wallet.balance is not None else 0.0
                    balance_usd = float(wallet.balance_USD) if wallet.balance_USD is not None else 0.0
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
                        "updated_at": buy_record.updated_at.timestamp() if buy_record.updated_at else 0
                    }
            return {}
        except Exception as e:
            logger.exception(f"獲取 {wallet_address}/{token_address} 購買數據時發生錯誤: {e}")
            return {}

    # def update_token_buy_data(self, wallet_address: str, token_address: str, buy_data: Dict) -> bool:
    #     """更新代幣購買數據"""
    #     db_ready = self._init_db_connection()
    #     if not db_ready:
    #         logger.error(f"無法更新 {wallet_address}/{token_address} 購買數據，因數據庫未就緒")
    #         return False

    #     try:
    #         amount = Decimal(str(buy_data.get("amount", 0)))
    #         cost = Decimal(str(buy_data.get("cost", 0)))
    #         is_buy = buy_data.get("is_buy", True)

    #         with self.session_factory() as session:
    #             query = session.execute(
    #                 select(TokenBuyData).where(
    #                     (TokenBuyData.wallet_address == wallet_address) &
    #                     (TokenBuyData.token_address == token_address)
    #                 ).with_for_update()
    #             )
    #             buy_record = query.scalars().first()

    #             if buy_record:
    #                 current_amount = Decimal(str(buy_record.total_amount or 0.0))
    #                 current_cost = Decimal(str(buy_record.total_cost or 0.0))
                    
    #                 new_total_amount = current_amount + amount
    #                 new_total_cost = current_cost + (cost if is_buy else Decimal(0))
                    
    #                 buy_record.total_amount = float(new_total_amount)
    #                 buy_record.total_cost = float(new_total_cost)
    #                 buy_record.updated_at = datetime.now()
                    
    #                 if new_total_amount > 0:
    #                     if new_total_cost > 0:
    #                         buy_record.avg_buy_price = float(new_total_cost / new_total_amount)
    #                     else:
    #                         buy_record.avg_buy_price = 0.0
    #                 else:
    #                     buy_record.avg_buy_price = 0.0
    #                     if new_total_amount == 0:
    #                         buy_record.total_cost = 0.0
    #             else:
    #                 if amount > 0:
    #                     avg_price = cost / amount if amount != 0 else Decimal(0)
    #                     new_record = TokenBuyData(
    #                         wallet_address=wallet_address,
    #                         token_address=token_address,
    #                         total_amount=float(amount),
    #                         total_cost=float(cost),
    #                         avg_buy_price=float(avg_price),
    #                         updated_at=datetime.now()
    #                     )
    #                     session.add(new_record)
    #                 else:
    #                     logger.warning(f"Attempted to create TokenBuyData for {wallet_address}/{token_address} with non-positive initial amount {amount}. Skipping.")

    #             session.commit()
    #             return True

    #     except Exception as e:
    #         logger.exception(f"更新 {wallet_address}/{token_address} 購買數據時發生錯誤: {e}")
    #         return False
    def update_token_buy_data(self, wallet_address: str, token_address: str, buy_data: Dict) -> bool:
        """更新代幣購買數據"""
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法更新 {wallet_address}/{token_address} 購買數據，因數據庫未就緒")
            return False

        try:
            # 調試日誌，記錄詳細信息
            logger.info(f"更新代幣購買數據: 地址={wallet_address}, 代幣={token_address}, 數據={buy_data}")
            
            amount = Decimal(str(buy_data.get("amount", 0)))
            cost = Decimal(str(buy_data.get("cost", 0)))
            is_buy = buy_data.get("is_buy", True)
            
            # 更多調試信息
            logger.info(f"解析後數據: amount={amount}, cost={cost}, is_buy={is_buy}")

            with self.session_factory() as session:
                query = session.execute(
                    select(TokenBuyData).where(
                        (TokenBuyData.wallet_address == wallet_address) &
                        (TokenBuyData.token_address == token_address)
                    ).with_for_update()
                )
                buy_record = query.scalars().first()

                if buy_record:
                    # 記錄現有數據
                    logger.info(f"找到現有記錄: 總量={buy_record.total_amount}, 總成本={buy_record.total_cost}")
                    
                    current_amount = Decimal(str(buy_record.total_amount or 0.0))
                    current_cost = Decimal(str(buy_record.total_cost or 0.0))
                    
                    new_total_amount = current_amount + amount
                    # 只有買入時才更新成本
                    new_total_cost = current_cost + (cost if is_buy else Decimal(0))
                    
                    logger.info(f"計算新值: 新總量={new_total_amount}, 新總成本={new_total_cost}")
                    
                    buy_record.total_amount = float(new_total_amount)
                    buy_record.total_cost = float(new_total_cost)
                    buy_record.updated_at = datetime.now()
                    
                    if new_total_amount > 0:
                        if new_total_cost > 0:
                            buy_record.avg_buy_price = float(new_total_cost / new_total_amount)
                            logger.info(f"更新平均買入價: {buy_record.avg_buy_price}")
                        else:
                            buy_record.avg_buy_price = 0.0
                    else:
                        buy_record.avg_buy_price = 0.0
                        if new_total_amount == 0:
                            buy_record.total_cost = 0.0
                else:
                    logger.info(f"未找到現有記錄，準備創建新記錄")
                    # 只有買入或者正數金額時才創建記錄
                    if is_buy or amount > 0:
                        logger.info(f"創建新記錄: 買入={is_buy}, 數量={amount}")
                        avg_price = cost / amount if amount != 0 else Decimal(0)
                        new_record = TokenBuyData(
                            wallet_address=wallet_address,
                            token_address=token_address,
                            total_amount=float(amount),
                            total_cost=float(cost),
                            avg_buy_price=float(avg_price),
                            updated_at=datetime.now()
                        )
                        session.add(new_record)
                        logger.info(f"新記錄已添加到會話")
                    else:
                        logger.warning(f"跳過創建記錄: 非買入交易且金額不為正數: {wallet_address}/{token_address} 數量={amount}")

                session.commit()
                logger.info(f"交易已提交到數據庫")
                return True

        except Exception as e:
            logger.exception(f"更新 {wallet_address}/{token_address} 購買數據時發生錯誤: {e}")
            return False
    
    # async def process_wallet_activities(self, wallet_address: str, activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    #     """處理錢包活動記錄 - 优化版本，支持大批量处理"""
    #     if not activities:
    #         logger.info(f"錢包 {wallet_address} 沒有活動記錄")
    #         return []

    #     # 初始化数据库连接
    #     db_ready = self._init_db_connection()
    #     if not db_ready and self.db_enabled:
    #         logger.error(f"數據庫未就緒，處理錢包 {wallet_address} 活動可能不完整")

    #     # 获取钱包余额
    #     try:
    #         balance_data = await self.get_sol_balance(wallet_address)
    #         wallet_balance = balance_data.get("balance", {}).get("float", 0)
    #         logger.info(f"钱包 {wallet_address} 余额: {wallet_balance}")
    #     except Exception as e:
    #         logger.warning(f"获取钱包 {wallet_address} 余额失败: {e}，使用默认值")
    #         wallet_balance = 0

    #     processed_activities_results = []

    #     # 按照block_time從舊到新排序
    #     activities.sort(key=lambda x: x.get("block_time", 0))
        
    #     # 计算活动总数和预计处理时间
    #     total_activities = len(activities)
    #     logger.info(f"准备处理 {wallet_address} 的 {total_activities} 个活动")
        
    #     # 如果活动数量超过阈值，分批处理
    #     MAX_BATCH_SIZE = 50  # 每批最大处理数量
        
    #     # 分批处理大量活动
    #     for i in range(0, total_activities, MAX_BATCH_SIZE):
    #         batch = activities[i:i+MAX_BATCH_SIZE]
    #         batch_size = len(batch)
    #         logger.info(f"处理 {wallet_address} 的第 {i//MAX_BATCH_SIZE + 1} 批活动 ({i+1}-{min(i+batch_size, total_activities)}/{total_activities})")
            
    #         # 处理当前批次活动
    #         for activity in batch:
    #             try:
    #                 activity_type = activity.get("activity_type", "unknown")
    #                 tx_hash = activity.get("tx_hash", "N/A")
                    
    #                 result = self.process_activity(activity, wallet_address, wallet_balance)
                    
    #                 if result and result.get("success", False):
    #                     processed_activities_results.append(result)
    #                 elif result:
    #                     logger.warning(f"處理活動 {tx_hash} 失敗: {result.get('message', 'unknown error')}")
    #                 else:
    #                     logger.error(f"處理活動 {tx_hash} 返回 None")
                    
    #             except Exception as e:
    #                 logger.exception(f"處理單個活動 (Tx: {activity.get('tx_hash', 'N/A')}) 時發生意外錯誤: {str(e)}")
    #                 processed_activities_results.append({
    #                     "success": False,
    #                     "transaction_hash": activity.get('tx_hash', 'N/A'),
    #                     "message": f"Unexpected processing error: {str(e)}"
    #                 })
            
    #         # 批次处理完成后，让出CPU，避免长时间阻塞
    #         if i + MAX_BATCH_SIZE < total_activities:
    #             await asyncio.sleep(0.1)
        
    #     logger.info(f"完成处理 {wallet_address} 的所有活动，成功处理 {len(processed_activities_results)} 个")
    #     return processed_activities_results

    # async def process_wallet_activities(self, wallet_address: str, activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    #     """處理錢包活動並保存交易信息"""
    #     logger.info(f"處理錢包 {wallet_address} 的 {len(activities)} 筆活動")
        
    #     if not activities:
    #         return []
        
    #     # 按時間戳排序活動（從舊到新）
    #     sorted_activities = sorted(activities, key=lambda x: x.get("block_time", 0))
        
    #     # 獲取錢包餘額
    #     wallet_balance = 0.0
    #     try:
    #         balance_info = await self.get_sol_balance(wallet_address)
    #         wallet_balance = balance_info.get("lamports", 0) / 1e9  # 轉換為 SOL
    #     except Exception as e:
    #         logger.error(f"獲取錢包餘額失敗: {str(e)}")
        
    #     # 預先收集所有涉及的代幣地址並預載入代幣信息（只執行一次）
    #     token_addresses = set()
    #     for activity in sorted_activities:
    #         if activity.get("activity_type") == "token_swap":
    #             from_token = activity.get("from_token", {})
    #             to_token = activity.get("to_token", {})
    #             if from_token and "address" in from_token:
    #                 token_addresses.add(from_token["address"])
    #             if to_token and "address" in to_token:
    #                 token_addresses.add(to_token["address"])
    #         elif activity.get("activity_type") == "token_transfer":
    #             token_address = activity.get("token_address", "")
    #             if token_address:
    #                 token_addresses.add(token_address)
        
    #     # 預先載入所有代幣信息
    #     if token_addresses:
    #         logger.info(f"預載入 {len(token_addresses)} 個代幣信息")
    #         await token_repository.prefetch_token_info(list(token_addresses))
        
    #     # 初始化數據收集結構
    #     processed_transactions = []
    #     transaction_data_batch = []  # 收集要批量寫入的交易數據
    #     token_updates = {}  # 用於收集代幣更新: {token_address: {'amount': 累計變化, 'cost': 累計成本, 'is_buy': 最後狀態}}
        
    #     # 單次遍歷處理每個活動
    #     for activity in sorted_activities:
    #         try:
    #             activity_type = activity.get("activity_type", "").lower()
    #             tx_hash = activity.get("tx_hash", "N/A")
    #             start_time = time.time()
                
    #             # 處理各種活動類型
    #             if activity_type == 'token_swap':
    #                 # 處理代幣交換
    #                 transaction_data = self._process_token_swap(wallet_address, activity, wallet_balance)
                    
    #                 # 更新代幣數據收集
    #                 if transaction_data and transaction_data.get("success"):
    #                     token_address = transaction_data.get("token_address")
    #                     is_buy = transaction_data.get("transaction_type") == "buy"
    #                     amount = Decimal(str(transaction_data.get("amount", 0)))
    #                     cost = Decimal(str(transaction_data.get("value", 0)))
                        
    #                     # 正確計算更新值（買入為正，賣出為負）
    #                     update_amount = amount if is_buy else -amount
                        
    #                     # 收集代幣更新
    #                     if token_address not in token_updates:
    #                         token_updates[token_address] = {'amount': 0, 'cost': 0, 'is_buy': is_buy}
                        
    #                     token_updates[token_address]['amount'] += update_amount
    #                     if is_buy:
    #                         token_updates[token_address]['cost'] += cost
    #                     token_updates[token_address]['is_buy'] = is_buy
                    
    #             elif activity_type == 'token_transfer':
    #                 # 處理代幣轉帳
    #                 transaction_data = self._process_token_transfer(wallet_address, activity)
                    
    #                 # 更新代幣數據收集
    #                 if transaction_data and transaction_data.get("success"):
    #                     token_address = transaction_data.get("token_address")
    #                     is_incoming = transaction_data.get("type") == "receive"
    #                     amount = Decimal(str(transaction_data.get("amount", 0)))
                        
    #                     # 轉帳進出計算（進為正，出為負）
    #                     update_amount = amount if is_incoming else -amount
                        
    #                     # 收集代幣更新
    #                     if token_address not in token_updates:
    #                         token_updates[token_address] = {'amount': 0, 'cost': 0, 'is_buy': False}
                        
    #                     token_updates[token_address]['amount'] += update_amount
    #                     # 轉帳不影響成本
                
    #             elif activity_type == 'token_activities':
    #                 # 處理批量代幣活動
    #                 transaction_data = self._process_bulk_token_activities(wallet_address, activity)
                    
    #                 # 批量活動有特殊處理邏輯，在函數內部處理token_updates
                
    #             else:
    #                 logger.warning(f"未知的活動類型: {activity_type} (Tx: {tx_hash})")
    #                 transaction_data = {
    #                     "success": False,
    #                     "transaction_hash": tx_hash,
    #                     "message": f"未知的活動類型: {activity_type}"
    #                 }
                
    #             # 記錄處理時間
    #             if transaction_data:
    #                 elapsed = time.time() - start_time
    #                 logger.debug(f"處理活動 {activity_type} (Tx: {tx_hash}) 完成, 耗時: {elapsed:.2f}秒")
                    
    #                 # 只有成功的交易才加入批次
    #                 if transaction_data.get("success", False):
    #                     processed_transactions.append(transaction_data)
                        
    #                     # 如果有transaction_data數據，加入批次
    #                     if "transaction_data" in transaction_data:
    #                         transaction_data_batch.append(transaction_data["transaction_data"])
                        
    #                     # 每50筆交易批量寫入一次
    #                     if len(transaction_data_batch) >= 50:
    #                         await self.save_transactions_batch(transaction_data_batch)
    #                         transaction_data_batch = []
                
    #         except Exception as e:
    #             logger.exception(f"處理活動時出錯: {str(e)}")
        
    #     # 處理剩餘的交易
    #     if transaction_data_batch:
    #         await self.save_transactions_batch(transaction_data_batch)
        
    #     # 批量更新TokenBuyData表（只執行一次）
    #     if token_updates:
    #         logger.info(f"批量更新錢包 {wallet_address} 的 {len(token_updates)} 個代幣購買數據")
    #         await self.update_token_buy_data_batch(wallet_address, token_updates)
        
    #     logger.info(f"完成處理 {wallet_address} 錢包的 {len(processed_transactions)} 筆交易")
    #     return processed_transactions

    async def process_wallet_activities(self, wallet_address: str, activities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """處理錢包活動 - 使用本地緩存計算利潤"""
        logger.info(f"處理錢包 {wallet_address} 的 {len(activities)} 筆活動")
        
        if not activities:
            return []
        
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
                    # 同樣使用緩存處理代幣轉帳
                    result = self._process_token_transfer_with_cache(
                        wallet_address, activity, token_cache, token_buy_cache
                    )
                    
                    if result and result.get("success"):
                        processed_transactions.append(result)
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

    # 帶緩存的代幣交換處理方法
    def _process_token_swap_cached(self, wallet_address, activity_data, wallet_balance, token_cache):
        """處理代幣交換活動 - 使用代幣資訊緩存"""
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
            
            # 安全計算價格 - 修復類型錯誤
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
            
            # 使用緩存獲取代幣資訊
            from_token_symbol = self._get_token_symbol_cached(from_token_address, token_cache)
            to_token_symbol = self._get_token_symbol_cached(to_token_address, token_cache)
            token_name = self._get_token_symbol_cached(token_address, token_cache)
            token_icon = self._get_token_icon_cached(token_address, token_cache)
            token_supply = self._get_token_supply_cached(token_address, token_cache)
            
            # 計算 marketcap
            marketcap = 0
            if float(price) > 0:  # 只要有價格就計算marketcap
                marketcap = float(price) * token_supply
            
            # 計算 holding_percentage
            holding_percentage = 0
            if is_buy and float(value_decimal) > 0:
                wallet_balance_float = float(wallet_balance)
                holding_pct = float(value_decimal) / (wallet_balance_float + float(value_decimal)) * 100
                holding_percentage = min(100, max(-100, holding_pct))
            elif not is_buy and float(amount) > 0:
                token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                current_amount = self._convert_to_float(token_buy_data.get("total_amount", 0))
                if current_amount > 0:
                    holding_pct = (float(amount) / current_amount) * 100
                    holding_percentage = min(100, max(-100, holding_pct))
                else:
                    holding_percentage = 100
            
            # 計算 realized_profit 和 realized_profit_percentage (只有賣出才有)
            realized_profit = 0
            realized_profit_percentage = 0
            
            if not is_buy:  # 賣出交易
                token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                avg_buy_price = self._convert_to_float(token_buy_data.get("avg_buy_price", 0))
                sell_price = float(price)
                
                logger.debug(f"賣出交易計算利潤: 地址={wallet_address}, 代幣={token_address}, avg_buy_price={avg_buy_price}, sell_price={sell_price}, amount={float(amount)}")
                
                if avg_buy_price > 0 and float(amount) > 0:
                    realized_profit, realized_profit_percentage = self.calculate_realized_profit(
                        token_address, float(amount), sell_price, avg_buy_price
                    )
                    realized_profit_percentage = min(1000, max(-100, realized_profit_percentage))
                else:
                    logger.debug(f"無法計算利潤: avg_buy_price={avg_buy_price}, amount={float(amount)}")
            
            # 創建交易資料
            transaction_data = {
                "wallet_address": wallet_address,
                "wallet_balance": wallet_balance,
                "signature": tx_hash,
                "transaction_time": timestamp,
                "transaction_type": 'buy' if is_buy else 'sell',
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
            
            return {
                "success": True,
                "transaction_hash": tx_hash,
                "transaction_data": transaction_data,
                "token_address": token_address,
                "amount": float(amount),
                "value": float(value_decimal),
                "transaction_type": 'buy' if is_buy else 'sell'
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
                            record.total_amount = total_amount
                            record.total_cost = total_cost
                            record.avg_buy_price = avg_buy_price
                            record.historical_total_buy_amount = historical_buy_amount
                            record.historical_total_buy_cost = historical_buy_cost
                            record.historical_total_sell_amount = historical_sell_amount
                            record.historical_total_sell_value = historical_sell_value
                            record.realized_profit = realized_profit
                            record.last_transaction_time = last_transaction_time
                            record.updated_at = datetime.now()
                            
                            # 特殊邏輯: 第一次交易和最後一次交易時間
                            if not record.position_opened_at and historical_buy_amount > 0:
                                record.position_opened_at = datetime.now()
                            
                            if total_amount == 0 and historical_sell_amount > 0:
                                record.last_active_position_closed_at = datetime.now()
                            
                            updated_records.append(token_address)
                        else:
                            # 創建新記錄
                            new_record = TokenBuyData(
                                wallet_address=wallet_address,
                                token_address=token_address,
                                total_amount=total_amount,
                                total_cost=total_cost,
                                avg_buy_price=avg_buy_price,
                                position_opened_at=datetime.now() if total_amount > 0 else None,
                                historical_total_buy_amount=historical_buy_amount,
                                historical_total_buy_cost=historical_buy_cost,
                                historical_total_sell_amount=historical_sell_amount,
                                historical_total_sell_value=historical_sell_value,
                                historical_avg_buy_price=avg_buy_price if historical_buy_amount > 0 else 0,
                                historical_avg_sell_price=historical_sell_value / historical_sell_amount if historical_sell_amount > 0 else 0,
                                last_active_position_closed_at=datetime.now() if total_amount == 0 and historical_sell_amount > 0 else None,
                                last_transaction_time=last_transaction_time,
                                realized_profit=realized_profit,
                                updated_at=datetime.now()
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
        """批量保存交易到數據庫"""
        if not self.db_enabled or not transactions:
            return False
        
        logger.info(f"嘗試批量保存 {len(transactions)} 筆交易")
        signatures = [tx.get("signature") for tx in transactions if "signature" in tx]
        logger.info(f"交易簽名樣本: {signatures[:3] if signatures else '無'}")
        
        # 优化1: 确保数据库连接始终正常
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法批量保存交易，因數據庫未就緒")
            return False
        
        start_time = time.time()
        try:
            with self.session_factory() as session:
                # 收集所有簽名以查詢已存在的記錄
                signatures = [tx.get("signature") for tx in transactions if "signature" in tx]
                
                # 优化2: 减少查询次数，先查询所有已存在的记录
                existing_signatures = set()
                if signatures:
                    # 使用IN查詢批量獲取現有記錄
                    try:
                        query = select(Transaction.signature).where(Transaction.signature.in_(signatures))
                        result = session.execute(query).scalars().all()
                        existing_signatures = set(result)
                    except Exception as e:
                        logger.warning(f"查詢現有交易簽名時出錯，將假設全部為新交易: {str(e)}")
                logger.info(f"查詢到 {len(existing_signatures)}/{len(signatures)} 個現有交易簽名")
                
                # 优化3: 分离新增和更新操作
                inserts = []
                updates = []
                
                # 处理每条交易
                for tx_data in transactions:
                    signature = tx_data.get("signature")
                    if not signature:
                        continue
                    
                    if signature not in existing_signatures:
                        # 收集待插入的记录
                        inserts.append(tx_data)
                    else:
                        # 收集待更新的记录
                        updates.append(tx_data)
                
                inserted_count = 0
                updated_count = 0
                
                # 优化4: 批量插入新记录
                if inserts:
                    try:
                        # 创建实体列表，一次性添加到会话
                        new_transactions = [Transaction(**tx_data) for tx_data in inserts]
                        session.add_all(new_transactions)
                        inserted_count = len(new_transactions)
                    except Exception as e:
                        logger.error(f"批量插入新交易时出错: {str(e)}")
                        session.rollback()
                        
                        # 回退到单条插入，跳过有问题的记录
                        inserted_count = 0
                        for tx_data in inserts:
                            try:
                                tx = Transaction(**tx_data)
                                session.add(tx)
                                inserted_count += 1
                            except Exception as e_inner:
                                # 记录错误但继续处理其他记录
                                logger.warning(f"插入交易 {tx_data.get('signature')} 时出错: {str(e_inner)}")
                
                # 优化5: 批量更新现有记录
                for tx_data in updates:
                    try:
                        tx_data_copy = tx_data.copy()
                        signature = tx_data_copy.get("signature")
                        tx_data_copy.pop("id", None)  # 移除主鍵以避免衝突
                        
                        stmt = (
                            update(Transaction)
                            .where(Transaction.signature == signature)
                            .values(**tx_data_copy)
                        )
                        session.execute(stmt)
                        updated_count += 1
                    except Exception as e:
                        logger.warning(f"更新交易 {tx_data.get('signature')} 时出错: {str(e)}")
                
                # 优化6: 使用事务保证原子性，但允许部分失败
                try:
                    session.commit()
                except Exception as e:
                    logger.error(f"提交交易批次时出错: {str(e)}")
                    
                    # 回退到单条提交模式
                    session.rollback()
                    inserted_count = 0
                    updated_count = 0
                    
                    # 创建新会话，避免使用已回滚的会话
                    session.close()
                    with self.session_factory() as new_session:
                        # 单条处理每个交易
                        for tx_data in transactions:
                            signature = tx_data.get("signature")
                            if not signature:
                                continue
                                
                            try:
                                # 检查是否存在
                                existing = new_session.execute(
                                    select(Transaction).where(Transaction.signature == signature)
                                ).scalar_one_or_none()
                                
                                if existing:
                                    # 更新现有记录
                                    tx_data_copy = tx_data.copy()
                                    tx_data_copy.pop("id", None)
                                    for key, value in tx_data_copy.items():
                                        setattr(existing, key, value)
                                    updated_count += 1
                                else:
                                    # 插入新记录
                                    new_tx = Transaction(**tx_data)
                                    new_session.add(new_tx)
                                    inserted_count += 1
                                    
                                # 每条记录单独提交
                                new_session.commit()
                            except Exception as e_inner:
                                new_session.rollback()
                                logger.warning(f"单条处理交易 {signature} 时出错: {str(e_inner)}")
                
                elapsed = time.time() - start_time
                logger.info(f"批量保存 {len(transactions)} 筆交易完成（插入: {inserted_count}, 更新: {updated_count}），耗時: {elapsed:.2f}秒")
                return True
        except Exception as e:
            logger.exception(f"批量保存交易失敗: {str(e)}")
            return False

    # def process_activity(self, activity: dict, wallet_address: str, wallet_balance: float, token_updates: Dict[str, Dict]) -> Optional[Dict[str, Any]]:
    #     """根據活動類型處理活動並創建交易記錄"""
    #     if not activity or not wallet_address:
    #         logger.error("處理活動失敗: 缺少活動數據或錢包地址")
    #         return {"success": False, "message": "Missing activity data or wallet address"}

    #     tx_hash = activity.get("tx_hash", "N/A")

    #     try:
    #         start_time = time.time()
    #         activity_type = activity.get("activity_type", "").lower()
    #         # logger.info(f"處理活動: {activity_type} 來自錢包 {wallet_address} (Tx: {tx_hash})")

    #         result = None
    #         if activity_type == 'token_swap':
    #             if token_address not in token_updates:
    #                 token_updates[token_address] = {'amount': 0, 'cost': 0, 'is_buy': is_buy}
                
    #             token_updates[token_address]['amount'] += update_amount
    #             if is_buy:
    #                 token_updates[token_address]['cost'] += cost
    #             token_updates[token_address]['is_buy'] = is_buy  # 記錄最後一次操作狀態
    #             result = self._process_token_swap(wallet_address, activity, wallet_balance)

    #         elif activity_type == 'token_transfer':
    #             result = self._process_token_transfer(wallet_address, activity)

    #         elif activity_type == 'token_activities':
    #             result = self._process_bulk_token_activities(wallet_address, activity)

    #         else:
    #             logger.warning(f"未知的活動類型: {activity_type} (Tx: {tx_hash})")
    #             result = {
    #                 "success": False,
    #                 "transaction_hash": tx_hash,
    #                 "message": f"未知的活動類型: {activity_type}"
    #             }

    #         if result:
    #             elapsed = time.time() - start_time
    #             logger.info(f"處理活動 {activity_type} (Tx: {tx_hash}) 完成, 耗時: {elapsed:.2f}秒, Success: {result.get('success', 'N/A')}")
    #         return result

    #     except Exception as e:
    #         logger.exception(f"處理活動 (Tx: {tx_hash}) 時發生不可預期的錯誤: {e}")
    #         return {
    #             "success": False,
    #             "transaction_hash": tx_hash,
    #             "message": f"處理活動時發生不可預期的錯誤: {str(e)}"
    #         }
    
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
            
            # 安全計算價格
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
            
            # 使用緩存獲取代幣資訊
            from_token_symbol = self._get_token_symbol_cached(from_token_address, token_cache)
            to_token_symbol = self._get_token_symbol_cached(to_token_address, token_cache)
            token_name = self._get_token_symbol_cached(token_address, token_cache)
            token_icon = self._get_token_icon_cached(token_address, token_cache)
            token_supply = self._get_token_supply_cached(token_address, token_cache)
            
            # 計算 marketcap
            marketcap = 0
            if float(price) > 0:
                marketcap = float(price) * float(token_supply)
            
            # 使用緩存更新並獲取代幣買入資訊
            if token_address not in token_buy_cache:
                token_buy_cache[token_address] = {
                    "total_amount": Decimal('0'),
                    "total_cost": Decimal('0'),
                    "avg_buy_price": Decimal('0'),
                    "historical_buy_amount": Decimal('0'),
                    "historical_buy_cost": Decimal('0'),
                    "historical_sell_amount": Decimal('0'),
                    "historical_sell_value": Decimal('0'),
                    "realized_profit": Decimal('0'),
                    "last_transaction_time": timestamp
                }
            
            token_buy_info = token_buy_cache[token_address]
            
            # 更新代幣買入資訊緩存
            current_amount = token_buy_info["total_amount"]
            current_cost = token_buy_info["total_cost"]
            
            # 買入/賣出更新邏輯
            if is_buy:
                # 買入: 增加餘額和成本
                token_buy_info["total_amount"] += amount
                token_buy_info["total_cost"] += cost
                token_buy_info["historical_buy_amount"] += amount
                token_buy_info["historical_buy_cost"] += cost
            else:
                # 賣出: 減少餘額但不減少成本，計算利潤
                sell_percentage = amount / current_amount if current_amount > 0 else Decimal('1')
                sell_percentage = min(Decimal('1'), sell_percentage)  # 確保不超過100%
                
                # 計算對應成本
                cost_basis = current_cost * sell_percentage
                
                # 計算利潤
                profit = value_decimal - cost_basis
                
                # 更新緩存
                token_buy_info["total_amount"] -= amount
                if token_buy_info["total_amount"] <= 0:
                    # 如果全部賣出
                    token_buy_info["total_amount"] = Decimal('0')
                    token_buy_info["total_cost"] = Decimal('0')
                else:
                    # 如果部分賣出，減少相應成本
                    token_buy_info["total_cost"] -= cost_basis
                
                token_buy_info["historical_sell_amount"] += amount
                token_buy_info["historical_sell_value"] += value_decimal
                token_buy_info["realized_profit"] += profit
            
            # 更新平均買入價
            if token_buy_info["total_amount"] > 0 and token_buy_info["total_cost"] > 0:
                token_buy_info["avg_buy_price"] = token_buy_info["total_cost"] / token_buy_info["total_amount"]
            else:
                token_buy_info["avg_buy_price"] = Decimal('0')
            
            token_buy_info["last_transaction_time"] = max(token_buy_info["last_transaction_time"], timestamp)
            
            # 計算 holding_percentage
            holding_percentage = 0
            if is_buy and float(value_decimal) > 0:
                wallet_balance_float = float(wallet_balance)
                holding_pct = float(value_decimal) / (wallet_balance_float + float(value_decimal)) * 100
                holding_percentage = min(100, max(-100, holding_pct))
            elif not is_buy and float(amount) > 0:
                current_amount_after = float(token_buy_info["total_amount"])
                if current_amount_after + float(amount) > 0:  # 加上賣出量來計算原始持有量
                    holding_pct = (float(amount) / (current_amount_after + float(amount))) * 100
                    holding_percentage = min(100, max(-100, holding_pct))
                else:
                    holding_percentage = 100
            
            # 計算 realized_profit 和 realized_profit_percentage
            realized_profit = 0
            realized_profit_percentage = 0
            
            if not is_buy:  # 賣出交易
                # 使用緩存或數據庫獲取平均買入價格
                avg_buy_price = 0.0
                
                # 1. 先嘗試從緩存中獲取
                if token_address in token_buy_cache:
                    avg_buy_price = float(token_buy_info["avg_buy_price"])
                
                # 2. 如果緩存中沒有或者平均價格為0，嘗試從數據庫獲取
                if avg_buy_price == 0:
                    try:
                        token_buy_data = self.get_token_buy_data(wallet_address, token_address)
                        historical_buy_amount = self._convert_to_float(token_buy_data.get("historical_total_buy_amount", 0))
                        historical_buy_cost = self._convert_to_float(token_buy_data.get("historical_total_buy_cost", 0))
                        
                        # 計算平均買入價格
                        if historical_buy_amount > 0:
                            avg_buy_price = historical_buy_cost / historical_buy_amount
                            logger.info(f"從數據庫獲取平均買入價格: {avg_buy_price}")
                    except Exception as e:
                        logger.error(f"獲取歷史買入數據時出錯: {e}")
                
                sell_price = float(price)
                logger.info(f"賣出計算 - 代幣: {token_address}, 平均買入價: {avg_buy_price}, 賣出價: {sell_price}, 數量: {float(amount)}")
                
                if avg_buy_price > 0 and float(amount) > 0:
                    # 計算利潤
                    cost_basis = float(amount) * avg_buy_price
                    realized_profit = float(value_decimal) - cost_basis
                    
                    if cost_basis > 0:
                        realized_profit_percentage = (realized_profit / cost_basis) * 100
                    else:
                        realized_profit_percentage = 0
                        
                    logger.info(f"計算利潤 - 成本基礎: {cost_basis}, 銷售價值: {float(value_decimal)}, 利潤: {realized_profit}, 利潤比例: {realized_profit_percentage}%")
                else:
                    realized_profit = 0
                    realized_profit_percentage = 0
                    logger.warning(f"無法計算利潤 - 平均買入價: {avg_buy_price}, 數量: {float(amount)}")

            time = datetime.now(tz_utc8)
            formatted_time = time.strftime('%Y-%m-%d %H:%M:%S.%f')
            # 創建交易資料
            transaction_data = {
                "wallet_address": wallet_address,
                "wallet_balance": wallet_balance,
                "signature": tx_hash,
                "transaction_time": timestamp,
                "transaction_type": 'buy' if is_buy else 'sell',
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
                "chain": "SOLANA",
                "time": formatted_time
            }
            
            return {
                "success": True,
                "transaction_hash": tx_hash,
                "transaction_data": transaction_data,
                "token_address": token_address,
                "amount": float(amount),
                "value": float(value_decimal),
                "transaction_type": 'buy' if is_buy else 'sell'
            }
            
        except Exception as e:
            logger.exception(f"處理代幣交換交易 {tx_hash} 時發生錯誤: {e}")
            return {
                "success": False,
                "transaction_hash": tx_hash,
                "message": f"處理代幣交換交易時發生錯誤: {str(e)}"
            }
    
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
            
            # 創建交易記錄
            return {
                "success": True,
                "transaction_hash": transaction_hash,
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
        logger.info(f"處理批量代幣活動 (主 Tx: {tx_hash_main}) for {wallet_address}")
        
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
            
            logger.info(f"處理批量代幣活動 {tx_hash_main} 完成: {processed_count}/{total_count} 筆活動")
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
                "name": "Tether USD",
                "decimals": 6,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB/logo.png"
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
        
        #
        return {"symbol": None, "name": None, "icon": None}

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
        
        is_buy = transaction_type == "buy"
        
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
        logger.info("開始更新缺失的交易利潤數據")
        
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
                    # 獲取該代幣的買入數據
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
    
    # def save_transaction(self, transaction: Dict[str, Any]) -> bool:
    #     try:
    #         signature = transaction.get("signature")
    #         if not signature:
    #             logger.error("保存交易失敗: 缺少簽名")
    #             return False
        
    #         with self.session_factory() as session:
    #             # 查詢是否存在相同signature的交易
    #             query = select(Transaction).where(Transaction.signature == signature)
    #             existing_transaction = session.execute(query).scalars().first()
                
    #             # 獲取所有需要的代幣信息
    #             token_address = transaction.get("token_address")
    #             from_token_address = transaction.get("from_token_address")
    #             dest_token_address = transaction.get("dest_token_address")
                
    #             # 獲取代幣信息
    #             token_info = self.get_token_info_safely(token_address)
    #             from_token_info = self.get_token_info_safely(from_token_address)
    #             dest_token_info = self.get_token_info_safely(dest_token_address)
                
    #             # 更新交易數據
    #             if token_info:
    #                 if "token_name" not in transaction or not transaction["token_name"]:
    #                     transaction["token_name"] = token_info.get("symbol") or token_info.get("name")
                    
    #                 if "token_icon" not in transaction or not transaction["token_icon"]:
    #                     transaction["token_icon"] = token_info.get("icon") or token_info.get("url")
                    
    #                 # 計算marketcap (如果有supply_float)
    #                 if "marketcap" not in transaction or not transaction["marketcap"]:
    #                     price = self._convert_to_float(transaction.get("price", 0))
    #                     # 從token_info獲取supply_float，如果沒有則使用預設值
    #                     supply_float = token_info.get("supply_float", 1000000000)  # 預設供應量為10億
    #                     if price:  # 只要有價格就計算marketcap
    #                         transaction["marketcap"] = price * supply_float
    #                     else:
    #                         transaction["marketcap"] = 0  # 如果沒有價格，marketcap設為0而不是null
                
    #             # 更新代幣符號
    #             if from_token_info and "from_token_symbol" not in transaction:
    #                 transaction["from_token_symbol"] = from_token_info.get("symbol")
                
    #             if dest_token_info and "dest_token_symbol" not in transaction:
    #                 transaction["dest_token_symbol"] = dest_token_info.get("symbol")

    #             # 計算holding_percentage
    #             transaction_type = transaction.get("transaction_type")
    #             amount = self._convert_to_float(transaction.get("amount", 0))
    #             value = self._convert_to_float(transaction.get("value", 0))
    #             wallet_address = transaction.get("wallet_address")
                
    #             if transaction_type == "buy" and value > 0:
    #                 wallet_balance = self._convert_to_float(transaction.get("wallet_balance", 0))
    #                 holding_pct = value / (wallet_balance + value) * 100
    #                 transaction["holding_percentage"] = min(100, max(-100, holding_pct))
    #             elif transaction_type == "sell" and amount > 0:
    #                 token_buy_data = self.get_token_buy_data(wallet_address, token_address)
    #                 current_amount = self._convert_to_float(token_buy_data.get("total_amount", 0))
    #                 if current_amount > 0:
    #                     holding_pct = (amount / current_amount) * 100
    #                     transaction["holding_percentage"] = min(100, max(-100, holding_pct))
    #                 else:
    #                     transaction["holding_percentage"] = 100
                
    #             # 計算realized_profit和realized_profit_percentage (只有賣出才有)
    #             if transaction_type == "sell":
    #                 token_buy_data = self.get_token_buy_data(wallet_address, token_address)
    #                 avg_buy_price = self._convert_to_float(token_buy_data.get("avg_buy_price", 0))
    #                 sell_price = self._convert_to_float(transaction.get("price", 0))
                    
    #                 logger.info(f"賣出交易計算利潤: 地址={wallet_address}, 代幣={token_address}")
    #                 logger.info(f"獲取到的avg_buy_price={avg_buy_price}, sell_price={sell_price}, amount={amount}")
    #                 logger.info(f"token_buy_data={token_buy_data}")
                    
    #                 if avg_buy_price > 0 and amount > 0:
    #                     realized_profit, realized_profit_percentage = self.calculate_realized_profit(token_address, amount, sell_price, avg_buy_price)
    #                     realized_profit_percentage = min(1000, max(-100, realized_profit_percentage))
    #                     transaction["realized_profit"] = realized_profit
    #                     transaction["realized_profit_percentage"] = realized_profit_percentage
    #                     logger.info(f"計算結果: realized_profit={realized_profit}, realized_profit_percentage={realized_profit_percentage}")
    #                 else:
    #                     logger.warning(f"無法計算利潤: avg_buy_price={avg_buy_price}, amount={amount}")
    #                     transaction["realized_profit"] = 0
    #                     transaction["realized_profit_percentage"] = 0
    #             else:
    #                 # 買入時這些值為0
    #                 transaction["realized_profit"] = 0
    #                 transaction["realized_profit_percentage"] = 0

    #             time = datetime.now(tz_utc8)
    #             formatted_time = time.strftime('%Y-%m-%d %H:%M:%S.%f')

    #             if not transaction.get("wallet_balance"):
    #                 try:
    #                     sol_balance = self.get_sol_balance(wallet_address)
    #                     if sol_balance and sol_balance.get("balance"):
    #                         transaction["wallet_balance"] = sol_balance.get("balance", {}).get("float", 0)
    #                     else:
    #                         logger.warning(f"無法獲取 {wallet_address} 的餘額，使用預設值 0")
    #                         transaction["wallet_balance"] = 0
    #                 except Exception as e:
    #                     logger.error(f"獲取餘額時出錯: {e}")
    #                     transaction["wallet_balance"] = 0  # 使用預設值

    #             # 如果交易已存在，則更新它
    #             if existing_transaction:
    #                 logger.info(f"交易 {signature} 已存在，正在更新...")
                    
    #                 # 更新現有記錄的各個欄位
    #                 existing_transaction.wallet_balance = transaction.get("wallet_balance")
    #                 existing_transaction.token_name = transaction.get("token_name")
    #                 existing_transaction.token_icon = transaction.get("token_icon")
    #                 existing_transaction.value = self._convert_to_float(transaction.get("value"))
    #                 existing_transaction.price = self._convert_to_float(transaction.get("price"))
    #                 existing_transaction.marketcap = transaction.get("marketcap")
    #                 existing_transaction.from_token_symbol = transaction.get("from_token_symbol")
    #                 existing_transaction.dest_token_symbol = transaction.get("dest_token_symbol")
    #                 existing_transaction.holding_percentage = self._convert_to_float(transaction.get("holding_percentage", 0))
    #                 existing_transaction.realized_profit = self._convert_to_float(transaction.get("realized_profit", 0))
    #                 existing_transaction.realized_profit_percentage = self._convert_to_float(transaction.get("realized_profit_percentage", 0))
    #                 existing_transaction.time = formatted_time  # 更新時間戳
                    
    #                 session.commit()
    #                 logger.info(f"成功更新交易 {signature}")
    #                 return True
    #             else:
    #                 # 如果交易不存在，創建新記錄
    #                 new_transaction = Transaction(
    #                     wallet_address=wallet_address,
    #                     wallet_balance=transaction.get("wallet_balance"),
    #                     signature=signature,
    #                     transaction_time=transaction.get("transaction_time"),
    #                     transaction_type=transaction_type,
    #                     token_address=token_address,
    #                     token_name=transaction.get("token_name"),
    #                     token_icon=transaction.get("token_icon"),
    #                     marketcap=transaction.get("marketcap"),
    #                     amount=self._convert_to_float(transaction.get("amount")),
    #                     value=self._convert_to_float(transaction.get("value")),
    #                     price=self._convert_to_float(transaction.get("price")),
    #                     holding_percentage=self._convert_to_float(transaction.get("holding_percentage", 0)),
    #                     realized_profit=self._convert_to_float(transaction.get("realized_profit", 0)),
    #                     realized_profit_percentage=self._convert_to_float(transaction.get("realized_profit_percentage", 0)),
    #                     chain=transaction.get("chain", "SOLANA"),
    #                     from_token_address=from_token_address,
    #                     from_token_symbol=transaction.get("from_token_symbol"),
    #                     from_token_amount=self._convert_to_float(transaction.get("from_token_amount")),
    #                     dest_token_address=dest_token_address,
    #                     dest_token_symbol=transaction.get("dest_token_symbol"),
    #                     dest_token_amount=self._convert_to_float(transaction.get("dest_token_amount")),
    #                     time=formatted_time
    #                 )
                
    #             session.add(new_transaction)
    #             session.commit()
    #             logger.info(f"成功新增交易 {signature} 到資料庫")
    #             return True
                
    #     except Exception as e:
    #         logger.exception(f"保存/更新交易 {signature} 時發生錯誤: {e}")
    #         return False
        
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
                            holding.value_USDT = float(value_USDT)
                            holding.unrealized_profits = float(unrealized_profits)
                            holding.pnl = float(pnl)
                            holding.pnl_percentage = float(pnl_percentage)
                            holding.avg_price = float(avg_buy_price) if avg_buy_price else 0.0
                            holding.is_cleared = False
                            holding.cumulative_cost = float(total_cost) if total_cost else 0.0
                            holding.last_transaction_time = token_data.get("last_transaction_time", current_time)
                            holding.time = datetime.now()
                            
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
                                value_USDT=float(value_USDT),
                                unrealized_profits=float(unrealized_profits),
                                pnl=float(pnl),
                                pnl_percentage=float(pnl_percentage),
                                avg_price=float(avg_buy_price) if avg_buy_price else 0.0,
                                marketcap=token_info.get("marketcap", 0.0),
                                is_cleared=False,
                                cumulative_cost=float(total_cost) if total_cost else 0.0,
                                last_transaction_time=token_data.get("last_transaction_time", current_time),
                                time=datetime.now()
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
        
    async def update_wallet_summary(self, wallet_address: str) -> bool:
        """更新錢包摘要資訊到 WalletSummary 表"""
        logger.info(f"開始更新錢包 {wallet_address} 的摘要資訊")
        
        db_ready = self._init_db_connection()
        if not db_ready:
            logger.error(f"無法更新錢包摘要，資料庫未就緒")
            return False
        
        try:
            with self.session_factory() as session:
                # 查詢現有記錄
                wallet_summary = session.execute(
                    select(WalletSummary).where(WalletSummary.address == wallet_address)
                ).scalar_one_or_none()
                
                # 獲取錢包餘額
                balance_data = await self.get_sol_balance(wallet_address)
                balance = balance_data.get("balance", {}).get("int", 0)
                balance_usd = balance_data.get("balance", {}).get("float", 0)  # 這裡已經是 USD 值
                
                # 預設值設定
                summary_data = {
                    "address": wallet_address,
                    "balance": balance,
                    "balance_USD": balance_usd,
                    "chain": "SOLANA",
                    "tag": None,
                    "twitter_name": None,
                    "twitter_username": None,
                    "is_smart_wallet": False,
                    "wallet_type": 0,
                    "is_active": True,
                }
                
                # 獲取交易數據統計
                tx_stats = await self._calculate_wallet_transaction_stats(wallet_address, session)
                if tx_stats:
                    summary_data.update(tx_stats)
                
                # 如果記錄已存在則更新，否則創建新記錄
                if wallet_summary:
                    logger.info(f"更新錢包 {wallet_address} 的現有摘要記錄")
                    for key, value in summary_data.items():
                        setattr(wallet_summary, key, value)
                    wallet_summary.update_time = datetime.now()
                else:
                    logger.info(f"創建錢包 {wallet_address} 的新摘要記錄")
                    summary_data["update_time"] = datetime.now()
                    wallet_summary = WalletSummary(**summary_data)
                    session.add(wallet_summary)
                
                session.commit()
                logger.info(f"成功更新錢包 {wallet_address} 的摘要記錄")
                return True
                
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
                return {}
            
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
                
                # 買入/賣出交易數
                buy_txs = [tx for tx in period_transactions[period] if tx.transaction_type == 'buy']
                sell_txs = [tx for tx in period_transactions[period] if tx.transaction_type == 'sell']
                stats[f"buy_num_{period}"] = len(buy_txs)
                stats[f"sell_num_{period}"] = len(sell_txs)
                
                # 計算PNL
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
                
                # 計算平均成本
                if period_tokens:
                    # 計算該時期所有buy交易的總成本
                    total_cost = sum(tx.value or 0 for tx in period_txs if tx.transaction_type == 'buy')
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
                
                # 計算勝率
                profitable_tokens = 0
                for token in period_tokens:
                    token_txs = [tx for tx in period_txs if tx.token_address == token]
                    token_profit = sum(tx.realized_profit or 0 for tx in token_txs if tx.transaction_type == 'sell')
                    if token_profit > 0:
                        profitable_tokens += 1
                
                stats[f"win_rate_{period}"] = (profitable_tokens / len(period_tokens)) * 100 if period_tokens else 0
                
                # 計算PNL分佈
                # 需要先獲取每個代幣的PNL百分比
                token_pnl_percentages = []
                for token in period_tokens:
                    token_txs = [tx for tx in period_txs if tx.token_address == token]
                    buy_value = sum(tx.value or 0 for tx in token_txs if tx.transaction_type == 'buy')
                    token_profit = sum(tx.realized_profit or 0 for tx in token_txs if tx.transaction_type == 'sell')
                    
                    if buy_value > 0:
                        pnl_percentage = (token_profit / buy_value) * 100
                        token_pnl_percentages.append(pnl_percentage)
                
                # 計算PNL分佈
                lt50_count = sum(1 for p in token_pnl_percentages if p < 0)  # 小於0的（虧損）
                from0to50_count = sum(1 for p in token_pnl_percentages if 0 <= p <= 50)  # 0-50%
                from50to200_count = sum(1 for p in token_pnl_percentages if 50 < p <= 200)  # 50-200%
                from200to500_count = sum(1 for p in token_pnl_percentages if 200 < p <= 500)  # 200-500%
                gt500_count = sum(1 for p in token_pnl_percentages if p > 500)  # 大於500%

                stats[f"distribution_lt50_{period}"] = lt50_count
                stats[f"distribution_0to50_{period}"] = from0to50_count
                stats[f"distribution_0to200_{period}"] = from50to200_count  # 修正：直接使用50-200%的計數
                stats[f"distribution_200to500_{period}"] = from200to500_count
                stats[f"distribution_gt500_{period}"] = gt500_count

                total_count = len(token_pnl_percentages)
                # 計算分佈百分比
                if total_count > 0:
                    stats[f"distribution_lt50_percentage_{period}"] = (lt50_count / total_count) * 100
                    stats[f"distribution_0to50_percentage_{period}"] = (from0to50_count / total_count) * 100
                    stats[f"distribution_0to200_percentage_{period}"] = ((from50to200_count - from0to50_count) / total_count) * 100
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
            end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
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
                    (Transaction.transaction_type == 'sell') &
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
                        (Transaction.transaction_type == 'sell')
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
            if not rpc_url.startswith(("http://", "https://")):
                rpc_url = f"https://{rpc_url}"
                logger.info(f"修正後的URL: {rpc_url}")
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
        获取 SOL 余额及其美元价值 - 增强错误处理和多次尝试
        """
        # 设定 RPC URLs
        primary_rpc = getattr(settings, "SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        backup_rpc = getattr(settings, "SOLANA_RPC_URL_BACKUP", "https://solana-api.projectserum.com")
        fallback_rpcs = [
            "https://rpc.ankr.com/solana",
            "https://solana-mainnet.rpc.extrnode.com"
        ]
        
        client = None
        max_retries = 3  # 最大尝试次数
        default_balance = {"decimals": 9, "balance": {"int": 0, "float": 0.0}, "lamports": 0}
        
        # 将pubkey转换移到try块内，避免因无效钱包地址导致崩溃
        try:
            pubkey = Pubkey(base58.b58decode(wallet_address))
        except Exception as e:
            logger.error(f"转换钱包地址到Pubkey时失败: {str(e)}")
            return default_balance
        
        # 按顺序尝试所有可用的 RPC
        rpcs_to_try = [primary_rpc, backup_rpc] + fallback_rpcs
        
        for retry in range(max_retries):
            for rpc_index, rpc_url in enumerate(rpcs_to_try):
                try:
                    # 确保URL格式正确
                    if not rpc_url.startswith(("http://", "https://")):
                        rpc_url = f"https://{rpc_url}"
                    
                    # 如果不是第一次尝试，记录日志
                    if retry > 0 or rpc_index > 0:
                        logger.info(f"尝试使用 RPC {rpc_index+1}/{len(rpcs_to_try)} (尝试 {retry+1}/{max_retries}): {rpc_url[:30]}...")
                    
                    # 创建客户端，设置超时时间
                    client = AsyncClient(rpc_url, timeout=15)
                    
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
                            'float': float(usd_value)  # USD 价值
                        },
                        'lamports': lamports
                    }
                    
                except Exception as e:
                    rpc_name = "主" if rpc_index == 0 else "备用" if rpc_index == 1 else f"额外 #{rpc_index-1}"
                    logger.error(f"使用{rpc_name} RPC 获取 SOL 余额时发生异常: {str(e)}")
                    
                    # 关闭客户端连接
                    if client:
                        try:
                            await client.close()
                        except:
                            pass
                        client = None
                    
                    # 如果是最后一个RPC，等待一小段时间后重试
                    if rpc_index == len(rpcs_to_try) - 1 and retry < max_retries - 1:
                        wait_time = (retry + 1) * 1.5  # 逐渐增加等待时间
                        logger.info(f"所有RPC都失败，等待 {wait_time:.1f} 秒后重试...")
                        await asyncio.sleep(wait_time)
        
        # 如果所有尝试都失败，返回默认值
        logger.warning(f"无法获取钱包 {wallet_address} 的余额，所有尝试均失败")
        return default_balance
        
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

# 創建單例實例
transaction_processor = TransactionProcessor()