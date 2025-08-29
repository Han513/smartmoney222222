# app/services/wallet_summary_service.py

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.models import WalletSummary
from app.core.db import get_session_factory
from app.services.solscan import solscan_client
from app.services.wallet_sync_service import wallet_sync_service

logger = logging.getLogger(__name__)

# 定義 UTC+8 時區
UTC_PLUS_8 = timezone(timedelta(hours=8))

class WalletSummaryService:
    """
    錢包摘要服務 - 負責管理錢包摘要數據的更新
    """
    
    def __init__(self):
        self.session_factory = get_session_factory()
        
    async def create_empty_wallet_summary(self, wallet_address: str) -> bool:
        """
        為沒有交易記錄的錢包創建摘要記錄，所有數值欄位設為 0
        
        Args:
            wallet_address: 錢包地址
            
        Returns:
            bool: 創建是否成功
        """
        try:
            # 獲取錢包餘額
            from app.services.transaction_processor import transaction_processor
            balance_data = await transaction_processor.get_sol_balance(wallet_address)
            balance = balance_data.get("balance", {}).get("int", 0)
            balance_usd = balance_data.get("balance", {}).get("float", 0)
            
            with self.session_factory() as session:
                # 查詢現有記錄
                wallet_summary = self._get_wallet_summary(session, wallet_address)
                
                if not wallet_summary:
                    # 如果記錄不存在，則創建基本記錄
                    wallet_summary = WalletSummary(
                        wallet_address=wallet_address,
                        chain="SOLANA",
                        chain_id=501,
                        is_active=True,
                        update_time=datetime.now(UTC_PLUS_8),
                        # 設置餘額
                        balance=balance,
                        balance_usd=balance_usd,
                        # 設置交易相關欄位為 0
                        total_transaction_num_30d=0,
                        total_transaction_num_7d=0,
                        total_transaction_num_1d=0,
                        buy_num_30d=0,
                        buy_num_7d=0,
                        buy_num_1d=0,
                        sell_num_30d=0,
                        sell_num_7d=0,
                        sell_num_1d=0,
                        win_rate_30d=0,
                        win_rate_7d=0,
                        win_rate_1d=0,
                        pnl_30d=0,
                        pnl_7d=0,
                        pnl_1d=0,
                        pnl_percentage_30d=0,
                        pnl_percentage_7d=0,
                        pnl_percentage_1d=0,
                        wallet_type=0
                    )
                    session.add(wallet_summary)
                    session.commit()
                    logger.info(f"已為無交易記錄的錢包 {wallet_address} 創建基本摘要記錄")
                    
                    return True
                else:
                    # 記錄已存在，更新餘額和時間
                    wallet_summary.balance = balance
                    wallet_summary.balance_usd = balance_usd
                    wallet_summary.update_time = datetime.now(UTC_PLUS_8)
                    session.commit()
                    logger.info(f"錢包 {wallet_address} 的記錄已存在，已更新餘額和時間")
                    try:
                        logger.info(f"正在將錢包 {wallet_address} 添加到同步隊列...")
                        await wallet_sync_service.add_wallet(wallet_address)
                        logger.info(f"錢包 {wallet_address} 已成功添加到同步隊列")
                    except Exception as e:
                        logger.error(f"將錢包 {wallet_address} 添加到同步隊列時發生錯誤: {e}")
                        
                    return True
                    
        except Exception as e:
            logger.exception(f"為無交易記錄的錢包創建摘要記錄時發生錯誤: {e}")
            return False
    
    async def update_full_summary(
        self, 
        wallet_address: str, 
        tx_stats: Dict[str, Any] = None,
        twitter_name: Optional[str] = None,
        twitter_username: Optional[str] = None,
        is_smart_wallet: bool = False
    ) -> bool:
        """
        完整更新錢包摘要（用於歷史數據分析）
        
        Args:
            wallet_address: 錢包地址
            tx_stats: 預先計算的交易統計數據（可選）
            twitter_name: Twitter 名稱（可選）
            twitter_username: Twitter 用戶名（可選）
            
        Returns:
            bool: 更新是否成功
        """
        try:
            from app.services.transaction_processor import transaction_processor
            with self.session_factory() as session:
                # 查詢現有記錄
                wallet_summary = self._get_wallet_summary(session, wallet_address)

                # 獲取錢包餘額
                balance_data = await transaction_processor.get_sol_balance(wallet_address)
                balance = balance_data.get("balance", {}).get("int", 0)
                balance_usd = balance_data.get("balance", {}).get("float", 0)
                # 預設值設定
                summary_data = {
                    "wallet_address": wallet_address,
                    "balance": balance,
                    "balance_usd": balance_usd,
                    "chain": "SOLANA",
                    "tag": "kol" if twitter_name and twitter_username else None,
                    "is_smart_wallet": True if twitter_name and twitter_username else False,
                    "chain_id": 501,
                    "wallet_type": 0,
                    "is_active": True,
                    "twitter_name": twitter_name,
                    "twitter_username": twitter_username,
                    "update_time": datetime.now(UTC_PLUS_8)
                }

                # 如果提供了交易統計數據，則合併
                if tx_stats:
                    summary_data.update(tx_stats)

                # 更新或創建記錄
                if wallet_summary:
                    self._update_wallet_summary(wallet_summary, summary_data)
                else:
                    wallet_summary = WalletSummary(**summary_data)
                    session.add(wallet_summary)

                session.commit()
                logger.info(f"成功完整更新錢包 {wallet_address} 的摘要記錄")
                try:
                    logger.info(f"正在將錢包 {wallet_address} 添加到同步隊列...")
                    await wallet_sync_service.add_wallet(wallet_address)
                    logger.info(f"錢包 {wallet_address} 已成功添加到同步隊列")
                except Exception as e:
                    logger.error(f"將錢包 {wallet_address} 添加到同步隊列時發生錯誤: {e}")

                return True

        except Exception as e:
            logger.exception(f"完整更新錢包摘要時發生錯誤: {e}")
            return False
    
    async def update_partial_summary(self, wallet_address: str, update_data: Dict[str, Any]) -> bool:
        """
        部分更新錢包摘要（用於即時數據更新）
        
        Args:
            wallet_address: 錢包地址
            update_data: 需要更新的字段和值
            
        Returns:
            bool: 更新是否成功
        """
        try:
            with self.session_factory() as session:
                # 查詢現有記錄
                wallet_summary = self._get_wallet_summary(session, wallet_address)
                
                if not wallet_summary:
                    # 如果記錄不存在，則創建基本記錄
                    wallet_summary = WalletSummary(
                        wallet_address=wallet_address,
                        chain="SOLANA",
                        chain_id=501,
                        is_active=True,
                        update_time=datetime.now(UTC_PLUS_8)
                    )
                    session.add(wallet_summary)
                
                # 更新指定字段
                update_data["update_time"] = datetime.now(UTC_PLUS_8)
                self._update_wallet_summary(wallet_summary, update_data)
                
                session.commit()
                logger.info(f"成功部分更新錢包 {wallet_address} 的摘要記錄")
                await wallet_sync_service.add_wallet(wallet_address)

                return True
                
        except Exception as e:
            logger.exception(f"部分更新錢包摘要時發生錯誤: {e}")
            return False
    
    async def increment_transaction_counts(self, wallet_address: str, transaction_type: str, timestamp: int) -> bool:
        """
        增加交易計數（用於即時更新交易數量）
        
        Args:
            wallet_address: 錢包地址
            transaction_type: 交易類型 ("buy" 或 "sell")
            timestamp: 交易時間戳
            
        Returns:
            bool: 更新是否成功
        """
        try:
            with self.session_factory() as session:
                # 查詢現有記錄
                wallet_summary = self._get_wallet_summary(session, wallet_address)
                
                if not wallet_summary:
                    # 如果記錄不存在，則創建基本記錄
                    wallet_summary = WalletSummary(
                        wallet_address=wallet_address,
                        chain="SOLANA",
                        chain_id=501,
                        is_active=True,
                        update_time=datetime.now(UTC_PLUS_8)
                    )
                    session.add(wallet_summary)
                
                # 更新最後交易時間
                wallet_summary.last_transaction_time = max(
                    timestamp, 
                    wallet_summary.last_transaction_time or 0
                )
                
                # 增加相應的交易計數
                # 1天、7天和30天的計數
                self._increment_period_counts(wallet_summary, transaction_type, timestamp)
                
                session.commit()
                logger.info(f"成功更新錢包 {wallet_address} 的交易計數")
                await wallet_sync_service.add_wallet(wallet_address)
                
                return True
                
        except Exception as e:
            logger.exception(f"更新錢包交易計數時發生錯誤: {e}")
            return False
    
    def _get_wallet_summary(self, session: Session, wallet_address: str) -> Optional[WalletSummary]:
        """獲取錢包摘要記錄"""
        return session.execute(
            select(WalletSummary).where(WalletSummary.wallet_address == wallet_address)
        ).scalar_one_or_none()
    
    def _update_wallet_summary(self, wallet_summary: WalletSummary, update_data: Dict[str, Any]) -> None:
        """更新錢包摘要記錄的字段"""
        for key, value in update_data.items():
            if hasattr(wallet_summary, key):
                setattr(wallet_summary, key, value)
    
    def _increment_period_counts(self, wallet_summary: WalletSummary, transaction_type: str, timestamp: int) -> None:
        """增加特定時間段的交易計數"""
        # 對於實時監聽到的交易，直接增加所有時間段的計數
        
        # 更新總交易次數
        wallet_summary.total_transaction_num_1d = (wallet_summary.total_transaction_num_1d or 0) + 1
        wallet_summary.total_transaction_num_7d = (wallet_summary.total_transaction_num_7d or 0) + 1
        wallet_summary.total_transaction_num_30d = (wallet_summary.total_transaction_num_30d or 0) + 1
        
        # 更新買入/賣出次數
        if transaction_type in ["build", "buy"]:
            wallet_summary.buy_num_1d = (wallet_summary.buy_num_1d or 0) + 1
            wallet_summary.buy_num_7d = (wallet_summary.buy_num_7d or 0) + 1
            wallet_summary.buy_num_30d = (wallet_summary.buy_num_30d or 0) + 1
        elif transaction_type in ["sell", "clean"]:
            wallet_summary.sell_num_1d = (wallet_summary.sell_num_1d or 0) + 1
            wallet_summary.sell_num_7d = (wallet_summary.sell_num_7d or 0) + 1
            wallet_summary.sell_num_30d = (wallet_summary.sell_num_30d or 0) + 1

# 創建單例實例
wallet_summary_service = WalletSummaryService()