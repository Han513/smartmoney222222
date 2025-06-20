import logging
import asyncio
from typing import Set, Optional
from sqlalchemy import select, text
from app.models.models import WalletSummary
from app.core.db import get_session_factory
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

class WalletCacheService:
    """
    錢包緩存服務 - 負責管理錢包地址的緩存
    """
    
    def __init__(self):
        self.session_factory = get_session_factory()
        self.cache_key = "wallet_addresses"
        self.refresh_interval = 600  # 10分鐘
        self._refresh_task = None
        self._initialized = False
    
    async def initialize(self):
        """初始化服務，加載數據到緩存並啟動定期刷新任務"""
        if self._initialized:
            return
            
        logger.info("開始初始化 WalletCacheService...")
        
        # 首次加載數據
        await self._load_wallet_addresses()
        
        # 啟動定期刷新任務
        self._refresh_task = asyncio.create_task(self._periodic_refresh())
        
        self._initialized = True
        logger.info("WalletCacheService 初始化完成")
    
    async def _load_wallet_addresses(self):
        """從數據庫加載所有錢包地址到緩存"""
        try:
            with self.session_factory() as session:
                result = session.execute(text("SELECT wallet_address FROM dex_query_v1.wallet WHERE chain = :chain"), {'chain': 'SOLANA'})
                wallets = result.fetchall()
                session.execute(text("SET search_path TO dex_query_v1;"))
                # 查詢所有SOLANA錢包地址
                stmt = select(WalletSummary.wallet_address).where(WalletSummary.chain == 'SOLANA')
                addresses = session.execute(stmt).scalars().all()
                
                # 將地址集合保存到緩存
                await cache_service.set(
                    self.cache_key,
                    list(addresses),
                    expiry=self.refresh_interval
                )
                
                logger.info(f"已加載 {len(addresses)} 個錢包地址到緩存")
        except Exception as e:
            logger.error(f"加載錢包地址到緩存時發生錯誤: {str(e)}")
    
    async def _periodic_refresh(self):
        """定期刷新緩存數據"""
        while True:
            try:
                logger.info("開始定期刷新錢包地址緩存...")
                await self._load_wallet_addresses()
                logger.info("錢包地址緩存刷新完成")
            except Exception as e:
                logger.error(f"刷新錢包地址緩存時發生錯誤: {str(e)}")
            
            await asyncio.sleep(self.refresh_interval)
    
    async def is_wallet_exists(self, wallet_address: str) -> bool:
        """
        檢查錢包地址是否存在於緩存中
        
        Args:
            wallet_address: 錢包地址
            
        Returns:
            bool: 錢包地址是否存在
        """
        try:
            # 從緩存獲取地址列表
            addresses = await cache_service.get(self.cache_key)
            
            if addresses is None:
                # 如果緩存不存在，重新加載
                await self._load_wallet_addresses()
                addresses = await cache_service.get(self.cache_key)
            
            # 檢查地址是否存在
            return wallet_address in addresses
        except Exception as e:
            logger.error(f"檢查錢包地址時發生錯誤: {str(e)}")
            return False
    
    async def add_wallet(self, wallet_address: str):
        """
        添加新的錢包地址到緩存
        
        Args:
            wallet_address: 錢包地址
        """
        try:
            addresses = await cache_service.get(self.cache_key)
            
            if addresses is None:
                addresses = []
            
            if wallet_address not in addresses:
                addresses.append(wallet_address)
                await cache_service.set(
                    self.cache_key,
                    addresses,
                    expiry=self.refresh_interval
                )
                logger.info(f"已添加錢包地址 {wallet_address} 到緩存")
        except Exception as e:
            logger.error(f"添加錢包地址到緩存時發生錯誤: {str(e)}")

# 創建單例實例
wallet_cache_service = WalletCacheService() 