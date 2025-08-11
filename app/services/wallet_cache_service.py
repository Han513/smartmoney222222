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
        self._init_timeout = 15  # 減少初始化超時時間到15秒
    
    async def initialize(self):
        """初始化服務，加載數據到緩存並啟動定期刷新任務"""
        if self._initialized:
            return
            
        logger.info("開始初始化 WalletCacheService...")
        
        try:
            # 使用超時機制進行首次加載
            await asyncio.wait_for(self._load_wallet_addresses(), timeout=self._init_timeout)
            
            # 啟動定期刷新任務
            self._refresh_task = asyncio.create_task(self._periodic_refresh())
            
            self._initialized = True
            logger.info("WalletCacheService 初始化完成")
            
        except asyncio.TimeoutError:
            logger.error(f"WalletCacheService 初始化超時（{self._init_timeout}秒），將使用空緩存繼續運行")
            # 即使超時也設置為已初始化，避免阻塞後續服務
            self._initialized = True

            await cache_service.set(self.cache_key, [], expiry=self.refresh_interval)
            # 啟動定期刷新任務
            self._refresh_task = asyncio.create_task(self._periodic_refresh())
            
        except Exception as e:
            logger.error(f"WalletCacheService 初始化失敗: {e}")
            # 即使失敗也設置為已初始化，避免阻塞後續服務
            self._initialized = True

            await cache_service.set(self.cache_key, [], expiry=self.refresh_interval)
            # 啟動定期刷新任務
            self._refresh_task = asyncio.create_task(self._periodic_refresh())
    
    async def _load_wallet_addresses(self):
        """從數據庫加載所有錢包地址到緩存"""
        try:
            logger.info("開始從數據庫加載錢包地址...")
            start_time = asyncio.get_event_loop().time()
            
            with self.session_factory() as session:
                # 優化：只使用一個查詢，優先使用 WalletSummary 表
                try:
                    session.execute(text("SET search_path TO dex_query_v1;"))
                    stmt = select(WalletSummary.wallet_address).where(WalletSummary.chain == 'SOLANA')
                    addresses = session.execute(stmt).scalars().all()
                    logger.info(f"從 WalletSummary 表加載了 {len(addresses)} 個錢包地址")
                except Exception as e:
                    logger.warning(f"從 WalletSummary 表加載失敗，嘗試使用原生SQL: {e}")
                    # 備用方案：使用原生SQL
                    result = session.execute(text("SELECT wallet_address FROM dex_query_v1.wallet WHERE chain = :chain"), {'chain': 'SOLANA'})
                    addresses = [row[0] for row in result.fetchall()]
                    logger.info(f"從原生SQL加載了 {len(addresses)} 個錢包地址")
                
                # 將地址集合保存到緩存
                await cache_service.set(
                    self.cache_key,
                    list(addresses),
                    expiry=self.refresh_interval
                )
                
                end_time = asyncio.get_event_loop().time()
                duration = end_time - start_time
                logger.info(f"錢包地址加載完成，耗時: {duration:.2f}秒，共 {len(addresses)} 個地址")
                
        except Exception as e:
            logger.error(f"加載錢包地址到緩存時發生錯誤: {str(e)}")
            # 設置空緩存，避免後續查詢失敗
            await cache_service.set(self.cache_key, [], expiry=self.refresh_interval)
            raise
    
    async def _periodic_refresh(self):
        """定期刷新緩存數據"""
        while True:
            try:
                await asyncio.sleep(self.refresh_interval)
                logger.info("開始定期刷新錢包地址緩存...")
                await asyncio.wait_for(self._load_wallet_addresses(), timeout=60)  # 刷新時也設置超時
                logger.info("錢包地址緩存刷新完成")
            except asyncio.TimeoutError:
                logger.warning("定期刷新錢包地址緩存超時，將在下次刷新時重試")
            except Exception as e:
                logger.error(f"刷新錢包地址緩存時發生錯誤: {str(e)}")
    
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
                # 如果緩存不存在，重新加載（使用較短的超時時間）
                try:
                    await asyncio.wait_for(self._load_wallet_addresses(), timeout=10)
                    addresses = await cache_service.get(self.cache_key)
                except asyncio.TimeoutError:
                    logger.warning("重新加載錢包地址超時，返回 False")
                    return False
            
            # 檢查地址是否存在
            return wallet_address in (addresses or [])
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
