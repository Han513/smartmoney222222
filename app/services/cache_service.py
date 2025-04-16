import aioredis
import json
import logging
from typing import Dict, Any, List, Optional, Union
from app.core.config import settings

logger = logging.getLogger(__name__)

class CacheService:
    """
    快取服務 - 使用 Redis 進行資料快取
    """
    
    def __init__(self):
        self.redis = None
        self.redis_url = settings.REDIS_URL
        logger.info(f"Redis 快取服務初始化，URL: {self.redis_url}")

    async def get_redis(self) -> aioredis.Redis:
        """
        獲取或創建 Redis 連接
        """
        if self.redis is None:
            try:
                logger.info(f"建立 Redis 連接: {self.redis_url}")
                self.redis = await aioredis.from_url(
                    self.redis_url,
                    decode_responses=True  # 自動解碼響應
                )
                logger.info("Redis 連接成功")
            except Exception as e:
                logger.error(f"Redis 連接失敗: {str(e)}")
                # 建立連接失敗時，返回一個模擬的 Redis 實例，避免應用崩潰
                self.redis = MockRedis()
                logger.warning("已切換到模擬 Redis 模式 (數據僅保存在內存中)")
        return self.redis
    
    async def set(self, key: str, value: Any, expiry: int = 3600) -> bool:
        """
        設置快取
        
        Args:
            key: 快取鍵
            value: 要快取的值
            expiry: 過期時間（秒）
            
        Returns:
            是否成功設置
        """
        try:
            redis = await self.get_redis()
            json_value = json.dumps(value)
            await redis.set(key, json_value, ex=expiry)
            return True
        except Exception as e:
            logger.error(f"設置快取失敗 ({key}): {str(e)}")
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """
        讀取快取
        
        Args:
            key: 快取鍵
            
        Returns:
            快取的值，如果不存在則為 None
        """
        try:
            redis = await self.get_redis()
            value = await redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"讀取快取失敗 ({key}): {str(e)}")
            return None
    
    async def delete(self, key: str) -> bool:
        """
        刪除快取
        
        Args:
            key: 快取鍵
            
        Returns:
            是否成功刪除
        """
        try:
            redis = await self.get_redis()
            await redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"刪除快取失敗 ({key}): {str(e)}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        檢查快取是否存在
        
        Args:
            key: 快取鍵
            
        Returns:
            是否存在
        """
        try:
            redis = await self.get_redis()
            return await redis.exists(key) > 0
        except Exception as e:
            logger.error(f"檢查快取是否存在失敗 ({key}): {str(e)}")
            return False
    
    async def get_multi(self, keys: List[str]) -> Dict[str, Any]:
        """
        批量獲取多個鍵的值
        
        Args:
            keys: 快取鍵列表
            
        Returns:
            鍵值對映射，僅包含存在的鍵
        """
        if not keys:
            return {}
            
        try:
            redis = await self.get_redis()
            
            # 使用 mget 批量獲取
            values = await redis.mget(keys)
            result = {}
            
            for i, value in enumerate(values):
                if value is not None:
                    try:
                        result[keys[i]] = json.loads(value)
                    except json.JSONDecodeError:
                        logger.warning(f"快取值 {keys[i]} 不是有效的 JSON")
                        result[keys[i]] = value
            
            return result
        except Exception as e:
            logger.error(f"批量獲取快取失敗: {str(e)}")
            # 回退到逐個獲取
            result = {}
            for key in keys:
                value = await self.get(key)
                if value is not None:
                    result[key] = value
            return result
    
    async def set_multi(self, mapping: Dict[str, Any], expiry: int = 3600) -> bool:
        """
        批量設置多個鍵值對
        
        Args:
            mapping: 鍵值對映射
            expiry: 過期時間（秒）
            
        Returns:
            是否全部成功設置
        """
        if not mapping:
            return True
            
        try:
            redis = await self.get_redis()
            pipeline = redis.pipeline()
            
            for key, value in mapping.items():
                try:
                    json_value = json.dumps(value)
                    pipeline.set(key, json_value, ex=expiry)
                except Exception as e:
                    logger.error(f"序列化 {key} 失敗: {str(e)}")
                    continue
            
            await pipeline.execute()
            return True
        except Exception as e:
            logger.error(f"批量設置快取失敗: {str(e)}")
            # 回退到逐個設置
            success = True
            for key, value in mapping.items():
                if not await self.set(key, value, expiry):
                    success = False
            return success
    
    async def close(self):
        """
        關閉 Redis 連接
        """
        if self.redis and not isinstance(self.redis, MockRedis):
            await self.redis.close()
            self.redis = None
            logger.info("Redis 連接已關閉")


class MockRedis:
    """
    模擬 Redis 類，僅用於 Redis 不可用時的回退方案
    """
    
    def __init__(self):
        self.store = {}
        logger.warning("使用模擬 Redis 存儲 (僅用於開發/測試)")
    
    async def set(self, key, value, ex=None):
        self.store[key] = value
        return True
    
    async def get(self, key):
        return self.store.get(key)
    
    async def delete(self, key):
        if key in self.store:
            del self.store[key]
        return True
    
    async def exists(self, key):
        return key in self.store
    
    async def mget(self, keys):
        return [self.store.get(key) for key in keys]
    
    async def close(self):
        self.store.clear()
    
    def pipeline(self):
        return MockPipeline(self)


class MockPipeline:
    """
    模擬 Redis Pipeline
    """
    
    def __init__(self, mock_redis):
        self.mock_redis = mock_redis
        self.commands = []
    
    def set(self, key, value, ex=None):
        self.commands.append(('set', key, value))
        return self
    
    async def execute(self):
        results = []
        for cmd, *args in self.commands:
            if cmd == 'set':
                await self.mock_redis.set(*args)
                results.append(True)
        return results

# 創建單例實例
cache_service = CacheService()