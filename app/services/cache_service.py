# import aioredis
# import json
# import logging
# from typing import Dict, Any, List, Optional, Union
# from app.core.config import settings

# logger = logging.getLogger(__name__)

# class CacheService:
#     """
#     快取服務 - 使用 Redis 進行資料快取
#     """
    
#     def __init__(self):
#         self.redis = None
#         self.redis_url = settings.REDIS_URL
#         logger.info(f"Redis 快取服務初始化，URL: {self.redis_url}")

#     async def get_redis(self) -> aioredis.Redis:
#         """
#         獲取或創建 Redis 連接
#         """
#         if self.redis is None:
#             try:
#                 logger.info(f"建立 Redis 連接: {self.redis_url}")
#                 self.redis = await aioredis.from_url(
#                     self.redis_url,
#                     decode_responses=True  # 自動解碼響應
#                 )
#                 logger.info("Redis 連接成功")
#             except Exception as e:
#                 logger.error(f"Redis 連接失敗: {str(e)}")
#                 # 建立連接失敗時，返回一個模擬的 Redis 實例，避免應用崩潰
#                 self.redis = MockRedis()
#                 logger.warning("已切換到模擬 Redis 模式 (數據僅保存在內存中)")
#         return self.redis
    
#     async def set(self, key: str, value: Any, expiry: int = 3600) -> bool:
#         """
#         設置快取
        
#         Args:
#             key: 快取鍵
#             value: 要快取的值
#             expiry: 過期時間（秒）
            
#         Returns:
#             是否成功設置
#         """
#         try:
#             redis = await self.get_redis()
#             json_value = json.dumps(value)
#             
#             # 檢查是否為同步 Redis 客戶端
#             if hasattr(redis, 'set') and not asyncio.iscoroutinefunction(redis.set):
#                 # 同步 Redis 客戶端
#                 redis.set(key, json_value, ex=expiry)
#             else:
#                 # 異步 Redis 客戶端
#                 await redis.set(key, json_value, ex=expiry)
            
#             return True
#         except Exception as e:
#             logger.error(f"設置快取失敗 ({key}): {str(e)}")
#             return False
    
#     async def get(self, key: str) -> Optional[Any]:
#         """
#         讀取快取
        
#         Args:
#             key: 快取鍵
            
#         Returns:
#             快取的值，如果不存在則為 None
#         """
#         try:
#             redis = await self.get_redis()
#             value = await redis.get(key)
#             if value:
#                 return json.loads(value)
#             return None
#         except Exception as e:
#             logger.error(f"讀取快取失敗 ({key}): {str(e)}")
#             return None
    
#     async def delete(self, key: str) -> bool:
#         """
#         刪除快取
        
#         Args:
#             key: 快取鍵
            
#         Returns:
#             是否成功刪除
#         """
#         try:
#             redis = await self.get_redis()
#             await redis.delete(key)
#             return True
#         except Exception as e:
#             logger.error(f"刪除快取失敗 ({key}): {str(e)}")
#             return False
    
#     async def exists(self, key: str) -> bool:
#         """
#         檢查快取是否存在
        
#         Args:
#             key: 快取鍵
            
#         Returns:
#             是否存在
#         """
#         try:
#             redis = await self.get_redis()
#             return await redis.exists(key) > 0
#         except Exception as e:
#             logger.error(f"檢查快取是否存在失敗 ({key}): {str(e)}")
#             return False
    
#     async def get_multi(self, keys: List[str]) -> Dict[str, Any]:
#         """
#         批量獲取多個鍵的值
        
#         Args:
#             keys: 快取鍵列表
            
#         Returns:
#             鍵值對映射，僅包含存在的鍵
#         """
#         if not keys:
#             return {}
            
#         try:
#             redis = await self.get_redis()
            
#             # 檢查是否為同步 Redis 客戶端
#             if hasattr(redis, 'mget') and not asyncio.iscoroutinefunction(redis.mget):
#                 # 同步 Redis 客戶端
#                 values = redis.mget(keys)
#             else:
#                 # 異步 Redis 客戶端
#                 values = await redis.mget(keys)
            
#             result = {}
            
#             for i, value in enumerate(values):
#                 if value is not None:
#                     try:
#                         result[keys[i]] = json.loads(value)
#                     except json.JSONDecodeError:
#                         logger.warning(f"快取值 {keys[i]} 不是有效的 JSON")
#                         result[keys[i]] = value
            
#             return result
#         except Exception as e:
#             logger.error(f"批量獲取快取失敗: {str(e)}")
#             # 回退到逐個獲取
#             result = {}
#             for key in keys:
#                 value = await self.get(key)
#                 if value is not None:
#                     result[key] = value
#             return result
    
#     async def set_multi(self, mapping: Dict[str, Any], expiry: int = 3600) -> bool:
#         """
#         批量設置多個鍵值對
        
#         Args:
#             mapping: 鍵值對映射
#             expiry: 過期時間（秒）
            
#         Returns:
#             是否全部成功設置
#         """
#         if not mapping:
#             return True
            
#         try:
#             redis = await self.get_redis()
#             
#             # 檢查是否為同步 Redis 客戶端
#             if hasattr(redis, 'pipeline') and not asyncio.iscoroutinefunction(redis.pipeline):
#                 # 同步 Redis 客戶端
#                 pipeline = redis.pipeline()
#                 
#                 for key, value in mapping.items():
#                     try:
#                         json_value = json.dumps(value)
#                         pipeline.set(key, json_value, ex=expiry)
#                     except Exception as e:
#                         logger.error(f"序列化 {key} 失敗: {str(e)}")
#                         continue
#                 
#                 pipeline.execute()
#             else:
#                 # 異步 Redis 客戶端
#                 pipeline = redis.pipeline()
#                 
#                 for key, value in mapping.items():
#                     try:
#                         json_value = json.dumps(value)
#                         pipeline.set(key, json_value, ex=expiry)
#                     except Exception as e:
#                         logger.error(f"序列化 {key} 失敗: {str(e)}")
#                         continue
#                 
#                 await pipeline.execute()
#             
#             return True
#         except Exception as e:
#             logger.error(f"批量設置快取失敗: {str(e)}")
#             # 回退到逐個設置
#             success = True
#             for key, value in mapping.items():
#                 if not await self.set(key, value, expiry):
#                     success = False
#             return success
    
#     async def close(self):
#         """
#         關閉 Redis 連接
#         """
#         if self.redis and not isinstance(self.redis, MockRedis):
#             await self.redis.close()
#             self.redis = None
#             logger.info("Redis 連接已關閉")


# class MockRedis:
#     """
#     模擬 Redis 類，僅用於 Redis 不可用時的回退方案
#     """
    
#     def __init__(self):
#         self.store = {}
#         logger.warning("使用模擬 Redis 存儲 (僅用於開發/測試)")
    
#     async def set(self, key, value, ex=None):
#         self.store[key] = value
#         return True
    
#     async def get(self, key):
#         return self.store.get(key)
    
#     async def delete(self, key):
#         if key in self.store:
#             del self.store[key]
#         return True
    
#     async def exists(self, key):
#         return key in self.store
    
#     async def mget(self, keys):
#         return [self.store.get(key) for key in keys]
    
#     async def close(self):
#         self.store.clear()
    
#     def pipeline(self):
#         return MockPipeline(self)


# class MockPipeline:
#     """
#     模擬 Redis Pipeline
#     """
    
#     def __init__(self, mock_redis):
#         self.mock_redis = mock_redis
#         self.commands = []
    
#     def set(self, key, value, ex=None):
#         self.commands.append(('set', key, value))
#         return self
    
#     async def execute(self):
#         results = []
#         for cmd, *args in self.commands:
#             if cmd == 'set':
#                 await self.mock_redis.set(*args)
#                 results.append(True)
#         return results

# # 創建單例實例
# cache_service = CacheService()


# app/services/cache_service.py

import json
import logging
from typing import Dict, Any, List, Optional, Union
from app.core.config import settings
import asyncio

logger = logging.getLogger(__name__)

class CacheService:
    """
    快取服務 - 使用 Redis 進行資料快取，並支援 Kafka 消息處理
    """
    
    def __init__(self):
        self.redis = None
        self.redis_url = settings.REDIS_URL
        logger.info(f"Redis 快取服務初始化，URL: {self.redis_url}")

    async def get_redis(self):
        """
        獲取或創建 Redis 連接
        """
        if self.redis is None:
            try:
                # 使用 redis 庫的異步客戶端
                logger.info("使用 redis 異步客戶端")
                import redis.asyncio as redis
                # 解析 Redis URL
                from urllib.parse import urlparse
                parsed = urlparse(self.redis_url)
                self.redis = redis.Redis(
                    host=parsed.hostname or 'localhost',
                    port=parsed.port or 6379,
                    db=int(parsed.path[1:]) if parsed.path and len(parsed.path) > 1 else 0,
                    decode_responses=True
                )
                # 測試連接
                await self.redis.ping()
                logger.info("Redis 異步連接成功")
            except Exception as e:
                logger.error(f"Redis 連接失敗: {str(e)}")
                # 建立連接失敗時，返回一個模擬的 Redis 實例，避免應用崩潰
                self.redis = MockRedis()
                logger.warning("已切換到模擬 Redis 模式 (數據僅保存在內存中)")
        return self.redis
    
    async def connect(self) -> bool:
        """連接到 Redis 的快捷方法"""
        try:
            await self.get_redis()
            return self.redis is not None and not isinstance(self.redis, MockRedis)
        except Exception as e:
            logger.error(f"連接到 Redis 失敗: {e}")
            return False
    
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
        
    async def delete_sync(self, key: str):
        return await self._redis.delete(key)
    
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
            
            return bool(await redis.exists(key))
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
            
            # 檢查是否為同步 Redis 客戶端
            if hasattr(redis, 'pipeline') and not asyncio.iscoroutinefunction(redis.pipeline):
                # 同步 Redis 客戶端
                pipeline = redis.pipeline()
                
                for key, value in mapping.items():
                    try:
                        json_value = json.dumps(value)
                        pipeline.set(key, json_value, ex=expiry)
                    except Exception as e:
                        logger.error(f"序列化 {key} 失敗: {str(e)}")
                        continue
                
                pipeline.execute()
            else:
                # 異步 Redis 客戶端
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
    
    async def keys_pattern(self, pattern: str) -> List[str]:
        """
        獲取匹配模式的鍵
        
        Args:
            pattern: 模式字符串
            
        Returns:
            匹配的鍵列表
        """
        try:
            redis = await self.get_redis()
            
            return await redis.keys(pattern)
        except Exception as e:
            logger.error(f"獲取鍵模式失敗 (pattern={pattern}): {str(e)}")
            return []

    # === 新增的列表操作方法（用於 Kafka 消息處理）===
    
    async def add_to_list(self, key: str, value: str) -> bool:
        """
        向列表添加元素
        
        Args:
            key: 列表鍵
            value: 要添加的值
            
        Returns:
            是否添加成功
        """
        try:
            redis = await self.get_redis()
            await redis.rpush(key, value)
            return True
        except Exception as e:
            logger.error(f"向列表添加元素失敗 (key={key}, value={value}): {str(e)}")
            return False
    
    async def remove_list_item(self, key: str, value: str) -> bool:
        """
        從列表中移除指定值
        
        Args:
            key: 列表鍵
            value: 要移除的值
            
        Returns:
            是否移除成功
        """
        try:
            redis = await self.get_redis()
            
            await redis.lrem(key, 1, value)
            
            return True
        except Exception as e:
            logger.error(f"從列表移除失敗 (key={key}, value={value}): {str(e)}")
            return False
    
    async def get_list_items(self, key: str, start: int = 0, end: int = -1) -> List[str]:
        """
        獲取列表中的元素
        
        Args:
            key: 列表鍵
            start: 起始索引
            end: 結束索引（-1表示到列表結尾）
            
        Returns:
            列表元素
        """
        try:
            redis = await self.get_redis()
            
            return await redis.lrange(key, start, end)
        except Exception as e:
            logger.error(f"獲取列表元素失敗 (key={key}): {str(e)}")
            return []
    
    async def get_list_length(self, key: str) -> int:
        """
        獲取列表長度
        
        Args:
            key: 列表鍵
            
        Returns:
            列表長度
        """
        try:
            redis = await self.get_redis()
            
            return await redis.llen(key)
        except Exception as e:
            logger.error(f"獲取列表長度失敗 (key={key}): {str(e)}")
            return 0
    
    async def move_list_item(self, source_key: str, dest_key: str, value: str) -> bool:
        """
        將項目從一個列表移動到另一個列表
        
        Args:
            source_key: 源列表鍵
            dest_key: 目標列表鍵
            value: 要移動的值
            
        Returns:
            是否移動成功
        """
        try:
            redis = await self.get_redis()
            
            pipe = redis.pipeline()
            pipe.lrem(source_key, 1, value)
            pipe.rpush(dest_key, value)
            await pipe.execute()
            
            return True
        except Exception as e:
            logger.error(f"移動列表項目失敗 (from={source_key}, to={dest_key}, value={value}): {str(e)}")
            
            # 嘗試分步驟執行
            try:
                redis = await self.get_redis()
                
                # 檢查是否為同步 Redis 客戶端
                if hasattr(redis, 'lrem') and not asyncio.iscoroutinefunction(redis.lrem):
                    # 同步 Redis 客戶端
                    redis.lrem(source_key, 1, value)
                    redis.rpush(dest_key, value)
                else:
                    # 異步 Redis 客戶端
                    await redis.lrem(source_key, 1, value)
                    await redis.rpush(dest_key, value)
                
                return True
            except Exception as inner_e:
                logger.error(f"分步移動列表項目失敗: {inner_e}")
                return False
    
    async def list_pop(self, key: str) -> Optional[str]:
        """
        彈出列表最左側的元素
        
        Args:
            key: 列表鍵
            
        Returns:
            彈出的元素或None
        """
        try:
            redis = await self.get_redis()
            
            return await redis.lpop(key)
        except Exception as e:
            logger.error(f"彈出列表元素失敗 (key={key}): {str(e)}")
            return None
            
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
    擴展支援列表操作
    """
    
    def __init__(self):
        self.store = {}  # 鍵值對存儲
        self.lists = {}  # 列表存儲
        logger.warning("使用模擬 Redis 存儲 (僅用於開發/測試)")
    
    async def set(self, key, value, ex=None):
        # 確保存儲的是JSON字符串，與真實Redis保持一致
        if isinstance(value, str):
            # 如果已經是字符串，直接存儲
            self.store[key] = value
        else:
            # 如果是其他類型，轉換為JSON字符串
            self.store[key] = json.dumps(value)
        return True
    
    async def get(self, key):
        # 返回原始字符串，讓CacheService的get方法進行JSON解析
        return self.store.get(key)
    
    async def delete(self, key):
        if key in self.store:
            del self.store[key]
        if key in self.lists:
            del self.lists[key]
        return True
    
    async def exists(self, key):
        return key in self.store or key in self.lists
    
    async def mget(self, keys):
        return [self.store.get(key) for key in keys]
    
    async def keys(self, pattern):
        import fnmatch
        result = []
        for key in list(self.store.keys()) + list(self.lists.keys()):
            if fnmatch.fnmatch(key, pattern):
                result.append(key)
        return result
    
    # 列表操作
    async def rpush(self, key, value):
        if key not in self.lists:
            self.lists[key] = []
        self.lists[key].append(value)
        return len(self.lists[key])
    
    async def lrem(self, key, count, value):
        if key not in self.lists:
            return 0
            
        if count > 0:
            # 從頭開始移除count個
            times = 0
            i = 0
            while i < len(self.lists[key]) and times < count:
                if self.lists[key][i] == value:
                    self.lists[key].pop(i)
                    times += 1
                else:
                    i += 1
            return times
        elif count < 0:
            # 從尾開始移除count個
            times = 0
            i = len(self.lists[key]) - 1
            while i >= 0 and times < abs(count):
                if self.lists[key][i] == value:
                    self.lists[key].pop(i)
                    times += 1
                i -= 1
            return times
        else:
            # 移除所有
            original_length = len(self.lists[key])
            self.lists[key] = [item for item in self.lists[key] if item != value]
            return original_length - len(self.lists[key])
    
    async def lrange(self, key, start, end):
        if key not in self.lists:
            return []
            
        # 處理負索引
        if start < 0:
            start = max(0, len(self.lists[key]) + start)
        if end < 0:
            end = len(self.lists[key]) + end
        else:
            end = min(end, len(self.lists[key]) - 1)
            
        return self.lists[key][start:end+1]
    
    async def llen(self, key):
        if key not in self.lists:
            return 0
        return len(self.lists[key])
    
    async def lpop(self, key):
        if key not in self.lists or not self.lists[key]:
            return None
        return self.lists[key].pop(0)
    
    def pipeline(self):
        return MockPipeline(self)
    
    async def close(self):
        self.store.clear()
        self.lists.clear()


class MockPipeline:
    """
    模擬 Redis Pipeline，擴展支援列表操作
    """
    
    def __init__(self, mock_redis):
        self.mock_redis = mock_redis
        self.commands = []
    
    def set(self, key, value, ex=None):
        self.commands.append(('set', key, value))
        return self
        
    def lrem(self, key, count, value):
        self.commands.append(('lrem', key, count, value))
        return self
        
    def rpush(self, key, value):
        self.commands.append(('rpush', key, value))
        return self
    
    async def execute(self):
        results = []
        for cmd, *args in self.commands:
            if cmd == 'set':
                await self.mock_redis.set(*args)
                results.append(True)
            elif cmd == 'lrem':
                result = await self.mock_redis.lrem(*args)
                results.append(result)
            elif cmd == 'rpush':
                result = await self.mock_redis.rpush(*args)
                results.append(result)
        
        # 清空命令列表
        self.commands = []
        return results

# 創建單例實例
cache_service = CacheService()