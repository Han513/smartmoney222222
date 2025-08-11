# app/services/kafka_consumer.py

import json
import asyncio
import logging
from typing import Dict, Any, List, Optional
from aiokafka import AIOKafkaConsumer
from datetime import datetime
import time
import uuid

from app.core.config import settings
from app.services.cache_service import cache_service

# 設置 Kafka 相關日誌級別，減少不必要的警告
logging.getLogger("aiokafka.consumer.group_coordinator").setLevel(logging.ERROR)
logging.getLogger("aiokafka.consumer").setLevel(logging.ERROR)
logging.getLogger("aiokafka").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class KafkaConsumerService:
    """
    Kafka 消費者服務 - 負責訂閱Kafka事件並保存到緩存中
    """
    
    def __init__(self):
        self.consumer = None
        self.topic = settings.KAFKA_TOPIC
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        # 自動生成新的group_id，包含時間戳和隨機字符串
        import uuid
        import time
        timestamp = int(time.time())
        random_suffix = str(uuid.uuid4())[:8]
        self.group_id = f"{settings.KAFKA_GROUP_ID}_{timestamp}_{random_suffix}"
        self.running = False
        self.task = None
        self.cache_key_prefix = "kafka_msg:"
        self.message_queue_key = "kafka_message_queue"
        self.processing_queue_key = "kafka_processing_queue"
        self.max_retries = 3
    
    async def start(self):
        """啟動 Kafka 消費者服務"""
        if self.running:
            logger.info("Kafka 消費者服務已在運行中")
            return True
        
        logger.info(f"啟動 Kafka 消費者服務，連接到 {self.bootstrap_servers}, 主題: {self.topic}, group_id: {self.group_id}")
        
        # 清除舊的Kafka相關緩存數據
        try:
            logger.info("清除舊的Kafka緩存數據...")
            await self._clear_old_cache_data()
            logger.info("舊的Kafka緩存數據已清除")
        except Exception as e:
            logger.warning(f"清除舊緩存數據時發生錯誤: {e}")
        
        try:
            # 設置消費者配置，只使用 aiokafka 0.8.0 支援的參數
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=False,
                # 基本配置 - 優化以提高消費速度
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                max_poll_interval_ms=300000,
                max_poll_records=1000,  # 增加每次拉取的消息數量，從500增加到1000
                fetch_max_wait_ms=100,  # 減少等待時間，從500ms減少到100ms
                fetch_min_bytes=1,
                fetch_max_bytes=52428800,
                # 其他配置
                check_crcs=True
            )
            
            await self.consumer.start()
            self.running = True
            logger.info("Kafka 消費者服務已啟動")
            
            # 創建異步任務來處理消息
            self.task = asyncio.create_task(self._consume_messages())
            return True
        except Exception as e:
            logger.exception(f"啟動 Kafka 消費者服務失敗: {e}")
            return False
    
    async def stop(self):
        """停止 Kafka 消費者服務"""
        if not self.running:
            return
            
        logger.info("停止 Kafka 消費者服務")
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            
        if self.consumer:
            await self.consumer.stop()
            
        logger.info("Kafka 消費者服務已停止")
    
    async def _consume_messages(self):
        """消費 Kafka 消息的主循環 - 僅將消息保存到緩存"""
        try:
            logger.info("開始消費 Kafka 消息")
            
            # 初始化重試計數器
            retry_count = 0
            backoff_time = 1.0  # 初始退避時間（秒）
            
            while self.running:
                try:
                    # 從Kafka獲取消息
                    async for msg in self.consumer:
                        try:
                            # 解析消息
                            event_data = msg.value
                            
                            # 檢查是否是我們期望的格式
                            if not self._validate_event(event_data):
                                # logger.warning(f"收到無效的事件格式: {event_data}")
                                await self.consumer.commit()
                                continue
                            
                            # 生成唯一ID用於跟踪消息
                            message_id = str(uuid.uuid4())
                            
                            # 將消息保存到緩存
                            await self._cache_message(message_id, event_data)
                            
                            # 將消息ID添加到處理隊列
                            await self._add_to_processing_queue(message_id)
                            
                            # 提交位移
                            await self.consumer.commit()
                            
                            # 重置重試計數器
                            retry_count = 0
                            backoff_time = 1.0
                            
                        except Exception as e:
                            logger.exception(f"處理 Kafka 消息時發生錯誤: {e}")
                            # 出錯仍然提交位移，避免同一條消息反覆處理
                            await self.consumer.commit()
                    
                except asyncio.CancelledError:
                    logger.info("消息消費任務被取消")
                    raise
                    
                except Exception as e:
                    retry_count += 1
                    logger.error(f"消費 Kafka 消息時發生錯誤 (嘗試 {retry_count}/{self.max_retries}): {e}")
                    
                    if retry_count >= self.max_retries:
                        logger.critical(f"達到最大重試次數 ({self.max_retries})，將重啟消費者")
                        retry_count = 0
                        await self._restart_consumer()
                    else:
                        # 使用指數退避策略
                        wait_time = backoff_time
                        backoff_time = min(backoff_time * 2, 60)  # 最多等待60秒
                        logger.info(f"等待 {wait_time:.1f} 秒後重試...")
                        await asyncio.sleep(wait_time)
                        
        except asyncio.CancelledError:
            logger.info("消息消費任務被取消")
            raise
        except Exception as e:
            logger.exception(f"消費 Kafka 消息時發生錯誤: {e}")
            if self.running:
                logger.info("嘗試重新啟動消費者...")
                await asyncio.sleep(5)
                self.task = asyncio.create_task(self._consume_messages())
    
    async def _restart_consumer(self):
        """重啟Kafka消費者"""
        logger.info("重啟 Kafka 消費者")
        try:
            if self.consumer:
                await self.consumer.stop()
            
            # 生成新的group_id
            timestamp = int(time.time())
            random_suffix = str(uuid.uuid4())[:8]
            new_group_id = f"{settings.KAFKA_GROUP_ID}_{timestamp}_{random_suffix}"
            self.group_id = new_group_id
            logger.info(f"使用新的group_id: {new_group_id}")
                
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=False,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                max_poll_interval_ms=300000,
                max_poll_records=1000,  # 增加每次拉取的消息數量
                fetch_max_wait_ms=100,  # 減少等待時間
                fetch_min_bytes=1,
                fetch_max_bytes=52428800,
                check_crcs=True
            )
            
            await self.consumer.start()
            logger.info("Kafka 消費者已重啟")
        except Exception as e:
            logger.exception(f"重啟 Kafka 消費者失敗: {e}")
    
    def _validate_event(self, event_data: Dict[str, Any]) -> bool:
        """驗證事件格式"""
        try:
            # 檢查必要字段
            if "event" not in event_data:
                return False
            
            event = event_data["event"]
            required_fields = [
                "network", "tokenAddress", "side", "txnValue", 
                "address", "hash", "price", "timestamp"
            ]
            
            for field in required_fields:
                if field not in event:
                    logger.warning(f"事件缺少必要字段: {field}")
                    return False
            
            # 確認是 SOLANA 網絡
            if event["network"] != "SOLANA":
                logger.info(f"跳過非 SOLANA 網絡事件: {event['network']}")
                return False
            
            # 過濾特定錢包地址
            wallet_address = event.get("address", "")
            if wallet_address in self._get_filtered_wallet_addresses():
                logger.info(f"跳過過濾錢包地址的交易: {wallet_address}")
                return False
            
            # 過濾 poolAddress 結尾為 -R 的交易
            pool_address = event.get("poolAddress", "")
            if pool_address.endswith("-R"):
                logger.info(f"跳過 poolAddress 結尾為 -R 的交易: {pool_address}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"驗證事件格式出錯: {e}")
            return False
    
    def _get_filtered_wallet_addresses(self) -> set:
        """獲取需要過濾的錢包地址列表"""
        return {
            'BF74pYX3A6freX6yfxwKUDDbzvysEiaJchr7zk1PN9wj',
            'MfDuWeqSHEqTFVYZ7LoexgAK9dxk7cy4DFJWjWMGVWa',
            '2kCm1RHGJjeCKL4SA3ZJCLyXqUD7nEJ7GMtVaP7c6jQ8',
            'F9CNKPKRS3FaMgQF4dq8nj5KF1FjTfCUFQvWnVr8ig9g',
            '5cU4swxyAyzbnmPMReCzXHXjfczUki626GHHiwEUmsu7',
            'C4S5tUxPr7RVPrhEN7g2tBgStd2NiXhWLnmAWqmSv5df',
            'Hv4Hkx5dgZtJMDwhoywhE6Wkh6dtbh6EFSpNYUT4VK5t',
            'YubFizptp3MXUAtZmqkRS9DyCkMeZaVwiaRCGBTxayo',
            '7BYDYDC3m67sWV9K3m4j5m9b7dVU3ZkXWgrr7KDxjQ4A',
            'HV1KXxWFaSeriyFvXyx48FqG9BoFbfinB8njCJonqP7K'
        }
    
    async def _cache_message(self, message_id: str, message_data: Dict[str, Any]):
        """將消息保存到緩存"""
        try:
            # 添加元數據
            message_with_metadata = {
                "id": message_id,
                "data": message_data,
                "status": "pending",
                "created_at": int(time.time()),
                "retries": 0,
                "last_error": None
            }
            
            # 保存到緩存，設置7天過期時間
            await cache_service.set(
                f"{self.cache_key_prefix}{message_id}", 
                message_with_metadata,
                expiry=7 * 24 * 3600
            )
            
            # logger.info(f"消息 {message_id} 已保存到緩存")
            return True
        except Exception as e:
            logger.error(f"保存消息到緩存失敗: {e}")
            return False
    
    async def _add_to_processing_queue(self, message_id: str):
        """將消息ID添加到處理隊列"""
        try:
            # 將消息ID添加到隊列（使用Redis列表）
            await cache_service.add_to_list(self.message_queue_key, message_id)
            logger.debug(f"消息 {message_id} 已添加到處理隊列")
            return True
        except Exception as e:
            logger.error(f"添加消息到處理隊列失敗: {e}")
            return False

    async def _clear_old_cache_data(self):
        """清除舊的Kafka相關緩存數據"""
        try:
            logger.info("開始清除舊的Kafka緩存數據...")
            
            # 顯示清除前的狀態
            message_queue_length = await cache_service.get_list_length(self.message_queue_key)
            processing_queue_length = await cache_service.get_list_length(self.processing_queue_key)
            
            logger.info(f"清除前狀態 - 消息隊列: {message_queue_length}, 處理中隊列: {processing_queue_length}")
            
            # 清除消息隊列
            await cache_service.delete(self.message_queue_key)
            await cache_service.delete(self.processing_queue_key)
            logger.info("已清除Kafka消息隊列")
            
            # 清除事件隊列（如果存在）
            await cache_service.delete("smart_token_events_queue")
            logger.info("已清除事件隊列")
            
            # 獲取所有以 "kafka_msg:" 開頭的緩存鍵
            keys_to_delete = await cache_service.keys_pattern(f"{self.cache_key_prefix}*")
            
            if keys_to_delete:
                logger.info(f"將清除 {len(keys_to_delete)} 個舊的Kafka緩存鍵")
                
                # 批量刪除以提高效率
                batch_size = 100
                for i in range(0, len(keys_to_delete), batch_size):
                    batch = keys_to_delete[i:i + batch_size]
                    for key in batch:
                        await cache_service.delete(key)
                    logger.info(f"已清除批次 {i//batch_size + 1}/{(len(keys_to_delete) + batch_size - 1)//batch_size}")
                
                logger.info("舊的Kafka緩存數據已清除")
            else:
                logger.info("沒有舊的Kafka緩存數據需要清除")
            
            # 顯示清除後的狀態
            message_queue_length_after = await cache_service.get_list_length(self.message_queue_key)
            processing_queue_length_after = await cache_service.get_list_length(self.processing_queue_key)
            
            logger.info(f"清除後狀態 - 消息隊列: {message_queue_length_after}, 處理中隊列: {processing_queue_length_after}")
            logger.info("Kafka緩存數據清除完成")
            
        except Exception as e:
            logger.exception(f"清除舊緩存數據時發生錯誤: {e}")
            # 即使清除失敗，也繼續啟動服務
            logger.warning("緩存清除失敗，但服務將繼續啟動")

# 創建全局實例
kafka_consumer = KafkaConsumerService()