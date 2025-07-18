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
        
        # 生成唯一的 group_id，確保每次重啟都消費最新數據
        timestamp = int(time.time())
        unique_id = str(uuid.uuid4())[:8]  # 使用UUID的前8個字符
        self.group_id = f"{settings.KAFKA_GROUP_ID}_{timestamp}_{unique_id}"
        
        self.running = False
        self.task = None
        self.cache_key_prefix = "kafka_msg:"
        self.message_queue_key = "kafka_message_queue"
        self.processing_queue_key = "kafka_processing_queue"
        self.max_retries = 3
        
        logger.info(f"生成新的 Kafka Group ID: {self.group_id}")
    
    async def start(self):
        """啟動 Kafka 消費者服務"""
        if self.running:
            logger.info("Kafka 消費者服務已在運行中")
            return True
            
        logger.info(f"啟動 Kafka 消費者服務，連接到 {self.bootstrap_servers}, 主題: {self.topic}")
        logger.info(f"使用 Group ID: {self.group_id}")
        
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
                # 優化配置
                session_timeout_ms=120000,  # 2分鐘
                heartbeat_interval_ms=2000, # 2秒
                max_poll_interval_ms=900000, # 15分鐘
                max_poll_records=100,        # 每批最多100條，避免單批處理過久
                fetch_max_wait_ms=100,       # 最多等100ms
                fetch_min_bytes=1,
                fetch_max_bytes=52428800,    # 50MB
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
                
            return True
        except Exception as e:
            logger.error(f"驗證事件格式出錯: {e}")
            return False
    
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
            # 清除消息隊列
            await cache_service.delete(self.message_queue_key)
            await cache_service.delete(self.processing_queue_key)
            logger.info("已清除Kafka消息隊列")
            
            # 獲取所有以 "kafka_msg:" 開頭的緩存鍵
            keys_to_delete = await cache_service.keys_pattern(f"{self.cache_key_prefix}*")
            
            if keys_to_delete:
                logger.info(f"將清除 {len(keys_to_delete)} 個舊的Kafka緩存鍵")
                for key in keys_to_delete:
                    await cache_service.delete(key)
                logger.info("舊的Kafka緩存數據已清除")
            else:
                logger.info("沒有舊的Kafka緩存數據需要清除")
            
            # 清除相關的事件隊列緩存
            event_queue_keys = [
                "smart_token_events_queue",
                "kafka_message_queue",
                "kafka_processing_queue"
            ]
            
            for key in event_queue_keys:
                try:
                    await cache_service.delete(key)
                    logger.info(f"已清除事件隊列: {key}")
                except Exception as e:
                    logger.warning(f"清除事件隊列 {key} 時發生錯誤: {e}")
                    
            logger.info("所有相關緩存數據已清除完成")
        except Exception as e:
            logger.exception(f"清除舊緩存數據時發生錯誤: {e}")

# 創建全局實例
kafka_consumer = KafkaConsumerService()