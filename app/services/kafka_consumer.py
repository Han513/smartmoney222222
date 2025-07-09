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
logging.getLogger("aiokafka.consumer.fetcher").setLevel(logging.WARNING)
logging.getLogger("aiokafka.consumer.group_coordinator").setLevel(logging.WARNING)
logging.getLogger("aiokafka.consumer").setLevel(logging.WARNING)
logging.getLogger("aiokafka").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class KafkaConsumerService:
    """
    Kafka 消費者服務 - 負責訂閱Kafka事件並保存到緩存中
    """
    
    def __init__(self):
        self.consumer = None
        self.topic = settings.KAFKA_TOPIC
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        # 初始化時不生成 group_id，在 start 方法中生成
        self.group_id = None
        self.running = False
        self.task = None
        self.cache_key_prefix = "kafka_msg:"
        self.message_queue_key = "kafka_message_queue"
        self.processing_queue_key = "kafka_processing_queue"
        self.max_retries = 5  # 增加重試次數
        self.connection_retry_count = 0
        self.max_connection_retries = 10
    
    async def start(self):
        """啟動 Kafka 消費者服務"""
        if self.running:
            logger.info("Kafka 消費者服務已在運行中")
            return True
        
        # 生成動態 group_id，每次啟動都從最新消息開始消費
        timestamp = int(time.time())
        self.group_id = f"{settings.KAFKA_GROUP_ID}_{timestamp}"
        logger.info(f"🎯 生成動態消費者組 ID: {self.group_id} (時間戳: {timestamp})")
        logger.info(f"🔄 每次重啟都將從最新 Kafka 消息開始消費")
            
        logger.info(f"啟動 Kafka 消費者服務，連接到 {self.bootstrap_servers}, 主題: {self.topic}, 消費者組: {self.group_id}")
        
        # 嘗試連接，如果失敗則重試
        for attempt in range(self.max_connection_retries):
            try:
                await self._create_and_start_consumer()
                self.connection_retry_count = 0  # 重置重試計數
                return True
            except Exception as e:
                self.connection_retry_count += 1
                logger.error(f"啟動 Kafka 消費者服務失敗 (嘗試 {self.connection_retry_count}/{self.max_connection_retries}): {e}")
                
                if self.connection_retry_count >= self.max_connection_retries:
                    logger.critical(f"達到最大連接重試次數，停止嘗試")
                    return False
                
                # 指數退避重試
                wait_time = min(2 ** self.connection_retry_count, 60)  # 最多等待60秒
                logger.info(f"等待 {wait_time} 秒後重試連接...")
                await asyncio.sleep(wait_time)
        
        return False
    
    async def _create_and_start_consumer(self):
        """創建並啟動消費者"""
        # 設置消費者配置，優化處理性能參數和連接穩定性
        consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': "latest",  # 新消費者組從最新消息開始
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'enable_auto_commit': False,
            
            # 增加超時時間，提高連接穩定性
            'request_timeout_ms': 60000,  # 增加到60秒
            'session_timeout_ms': 60000,  # 增加到60秒
            'heartbeat_interval_ms': 10000,  # 增加到10秒，減少心跳頻率
            'max_poll_interval_ms': 600000,  # 增加到10分鐘最大輪詢間隔
            
            # 減少每次輪詢的記錄數，避免處理時間過長
            'max_poll_records': 5,  # 進一步減少到5條
            
            # 優化fetch配置
            'fetch_max_wait_ms': 1000,  # 增加到1秒
            'fetch_min_bytes': 1,
            'fetch_max_bytes': 524288,  # 減少到512KB
            
            # 其他優化
            'check_crcs': False,  # 關閉CRC檢查以提高性能
            'isolation_level': "read_committed"
        }
        
        logger.info(f"Kafka 消費者配置: {consumer_config}")
        
        self.consumer = AIOKafkaConsumer(
            self.topic,
            **consumer_config
        )
        
        await self.consumer.start()
        self.running = True
        logger.info("Kafka 消費者服務已啟動")
        
        # 創建異步任務來處理消息
        self.task = asyncio.create_task(self._consume_messages())
    
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
            try:
                await self.consumer.stop()
            except Exception as e:
                logger.warning(f"停止消費者時發生錯誤: {e}")
            
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
                            # 對於 CommitFailedError，不提交位移，讓消息重新處理
                            if "CommitFailedError" in str(e) or "IllegalGenerationError" in str(e):
                                logger.warning("檢測到位移提交失敗，跳過提交以避免重複處理")
                                continue
                            # 其他錯誤仍然提交位移，避免同一條消息反覆處理
                            try:
                                await self.consumer.commit()
                            except Exception as commit_error:
                                logger.error(f"提交位移失敗: {commit_error}")
                    
                except asyncio.CancelledError:
                    logger.info("消息消費任務被取消")
                    raise
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    logger.error(f"消費 Kafka 消息時發生錯誤 (嘗試 {retry_count}/{self.max_retries}): {e}")
                    
                    # 檢查是否是連接相關錯誤
                    if any(keyword in error_msg for keyword in [
                        "RequestTimedOutError", "ConnectionError", "NetworkError",
                        "IllegalGenerationError", "UnknownMemberIdError", "CommitFailedError"
                    ]):
                        logger.warning("檢測到連接錯誤，立即重啟消費者")
                        retry_count = 0
                        await self._restart_consumer()
                        continue
                    
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
        # 重新生成 group_id，確保從最新消息開始消費
        timestamp = int(time.time())
        self.group_id = f"{settings.KAFKA_GROUP_ID}_{timestamp}"
        logger.info(f"🔄 重啟 Kafka 消費者，新的消費者組: {self.group_id} (時間戳: {timestamp})")
        
        try:
            if self.consumer:
                try:
                    await self.consumer.stop()
                except Exception as stop_error:
                    logger.warning(f"停止舊消費者時發生錯誤: {stop_error}")
            
            # 等待一段時間確保舊連接完全關閉
            await asyncio.sleep(3)
            
            # 重新創建消費者
            await self._create_and_start_consumer()
            logger.info("Kafka 消費者已重啟")
            
        except Exception as e:
            logger.exception(f"重啟 Kafka 消費者失敗: {e}")
            # 如果重啟失敗，等待更長時間後再嘗試
            await asyncio.sleep(15)
    
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
            # 檢查是否已經處理過相同的交易（基於signature去重）
            event = message_data.get("event", {})
            signature = event.get("hash")
            
            if signature:
                # 檢查是否已經處理過這個signature
                existing_message = await cache_service.get(f"processed_signature:{signature}")
                if existing_message:
                    logger.info(f"跳過重複的signature: {signature}")
                    return True
            
            # 添加元數據
            message_with_metadata = {
                "id": message_id,
                "data": message_data,
                "status": "pending",
                "created_at": int(time.time()),
                "retries": 0,
                "last_error": None
            }
            
            # 保存到緩存，設置24小時過期時間（減少Redis內存佔用）
            await cache_service.set(
                f"{self.cache_key_prefix}{message_id}", 
                message_with_metadata,
                expiry=24 * 3600  # 24小時過期
            )
            
            # 如果signature存在，記錄已處理的signature（設置較短的過期時間，避免記憶體洩漏）
            if signature:
                await cache_service.set(
                    f"processed_signature:{signature}",
                    {"processed_at": int(time.time())},
                    expiry=3600  # 1小時過期
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

# 創建全局實例
kafka_consumer = KafkaConsumerService()