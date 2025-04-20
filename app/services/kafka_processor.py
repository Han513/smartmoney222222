# app/services/kafka_processor.py
import json
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from app.core.config import settings
from app.services.cache_service import cache_service
from app.services.transaction_processor import transaction_processor
from app.services.wallet_summary_service import wallet_summary_service

logger = logging.getLogger(__name__)

class MessageProcessor:
    """
    消息處理器 - 負責從緩存中讀取並處理Kafka消息
    """
    
    def __init__(self):
        self.running = False
        self.task = None
        self.cache_key_prefix = "kafka_msg:"
        self.message_queue_key = "kafka_message_queue"
        self.processing_queue_key = "kafka_processing_queue"
        self.max_retries = 3
        self.batch_size = 10  # 每次處理的批次大小
        self.processing_interval = 1.0  # 處理間隔(秒)
        self.error_backoff = {
            0: 5,    # 首次錯誤等待5秒
            1: 30,   # 第二次錯誤等待30秒
            2: 300,  # 第三次錯誤等待5分鐘
            3: 1800  # 更多錯誤等待30分鐘
        }
    
    async def start(self):
        """啟動消息處理器"""
        if self.running:
            logger.info("消息處理器已在運行中")
            return
            
        logger.info("啟動消息處理器")
        self.running = True
        self.task = asyncio.create_task(self._process_messages_loop())
        logger.info("消息處理器已啟動")
    
    async def stop(self):
        """停止消息處理器"""
        if not self.running:
            return
            
        logger.info("停止消息處理器")
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            
        logger.info("消息處理器已停止")
    
    async def _process_messages_loop(self):
        """消息處理主循環"""
        try:
            logger.info("開始處理消息隊列")
            
            while self.running:
                try:
                    # 獲取隊列中的消息批次
                    message_ids = await cache_service.get_list_items(
                        self.message_queue_key, 
                        0, 
                        self.batch_size - 1
                    )
                    
                    if not message_ids:
                        # 隊列為空，等待一段時間後再檢查
                        await asyncio.sleep(self.processing_interval)
                        continue
                    
                    logger.info(f"從隊列獲取 {len(message_ids)} 條消息")
                    
                    # 處理批次中的每個消息
                    for msg_id in message_ids:
                        # 先將消息從待處理隊列移到處理中隊列
                        await cache_service.move_list_item(
                            self.message_queue_key,
                            self.processing_queue_key,
                            msg_id
                        )
                        
                        # 獲取消息內容
                        message = await cache_service.get(f"{self.cache_key_prefix}{msg_id}")
                        
                        if not message:
                            logger.warning(f"消息 {msg_id} 不存在或已過期")
                            await cache_service.remove_list_item(self.processing_queue_key, msg_id)
                            continue
                        
                        # 處理消息
                        success = await self._process_message(msg_id, message)
                        
                        if success:
                            # 成功處理，從處理中隊列移除
                            await cache_service.remove_list_item(self.processing_queue_key, msg_id)
                            # 更新消息狀態為已完成
                            message["status"] = "completed"
                            message["completed_at"] = int(time.time())
                            await cache_service.set(
                                f"{self.cache_key_prefix}{msg_id}",
                                message,
                                expiry=7 * 24 * 3600  # 保留7天
                            )
                        else:
                            # 處理失敗，根據重試次數決定下一步
                            message["retries"] += 1
                            message["status"] = "failed"
                            message["last_processed"] = int(time.time())
                            
                            if message["retries"] >= self.max_retries:
                                logger.warning(f"消息 {msg_id} 達到最大重試次數，標記為永久失敗")
                                message["status"] = "permanent_failure"
                                # 從處理中隊列移除
                                await cache_service.remove_list_item(self.processing_queue_key, msg_id)
                            else:
                                # 計算下次重試時間
                                retry_key = min(message["retries"], max(self.error_backoff.keys()))
                                backoff_time = self.error_backoff.get(retry_key, 1800)
                                
                                # 更新消息
                                message["next_retry"] = int(time.time()) + backoff_time
                                
                                # 暫時從處理中隊列移除，稍後會重新加入待處理隊列
                                await cache_service.remove_list_item(self.processing_queue_key, msg_id)
                                
                                # 延遲一段時間後重新加入待處理隊列
                                asyncio.create_task(self._requeue_after_delay(msg_id, backoff_time))
                            
                            # 保存更新後的消息
                            await cache_service.set(
                                f"{self.cache_key_prefix}{msg_id}",
                                message,
                                expiry=7 * 24 * 3600
                            )
                    
                    # 批次處理完成後，短暫休息
                    await asyncio.sleep(0.1)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(f"處理消息批次時發生錯誤: {e}")
                    await asyncio.sleep(5)  # 發生錯誤後等待5秒
            
        except asyncio.CancelledError:
            logger.info("消息處理循環被取消")
            raise
        except Exception as e:
            logger.exception(f"消息處理循環發生未預期的錯誤: {e}")
            if self.running:
                # 嘗試重啟處理循環
                await asyncio.sleep(5)
                self.task = asyncio.create_task(self._process_messages_loop())
    
    async def _process_message(self, message_id: str, message: Dict[str, Any]) -> bool:
        """處理單個消息"""
        try:
            start_time = time.time()
            logger.info(f"開始處理消息: {message_id}")
            
            # 獲取事件數據
            event_data = message["data"]
            event = event_data["event"]
            
            # 解析事件數據
            wallet_address = event["address"]
            token_address = event["tokenAddress"]
            txn_hash = event["hash"]
            side = event["side"]  # "buy" 或 "sell"
            is_buy = side == "buy"
            price = float(event["price"])
            amount = float(event["txnValue"])
            timestamp = int(event["timestamp"])
            
            logger.info(f"處理交易: {wallet_address} {side} {amount} of {token_address} at {price}")
            
            # 構建基本交易數據 (只包含必要字段，其餘由 save_transaction 方法補充)
            transaction_data = {
                "wallet_address": wallet_address,
                "signature": txn_hash,
                "transaction_time": timestamp,
                "transaction_type": "buy" if is_buy else "sell",
                "token_address": token_address,
                "amount": amount,
                "price": price,
                "chain": "SOLANA",
                
                # 添加可能存在的其他字段
                "from_token_address": event.get("baseMint", ""),
                "from_token_amount": float(event.get("fromTokenAmount", 0)),
                "dest_token_address": event.get("quoteMint", ""),
                "dest_token_amount": float(event.get("toTokenAmount", 0)),
                "value": price * amount  # 根據價格和數量計算價值
            }
            
            # 使用 transaction_processor 保存交易 (裡面會計算所有必要字段)
            # 注意: 因為 save_transaction 是同步方法，所以不用 await
            save_result = await transaction_processor.save_transaction(transaction_data)
            
            if save_result:
                logger.info(f"成功保存交易: {txn_hash}")

                await wallet_summary_service.increment_transaction_counts(
                    wallet_address, 
                    "buy" if is_buy else "sell",
                    timestamp
                )
                
                # 處理 TokenBuyData 已在 save_transaction 中處理過，無需重複處理
                processing_time = time.time() - start_time
                logger.info(f"完成處理消息 {message_id}，耗時: {processing_time:.2f}秒")
                return True
            else:
                logger.error(f"保存交易失敗: {txn_hash}")
                return False
                
        except Exception as e:
            logger.exception(f"處理消息 {message_id} 時發生錯誤: {e}")
            return False
    
    async def _requeue_after_delay(self, message_id: str, delay: int):
        """延遲一段時間後，將消息重新加入待處理隊列"""
        try:
            await asyncio.sleep(delay)
            # 檢查消息是否仍然存在
            message = await cache_service.get(f"{self.cache_key_prefix}{message_id}")
            if not message:
                return
                
            # 檢查消息狀態
            if message["status"] == "permanent_failure":
                return  # 永久失敗的消息不重新加入隊列
                
            # 將消息ID重新加入待處理隊列
            await cache_service.add_to_list(self.message_queue_key, message_id)
            logger.info(f"消息 {message_id} 在延遲 {delay} 秒後重新加入處理隊列")
            
        except Exception as e:
            logger.error(f"重新加入消息 {message_id} 到隊列時發生錯誤: {e}")

# 創建全局實例
message_processor = MessageProcessor()