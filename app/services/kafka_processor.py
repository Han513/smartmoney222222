# # app/services/kafka_processor.py
# import json
# import asyncio
# import logging
# import time
# from typing import Dict, Any, List, Optional
# from datetime import datetime
# from app.core.config import settings
# from app.services.cache_service import cache_service
# from app.services.transaction_processor import transaction_processor
# from app.services.wallet_summary_service import wallet_summary_service
# import aiohttp

# logger = logging.getLogger(__name__)

# class MessageProcessor:
#     """
#     消息處理器 - 負責從緩存中讀取並處理Kafka消息
#     """
    
#     def __init__(self):
#         self.running = False
#         self.task = None
#         self.cache_key_prefix = "kafka_msg:"
#         self.message_queue_key = "kafka_message_queue"
#         self.processing_queue_key = "kafka_processing_queue"
#         self.max_retries = 3
#         self.batch_size = 10  # 每次處理的批次大小
#         self.processing_interval = 1.0  # 處理間隔(秒)
#         self.error_backoff = {
#             0: 5,    # 首次錯誤等待5秒
#             1: 30,   # 第二次錯誤等待30秒
#             2: 300,  # 第三次錯誤等待5分鐘
#             3: 1800  # 更多錯誤等待30分鐘
#         }
#         self.event_queue_key = "smart_token_events_queue"
#         self.event_batch_size = 50  # 每次批量發送的大小
#         self.event_process_interval = 5.0  # 改為5秒檢查一次
#         self.event_processor_task = None
    
#     async def start(self):
#         """啟動消息處理器和事件處理器"""
#         if self.running:
#             logger.info("消息處理器已在運行中")
#             return
            
#         logger.info("啟動消息處理器和事件處理器")
#         self.running = True
#         self.task = asyncio.create_task(self._process_messages_loop())
#         self.event_processor_task = asyncio.create_task(self._process_events_loop())
#         logger.info("消息處理器和事件處理器已啟動")
    
#     async def stop(self):
#         """停止消息處理器和事件處理器"""
#         if not self.running:
#             return
            
#         logger.info("停止消息處理器和事件處理器")
#         self.running = False
        
#         if self.task:
#             self.task.cancel()
#             try:
#                 await self.task
#             except asyncio.CancelledError:
#                 pass

#         if self.event_processor_task:
#             self.event_processor_task.cancel()
#             try:
#                 await self.event_processor_task
#             except asyncio.CancelledError:
#                 pass
            
#         logger.info("消息處理器和事件處理器已停止")
    
#     async def _process_messages_loop(self):
#         """消息處理主循環"""
#         try:
#             logger.info("開始處理消息隊列")
            
#             while self.running:
#                 try:
#                     # 獲取隊列中的消息批次
#                     message_ids = await cache_service.get_list_items(
#                         self.message_queue_key, 
#                         0, 
#                         self.batch_size - 1
#                     )
                    
#                     if not message_ids:
#                         # 隊列為空，等待一段時間後再檢查
#                         await asyncio.sleep(self.processing_interval)
#                         continue
                    
#                     logger.info(f"從隊列獲取 {len(message_ids)} 條消息")
                    
#                     # 處理批次中的每個消息
#                     for msg_id in message_ids:
#                         # 先將消息從待處理隊列移到處理中隊列
#                         await cache_service.move_list_item(
#                             self.message_queue_key,
#                             self.processing_queue_key,
#                             msg_id
#                         )
                        
#                         # 獲取消息內容
#                         message = await cache_service.get(f"{self.cache_key_prefix}{msg_id}")
                        
#                         if not message:
#                             logger.warning(f"消息 {msg_id} 不存在或已過期")
#                             await cache_service.remove_list_item(self.processing_queue_key, msg_id)
#                             continue
                        
#                         # 處理消息
#                         success = await self._process_message(msg_id, message)
                        
#                         if success:
#                             # 成功處理，從處理中隊列移除
#                             await cache_service.remove_list_item(self.processing_queue_key, msg_id)
#                             # 更新消息狀態為已完成
#                             message["status"] = "completed"
#                             message["completed_at"] = int(time.time())
#                             await cache_service.set(
#                                 f"{self.cache_key_prefix}{msg_id}",
#                                 message,
#                                 expiry=7 * 24 * 3600  # 保留7天
#                             )
#                         else:
#                             # 處理失敗，根據重試次數決定下一步
#                             message["retries"] += 1
#                             message["status"] = "failed"
#                             message["last_processed"] = int(time.time())
                            
#                             if message["retries"] >= self.max_retries:
#                                 logger.warning(f"消息 {msg_id} 達到最大重試次數，標記為永久失敗")
#                                 message["status"] = "permanent_failure"
#                                 # 從處理中隊列移除
#                                 await cache_service.remove_list_item(self.processing_queue_key, msg_id)
#                             else:
#                                 # 計算下次重試時間
#                                 retry_key = min(message["retries"], max(self.error_backoff.keys()))
#                                 backoff_time = self.error_backoff.get(retry_key, 1800)
                                
#                                 # 更新消息
#                                 message["next_retry"] = int(time.time()) + backoff_time
                                
#                                 # 暫時從處理中隊列移除，稍後會重新加入待處理隊列
#                                 await cache_service.remove_list_item(self.processing_queue_key, msg_id)
                                
#                                 # 延遲一段時間後重新加入待處理隊列
#                                 asyncio.create_task(self._requeue_after_delay(msg_id, backoff_time))
                            
#                             # 保存更新後的消息
#                             await cache_service.set(
#                                 f"{self.cache_key_prefix}{msg_id}",
#                                 message,
#                                 expiry=7 * 24 * 3600
#                             )
                    
#                     # 批次處理完成後，短暫休息
#                     await asyncio.sleep(0.1)
                    
#                 except asyncio.CancelledError:
#                     raise
#                 except Exception as e:
#                     logger.exception(f"處理消息批次時發生錯誤: {e}")
#                     await asyncio.sleep(5)  # 發生錯誤後等待5秒
            
#         except asyncio.CancelledError:
#             logger.info("消息處理循環被取消")
#             raise
#         except Exception as e:
#             logger.exception(f"消息處理循環發生未預期的錯誤: {e}")
#             if self.running:
#                 # 嘗試重啟處理循環
#                 await asyncio.sleep(5)
#                 self.task = asyncio.create_task(self._process_messages_loop())
    
#     async def _process_message(self, message_id: str, message: Dict[str, Any]) -> bool:
#         """處理單個消息"""
#         try:
#             start_time = time.time()
#             logger.info(f"開始處理消息: {message_id}")
            
#             # 獲取事件數據
#             event_data = message["data"]
#             event = event_data["event"]
            
#             # 解析事件數據
#             wallet_address = event["address"]
#             token_address = event["tokenAddress"]
#             txn_hash = event["hash"]
#             side = event["side"]  # "buy" 或 "sell"
#             is_buy = side == "buy"
#             price = float(event["price"])
#             amount = float(event["txnValue"])
#             timestamp = int(event["timestamp"])
            
#             # 檢查並修正timestamp (如果是毫秒格式轉換為秒)
#             if timestamp > 10**12:  # 判斷是否為毫秒時間戳 (13位數)
#                 timestamp = timestamp // 1000
#                 logger.info(f"轉換毫秒時間戳為秒: {event['timestamp']} -> {timestamp}")

#             logger.info(f"處理交易: {wallet_address} {side} {amount} of {token_address} at {price}")
            
#             # 構建基本交易數據
#             transaction_data = {
#                 "wallet_address": wallet_address,
#                 "signature": txn_hash,
#                 "transaction_time": timestamp,
#                 "transaction_type": "buy" if is_buy else "sell",
#                 "token_address": token_address,
#                 "amount": amount,
#                 "price": price,
#                 "chain": "SOLANA",
                
#                 # 添加可能存在的其他字段
#                 "from_token_address": event.get("baseMint", ""),
#                 "from_token_amount": float(event.get("fromTokenAmount", 0)),
#                 "dest_token_address": event.get("quoteMint", ""),
#                 "dest_token_amount": float(event.get("toTokenAmount", 0)),
#                 "value": price * amount
#             }
            
#             # 構建 smart_token_event API 所需的數據
#             smart_token_event_data = {
#                 "network": "SOLANA",
#                 "tokenAddress": token_address,
#                 "smartAddress": wallet_address,
#                 "transactionType": "buy" if is_buy else "sell",
#                 "transactionFromAmount": str(event.get("fromTokenAmount", "0")),
#                 "transactionFromToken": event.get("baseMint", ""),  # 從代幣
#                 "transactionToAmount": str(event.get("toTokenAmount", "0")),
#                 "transactionToToken": event.get("quoteMint", ""),  # 到代幣
#                 "transactionPrice": str(price),
#                 "totalPnl": "0",  # 需要計算或從其他地方獲取
#                 "transactionTime": str(timestamp),
#                 "brand": "BYD"
#             }
            
#             # 將事件數據添加到事件隊列
#             await cache_service.add_to_list(
#                 self.event_queue_key, 
#                 json.dumps(smart_token_event_data)
#             )
            
#             # 繼續處理交易保存等其他邏輯
#             save_result = await transaction_processor.save_transaction(transaction_data)
            
#             if save_result:
#                 logger.info(f"成功保存交易: {txn_hash}")

#                 await wallet_summary_service.increment_transaction_counts(
#                     wallet_address, 
#                     "buy" if is_buy else "sell",
#                     timestamp
#                 )
                
#                 processing_time = time.time() - start_time
#                 logger.info(f"完成處理消息 {message_id}，耗時: {processing_time:.2f}秒")
#                 return True
#             else:
#                 logger.error(f"保存交易失敗: {txn_hash}")
#                 return False
                
#         except Exception as e:
#             logger.exception(f"處理消息 {message_id} 時發生錯誤: {e}")
#             return False
    
#     async def _requeue_after_delay(self, message_id: str, delay: int):
#         """延遲一段時間後，將消息重新加入待處理隊列"""
#         try:
#             await asyncio.sleep(delay)
#             # 檢查消息是否仍然存在
#             message = await cache_service.get(f"{self.cache_key_prefix}{message_id}")
#             if not message:
#                 return
                
#             # 檢查消息狀態
#             if message["status"] == "permanent_failure":
#                 return  # 永久失敗的消息不重新加入隊列
                
#             # 將消息ID重新加入待處理隊列
#             await cache_service.add_to_list(self.message_queue_key, message_id)
#             logger.info(f"消息 {message_id} 在延遲 {delay} 秒後重新加入處理隊列")
            
#         except Exception as e:
#             logger.error(f"重新加入消息 {message_id} 到隊列時發生錯誤: {e}")

#     async def _process_events_loop(self):
#         """事件處理循環 - 每5秒批量發送事件到 API"""
#         try:
#             while self.running:
#                 try:
#                     # 每5秒檢查一次隊列
#                     await asyncio.sleep(self.event_process_interval)
                    
#                     # 獲取一批事件
#                     events = await cache_service.get_list_items(
#                         self.event_queue_key,
#                         0,
#                         self.event_batch_size - 1
#                     )

#                     if not events:
#                         logger.debug("沒有待處理的事件")
#                         continue

#                     logger.info(f"開始處理 {len(events)} 個事件")
                    
#                     # 解析事件數據
#                     event_data_list = [json.loads(event) for event in events]
                    
#                     if event_data_list:
#                         # 批量發送事件
#                         try:
#                             async with aiohttp.ClientSession() as session:
#                                 tasks = []
#                                 for event_data in event_data_list:
#                                     task = session.post(
#                                         "http://172.25.183.205/internal/smart_token_event",
#                                         json=event_data,
#                                         headers={"Content-Type": "application/json"},
#                                         timeout=aiohttp.ClientTimeout(total=30)
#                                     )
#                                     tasks.append(task)

#                                 # 並行發送所有請求
#                                 responses = await asyncio.gather(*tasks, return_exceptions=True)
                                
#                                 # 處理響應
#                                 success_count = 0
#                                 for i, response in enumerate(responses):
#                                     if isinstance(response, Exception):
#                                         logger.error(f"發送事件失敗: {str(response)}")
#                                         continue
                                        
#                                     try:
#                                         await response.text()
#                                         if response.status == 200:
#                                             # 成功發送，從隊列中移除
#                                             await cache_service.remove_list_item(
#                                                 self.event_queue_key,
#                                                 events[i]
#                                             )
#                                             success_count += 1
#                                         else:
#                                             logger.error(f"事件發送失敗，狀態碼: {response.status}")
#                                     except Exception as e:
#                                         logger.error(f"處理響應時發生錯誤: {str(e)}")

#                                 logger.info(f"批量處理完成: 成功 {success_count}/{len(events)} 個事件")

#                         except Exception as e:
#                             logger.error(f"批量發送事件時發生錯誤: {str(e)}")

#                 except asyncio.CancelledError:
#                     raise
#                 except Exception as e:
#                     logger.exception(f"事件處理循環發生錯誤: {e}")
#                     await asyncio.sleep(5)

#         except asyncio.CancelledError:
#             logger.info("事件處理循環被取消")
#             raise
#         except Exception as e:
#             logger.exception(f"事件處理循環發生未預期的錯誤: {e}")

# # 創建全局實例
# message_processor = MessageProcessor()
# -------------------------------------------------------------------------------------------------------------------
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
from app.services.wallet_cache_service import wallet_cache_service
from app.models.models import TokenBuyData
from sqlalchemy import select
from app.database.session import async_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class MessageProcessor:
    """
    消息處理器 - 負責從緩存中讀取並處理Kafka消息
    """

    def __init__(self):
        self.running = False
        self.tasks = []  # 改為多個任務
        self.cache_key_prefix = "kafka_msg:"
        self.message_queue_key = "kafka_message_queue"
        self.processing_queue_key = "kafka_processing_queue"
        self.max_retries = 3
        self.batch_size = 20  # 增加批次大小，從5增加到20
        self.base_batch_size = 20  # 基礎批次大小
        self.max_batch_size = 100  # 增加最大批次大小到100
        self.processing_interval = 0.05  # 進一步減少處理間隔到0.05秒
        self.worker_count = 3  # 並行處理器數量
        self.error_backoff = {
            0: 1,    # 首次錯誤等待1秒
            1: 5,    # 第二次錯誤等待5秒
            2: 15,   # 第三次錯誤等待15秒
            3: 60    # 更多錯誤等待1分鐘
        }

    async def start(self):
        """啟動消息處理器"""
        if self.running:
            logger.info("消息處理器已在運行中")
            return

        logger.info("啟動消息處理器")
        
        # 清除舊的處理隊列數據
        try:
            logger.info("清除舊的消息處理隊列數據...")
            await self._clear_old_processing_data()
            logger.info("舊的消息處理隊列數據已清除")
        except Exception as e:
            logger.warning(f"清除舊處理數據時發生錯誤: {e}")
        
        self.running = True
        
        # 啟動多個並行處理任務
        for i in range(self.worker_count):
            task = asyncio.create_task(self._process_messages_loop(worker_id=i))
            self.tasks.append(task)
            logger.info(f"啟動處理器工作線程 {i+1}/{self.worker_count}")
        
        logger.info(f"消息處理器已啟動，共 {self.worker_count} 個並行處理器")

    async def stop(self):
        """停止消息處理器"""
        if not self.running:
            return

        logger.info("停止消息處理器")
        self.running = False

        # 停止所有處理任務
        for task in self.tasks:
            task.cancel()
        
        # 等待所有任務完成
        for task in self.tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self.tasks.clear()
        logger.info("消息處理器已停止")

    async def _process_messages_loop(self, worker_id: int = 0):
        """消息處理主循環"""
        try:
            logger.info(f"工作線程 {worker_id} 開始處理消息隊列")

            while self.running:
                try:
                    # 動態調整批次大小 - 根據隊列長度調整
                    queue_length = await cache_service.get_list_length(self.message_queue_key)
                    if queue_length > 500:
                        # 隊列嚴重積壓，使用最大批次大小
                        self.batch_size = self.max_batch_size
                    elif queue_length > 200:
                        # 隊列積壓嚴重，增加批次大小
                        self.batch_size = min(self.max_batch_size, self.base_batch_size * 3)
                    elif queue_length > 100:
                        # 隊列中等積壓，適度增加批次大小
                        self.batch_size = min(self.max_batch_size, self.base_batch_size * 2)
                    elif queue_length > 50:
                        # 隊列輕微積壓，適度增加批次大小
                        self.batch_size = min(self.max_batch_size, int(self.base_batch_size * 1.5))
                    else:
                        # 隊列正常，使用基礎批次大小
                        self.batch_size = self.base_batch_size
                    
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

                    logger.info(f"工作線程 {worker_id} 從隊列獲取 {len(message_ids)} 條消息")

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
                    await asyncio.sleep(0.02)  # 進一步減少休息時間到0.02秒

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(f"處理消息批次時發生錯誤: {e}")
                    await asyncio.sleep(5)  # 發生錯誤後等待5秒

        except asyncio.CancelledError:
            logger.info("消息處理循環被取消")
            raise
        except Exception as e:
            logger.exception(f"工作線程 {worker_id} 消息處理循環發生未預期的錯誤: {e}")
            if self.running:
                # 嘗試重啟處理循環
                await asyncio.sleep(5)
                # 注意：這裡不應該重新創建任務，因為任務管理已經在start方法中處理

    async def _process_message(self, message_id: str, message: Dict[str, Any]) -> bool:
        """處理單個消息"""
        try:
            start_time = time.time()
            # logger.info(f"開始處理消息: {message_id}")

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
            
            # 檢查並修正timestamp (如果是毫秒格式轉換為秒)
            if timestamp > 10**12:  # 判斷是否為毫秒時間戳 (13位數)
                timestamp = timestamp // 1000
                logger.info(f"轉換毫秒時間戳為秒: {event['timestamp']} -> {timestamp}")

            # 檢查錢包地址是否存在
            wallet_exists = await wallet_cache_service.is_wallet_exists(wallet_address)
            if not wallet_exists:
                # logger.info(f"跳過處理不存在的錢包地址: {wallet_address}")
                return True

            # 使用 WalletTokenState 緩存機制判斷交易類型
            transaction_type = transaction_processor.determine_transaction_type(
                wallet_address, token_address, is_buy, amount
            )

            logger.info(f"處理交易: {wallet_address} {transaction_type} {amount} of {token_address} at {price}")

            # 根據 side 動態判斷 from/dest token（新版邏輯）
            base_mint = event.get("baseMint", "")
            quote_mint = event.get("quoteMint", "")

            if side == "buy":
                dest_token_address = token_address
                from_token_address = base_mint if quote_mint == token_address else quote_mint
                dest_token_amount = float(event.get("toTokenAmount", 0))
                from_token_amount = float(event.get("fromTokenAmount", 0))
            else:  # sell
                from_token_address = token_address
                dest_token_address = base_mint if quote_mint == token_address else quote_mint
                from_token_amount = float(event.get("fromTokenAmount", 0))
                dest_token_amount = float(event.get("toTokenAmount", 0))

            # 構建基本交易數據
            transaction_data = {
                "wallet_address": wallet_address,
                "signature": txn_hash,
                "transaction_time": timestamp,
                "transaction_type": transaction_type,
                "token_address": token_address,
                "amount": amount,
                "price": price,
                "chain": "SOLANA",
                "source": event.get("source", ""),
                "pool_address": event.get("poolAddress", ""),
                "from_token_address": from_token_address,
                "from_token_amount": from_token_amount,
                "dest_token_address": dest_token_address,
                "dest_token_amount": dest_token_amount,
                "value": price * amount
            }

            save_result = await transaction_processor.save_transaction(transaction_data)

            if save_result:
                logger.info(f"成功保存交易: {txn_hash}")

                transaction_processor._update_wallet_token_state_after_transaction(
                    wallet_address, token_address, transaction_type,
                    amount, price * amount, price, timestamp
                )

                await wallet_summary_service.increment_transaction_counts(
                    wallet_address,
                    transaction_type,
                    timestamp
                )

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

    async def _clear_old_processing_data(self):
        """清除舊的處理隊列數據"""
        try:
            # 獲取所有處理中的消息ID
            processing_messages = await cache_service.get_list_items(self.processing_queue_key)
            if not processing_messages:
                return

            logger.info(f"發現 {len(processing_messages)} 條舊的處理中消息，將其移除...")
            for msg_id in processing_messages:
                await cache_service.remove_list_item(self.processing_queue_key, msg_id)
                logger.info(f"移除處理中消息: {msg_id}")

            # 獲取所有待處理的消息ID
            message_queue_messages = await cache_service.get_list_items(self.message_queue_key)
            if not message_queue_messages:
                return

            logger.info(f"發現 {len(message_queue_messages)} 條舊的待處理消息，將其移除...")
            for msg_id in message_queue_messages:
                await cache_service.remove_list_item(self.message_queue_key, msg_id)
                logger.info(f"移除待處理消息: {msg_id}")

        except Exception as e:
            logger.error(f"清除舊處理數據時發生錯誤: {e}")

# 創建全局實例
message_processor = MessageProcessor()