import asyncio
from typing import Dict, List, Any, Optional, Tuple
import time
from datetime import datetime, timedelta
import logging
from app.services.solscan import solscan_client
from app.services.cache_service import CacheService
from app.services.metrics_calculator import MetricsCalculator
from app.services.transaction_processor import transaction_processor
import concurrent.futures
from functools import lru_cache
import math
from sqlalchemy import update, func
from app.core.db import get_session_factory
from app.models.models import WalletSummary

logger = logging.getLogger(__name__)
cache_service = CacheService()

class WalletAnalyzer:
    """
    錢包分析服務：負責收集錢包數據並計算各種指標
    """
    
    def __init__(self):
        self.metrics_calculator = MetricsCalculator()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)  # Increased thread pool size
        self._processing_queue = asyncio.Queue()
        self._processing_tasks = {}
        self._cache_prefix = "wallet:"
        self._bulk_semaphore = asyncio.Semaphore(30)  # Control bulk API requests
        self._request_counter = 0
        self._last_request_time = time.time()
        self._rate_limit = 20  # Requests per second
        self._running = False
        self._worker_task = None
        self._health_check_interval = 300  # seconds
        self._health_check_task = None
        self._last_activity_time = time.time()
        self._error_backoff = 1.0
        self._shutdown_event = asyncio.Event()
        self._processing_addresses = set()  # 追蹤正在處理中的地址
        self._processing_lock = asyncio.Lock()  # 用於同步訪問
    
    async def start_worker(self):
        """Start the background worker if not already running"""
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._process_queue_worker())
            self._health_check_task = asyncio.create_task(self._health_check())
            self._shutdown_event.clear()
            logger.info("Started wallet analyzer background worker")
    
    async def _health_check(self):
        """Periodically check worker health and restart if necessary"""
        try:
            while self._running and not self._shutdown_event.is_set():
                await asyncio.sleep(self._health_check_interval)
                
                if time.time() - self._last_activity_time > self._health_check_interval * 2:
                    logger.warning("Worker appears to be idle for too long, checking queue")
                    if not self._processing_queue.empty() and len(self._processing_tasks) > 0:
                        logger.warning("Queue has items but no processing activity detected, restarting worker")
                        # Restart worker process
                        if self._worker_task:
                            self._worker_task.cancel()
                        self._worker_task = asyncio.create_task(self._process_queue_worker())
                
        except asyncio.CancelledError:
            logger.info("Health check task cancelled")
        except Exception as e:
            logger.exception(f"Error in health check task: {str(e)}")
    
    async def _process_queue_worker(self):
        """Background worker to process wallet analysis requests from queue"""
        try:
            while self._running and not self._shutdown_event.is_set():
                try:
                    address, future = await asyncio.wait_for(
                        self._processing_queue.get(), 
                        timeout=30
                    )
                    
                    self._last_activity_time = time.time()
                    
                    if address not in self._processing_tasks:
                        logger.warning(f"Processing task for {address} not found, skipping")
                        self._processing_queue.task_done()
                        continue
                    
                    try:
                        result = await self._analyze_wallet_internal(address)
                        if not future.done():
                            future.set_result(result)
                        self._error_backoff = max(1.0, self._error_backoff * 0.9)  # Gradually reduce backoff on success
                    except Exception as e:
                        logger.exception(f"Error processing queued wallet {address}: {str(e)}")
                        if not future.done():
                            future.set_exception(e)
                        # Implement exponential backoff on errors
                        self._error_backoff = min(30, self._error_backoff * 1.5)
                        await asyncio.sleep(self._error_backoff)
                    finally:
                        if address in self._processing_tasks:
                            del self._processing_tasks[address]
                        self._processing_queue.task_done()
                        
                except asyncio.TimeoutError:
                    # No items in queue for a while, just continue
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.exception(f"Error in queue worker: {str(e)}")
                    await asyncio.sleep(self._error_backoff)  # Prevent tight loop on errors
        finally:
            self._running = False
            logger.info("Wallet analyzer background worker stopped")
    
    async def stop_worker(self):
        """Stop the background worker"""
        if self._running:
            logger.info("Stopping wallet analyzer background worker")
            self._shutdown_event.set()
            self._running = False
            
            if self._worker_task:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.exception(f"Error stopping worker task: {str(e)}")
                
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.exception(f"Error stopping health check task: {str(e)}")
                
            # Wait for pending tasks to complete with timeout
            try:
                if not self._processing_queue.empty():
                    logger.info(f"Waiting for {self._processing_queue.qsize()} pending tasks to complete...")
                    await asyncio.wait_for(self._processing_queue.join(), timeout=30)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for queue to empty, some tasks may be incomplete")
            
            logger.info("Wallet analyzer background worker stopped")
    
    async def _rate_limit_request(self):
        """Implement rate limiting for API requests"""
        self._request_counter += 1
        current_time = time.time()
        time_diff = current_time - self._last_request_time
        
        # Reset counter if more than 1 second has passed
        if time_diff > 1:
            self._request_counter = 1
            self._last_request_time = current_time
        
        # If we're exceeding our rate limit, wait
        if self._request_counter > self._rate_limit:
            wait_time = 1 - time_diff
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                self._request_counter = 1
                self._last_request_time = time.time()
    
    async def _analyze_wallet_internal(
        self, 
        address: str, 
        chain: str,
        time_range: int = 7,
        include_metrics: Optional[List[str]] = None,
        twitter_name: Optional[str] = None,
        twitter_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Internal method for analyzing a single wallet
        
        Args:
            address: 錢包地址
            time_range: 分析的時間範圍(天)
            include_metrics: 需要包含的指標列表
            chain: 鏈名稱
            twitter_name: Twitter 名稱
            twitter_username: Twitter 用戶名
        """
        try:
            # 獲取錢包活動記錄
            logger.info(f"Fetching activities for wallet {address}")
            await self._rate_limit_request()
            
            try:
                activities = await solscan_client.get_all_wallet_activities(address)
                logger.info(f"Retrieved {len(activities)} activities for address {address}")
                
                # 添加调试信息: 记录每种活动类型的数量
                activity_types_count = {}
                for activity in activities:
                    activity_type = activity.get("activity_type", "UNKNOWN")
                    activity_types_count[activity_type] = activity_types_count.get(activity_type, 0) + 1
                
                logger.info(f"Activity types count for {address}: {activity_types_count}")
                
                try:
                    logger.info(f"Processing transaction records for {address}")
                    transactions = await transaction_processor.process_wallet_activities(address, activities)
                    logger.info(f"Processed {len(transactions)} transactions for {address}")
                except Exception as e:
                    logger.error(f"Error processing wallet transactions: {str(e)}")
                    transactions = []  # 如果處理失敗，使用空列表
                
            except Exception as e:
                logger.error(f"Error fetching wallet activities: {str(e)}")
                activities = []  # 如果獲取失敗，使用空列表
                transactions = []
            
            # 如果沒有活動，返回模擬結果
            if not activities:
                current_time = int(time.time())
                mock_result = {
                    "address": address,
                    "chain": chain,
                    "total_transactions": 0,
                    "last_updated": current_time,
                    "balance": 0.0,
                    "balance_usd": 0.0,
                    "token_holdings": [],
                    "trading_stats": {
                        "win_rate": 0,
                        "avg_profit": 0,
                        "trades_count": 0
                    },
                    "performance": {
                        "1d": 0,
                        "7d": 0,
                        "30d": 0,
                        "all_time": 0
                    },
                    "is_smart_money": False,
                    "transactions": []
                }
                await cache_service.set(f"{self._cache_prefix}{address}", mock_result, expiry=1800)  # 30分鐘過期
                return mock_result
            
            # 計算時間範圍
            current_time = int(time.time())
            time_ranges = {
                "1d": current_time - 86400,
                "7d": current_time - 86400 * 7,
                "30d": current_time - 86400 * 30
            }
            
            # 計算各項指標
            metrics = {
                "address": address,
                "total_transactions": len(activities),
                "last_transaction_time": activities[0]["block_time"] if activities else None,
                "first_transaction_time": activities[-1]["block_time"] if activities else None,
                "last_updated": current_time,
                # "transactions": transactions
            }
            
            # 1天/7天/30天交易數
            for period, start_time in time_ranges.items():
                period_activities = [a for a in activities if a["block_time"] >= start_time]
                metrics[f"{period}_transactions"] = len(period_activities)
            
            # 計算代幣交易統計
            try:
                token_stats = self.metrics_calculator.calculate_token_stats(activities)
                metrics["token_stats"] = token_stats
                
                # 計算勝率
                win_rate_metrics = self.metrics_calculator.calculate_win_rate(token_stats["tokens"])
                metrics["win_rate"] = win_rate_metrics
                
                # 更新持倉記錄到 Holding 表
                logger.info(f"正在為錢包 {address} 更新持倉記錄")
                holding_update_result = await transaction_processor.update_wallet_holdings(address, token_stats)
                logger.info(f"錢包 {address} 持倉記錄更新結果: {holding_update_result}")
                
                # 更新錢包摘要
                logger.info(f"正在更新錢包 {address} 的摘要資訊")
                await transaction_processor.update_wallet_summary(
                    wallet_address=address,
                    twitter_name=twitter_name,
                    twitter_username=twitter_username
                )
                
            except Exception as e:
                logger.error(f"計算 {address} 代幣統計和勝率錯誤: {str(e)}")
                metrics["token_stats"] = {}
                metrics["win_rate"] = {
                    "win_rate_percentage": 0,
                    "total_tokens_traded": 0,
                    "winning_tokens": 0
                }
            
            # 計算收益指標
            try:
                profit_metrics = self.metrics_calculator.calculate_profit_metrics(activities, time_ranges)
                metrics.update(profit_metrics)
            except Exception as e:
                logger.error(f"計算 {address} 收益指標錯誤: {str(e)}")
                # 添加空的收益指標以避免缺少鍵
                for period in time_ranges.keys():
                    metrics[f"{period}_token_balance"] = {}
                    metrics[f"{period}_token_cost"] = {}
                    metrics[f"{period}_avg_cost"] = {}
                metrics["profit_metrics_error"] = str(e)
            
            # 計算平均買入/賣出價格
            try:
                avg_prices = self.metrics_calculator.calculate_average_prices(activities, time_range)
                metrics.update(avg_prices)
            except Exception as e:
                logger.error(f"計算 {address} 平均價格錯誤: {str(e)}")
                # 添加空的價格指標以避免缺少鍵
                metrics[f"{time_range}d_avg_buy_prices"] = {}
                metrics[f"{time_range}d_avg_sell_prices"] = {}
                metrics[f"{time_range}d_buy_volumes"] = {}
                metrics[f"{time_range}d_sell_volumes"] = {}
                metrics["avg_prices_error"] = str(e)
            
            # 存入快取
            await cache_service.set(f"{self._cache_prefix}{address}", metrics, expiry=1800)  # 30分鐘過期
            logger.info(f"完成 {address} 錢包分析並儲存至快取")
            
            return metrics
        except Exception as e:
            logger.exception(f"分析錢包 {address} 時發生錯誤: {str(e)}")
            # 生成一個基本的回退結果
            current_time = int(time.time())
            fallback_result = {
                "address": address,
                "total_transactions": 0,
                "last_updated": current_time,
                "status": "error",
                "error_message": str(e),
                "is_mock_data": True
            }
            return fallback_result
    
    async def analyze_wallet(
        self, 
        address: str, 
        chain: str,
        time_range: int = 7,
        include_metrics: Optional[List[str]] = None,
        use_cache: bool = True,
        twitter_name: Optional[str] = None,
        twitter_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分析單個錢包，計算指標
        
        Args:
            address: 錢包地址
            chain: 鏈接名稱
            time_range: 分析的時間範圍(天)
            include_metrics: 需要包含的指標列表，如果為空則包含所有
            use_cache: 是否使用快取的交易數據
            twitter_name: Twitter 名稱
            twitter_username: Twitter 用戶名
        
        Returns:
            包含各種指標的字典
        """
        try:
            cache_key = f"{self._cache_prefix}{address}"
            
            # 檢查快取
            if use_cache:
                cached = await cache_service.get(cache_key)
                if cached:
                    # 如果快取的數據包含所有請求的指標，直接返回
                    if not include_metrics or all(metric in cached for metric in include_metrics):
                        logger.info(f"使用快取的分析結果: {address}")
                        return cached
            
            # 檢查是否已有處理中的任務
            async with self._processing_lock:
                if address in self._processing_tasks:
                    logger.info(f"錢包 {address} 分析已在進行中，等待結果")
                    try:
                        # 等待現有任務完成，避免重複分析
                        return await asyncio.wait_for(self._processing_tasks[address], timeout=60)
                    except asyncio.TimeoutError:
                        logger.warning(f"等待 {address} 分析超時，創建新任務")
                    except Exception as e:
                        logger.error(f"等待 {address} 任務時出錯: {str(e)}")
                
                # 創建新任務
                future = asyncio.Future()
                self._processing_tasks[address] = future
                self._processing_addresses.add(address)
            
            # 開始分析
            try:
                logger.info(f"開始分析錢包 {address}")
                result = await self._analyze_wallet_internal(
                    address, 
                    time_range=time_range,
                    include_metrics=include_metrics,
                    chain=chain,
                    twitter_name=twitter_name,
                    twitter_username=twitter_username
                )
                
                # 如果提供了 Twitter 資訊，更新數據庫
                if twitter_name or twitter_username:
                    try:
                        session_factory = get_session_factory()
                        with session_factory() as session:
                            stmt = update(WalletSummary).where(
                                WalletSummary.wallet_address == address,
                                WalletSummary.chain == chain
                            ).values(
                                twitter_name=twitter_name,
                                twitter_username=twitter_username
                            )
                            session.execute(stmt)
                            session.commit()
                            logger.info(f"已更新錢包 {address} 的 Twitter 資訊")
                    except Exception as e:
                        logger.error(f"更新錢包 {address} 的 Twitter 資訊時出錯: {str(e)}")
                
                # 設置結果
                if not future.done():
                    future.set_result(result)
                
                return result
            except Exception as e:
                logger.exception(f"分析錢包 {address} 時出錯: {str(e)}")
                if not future.done():
                    future.set_exception(e)
                raise
            finally:
                # 清理
                async with self._processing_lock:
                    self._processing_addresses.discard(address)
                    if address in self._processing_tasks:
                        del self._processing_tasks[address]
        
        except Exception as e:
            logger.exception(f"分析錢包 {address} 時發生錯誤: {str(e)}")
            # 返回基本結果
            current_time = int(time.time())
            fallback_result = {
                "address": address,
                "total_transactions": 0,
                "last_updated": current_time,
                "status": "error",
                "error_message": str(e),
                "is_mock_data": True
            }
            return fallback_result
    
    def _chunk_addresses(self, addresses: List[str], chunk_size: int) -> List[List[str]]:
        """Split addresses into chunks of specified size"""
        return [addresses[i:i + chunk_size] for i in range(0, len(addresses), chunk_size)]
    
    async def prefetch_wallet_cache(self, addresses: List[str]) -> None:
        """Prefetch wallet data into cache"""
        # Check which addresses are already in cache
        missing_addresses = []
        
        # Check cache in bulk using cache_service.get_multi
        cache_keys = [f"{self._cache_prefix}{addr}" for addr in addresses]
        cached_results = await cache_service.get_multi(cache_keys)
        
        for addr in addresses:
            if f"{self._cache_prefix}{addr}" not in cached_results:
                missing_addresses.append(addr)
        
        if not missing_addresses:
            logger.info("All wallet data already in cache")
            return
        
        logger.info(f"Prefetching data for {len(missing_addresses)} wallets not in cache")
        
        # Start the background processing if not running
        await self.start_worker()
        
        # Add all missing addresses to the processing queue
        for addr in missing_addresses:
            if addr not in self._processing_tasks:
                future = asyncio.Future()
                self._processing_tasks[addr] = future
                await self._processing_queue.put((addr, future))
    
    async def _get_token_addresses_from_activities(self, activities: List[Dict[str, Any]]) -> List[str]:
        """Extract unique token addresses from activities for prefetching"""
        token_addresses = set()
        for activity in activities:
            if 'from_token' in activity and activity['from_token'] and 'address' in activity['from_token']:
                token_addresses.add(activity['from_token']['address'])
            if 'to_token' in activity and activity['to_token'] and 'address' in activity['to_token']:
                token_addresses.add(activity['to_token']['address'])
        return list(token_addresses)
    
    async def _prefetch_token_info(self, token_addresses: List[str]) -> None:
        """Prefetch token information to optimize bulk processing"""
        from app.services.token_repository import token_repository
        
        if not token_addresses:
            return
            
        logger.info(f"Prefetching info for {len(token_addresses)} tokens")
        try:
            # Process in chunks to avoid overwhelming the API
            chunk_size = 50
            token_chunks = [token_addresses[i:i+chunk_size] for i in range(0, len(token_addresses), chunk_size)]
            
            for chunk in token_chunks:
                try:
                    # Get token info in batch
                    await token_repository.get_multiple_token_info(chunk)
                    await asyncio.sleep(1)  # Rate limiting
                except Exception as e:
                    logger.error(f"Error prefetching token chunk: {str(e)}")
        except Exception as e:
            logger.error(f"Error in token prefetching: {str(e)}")
    
    async def analyze_wallet_batch(
        self, 
        addresses: List[str],
        chain: str,
        time_range: int = 7,
        include_metrics: Optional[List[str]] = None,
        max_concurrent: int = 10,
        prefetch: bool = True,
        prefetch_tokens: bool = True,
        twitter_names: Optional[List[str]] = None,  # 改為複數形式
        twitter_usernames: Optional[List[str]] = None  # 改為複數形式
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量分析多個錢包
        
        Args:
            addresses: 錢包地址列表
            time_range: 分析的時間範圍(天)
            include_metrics: 需要包含的指標列表
            max_concurrent: 最大並行處理數
            chain: 鏈接名稱
            prefetch: 是否預先載入錢包資料
            prefetch_tokens: 是否預先載入代幣資料
            twitter_names: Twitter 名稱列表
            twitter_usernames: Twitter 用戶名列表
        
        Returns:
            地址到分析結果的映射
        """
        if not addresses:
            return {}
        
        # 初始化 Twitter 資訊列表
        twitter_names = twitter_names if twitter_names else [None] * len(addresses)
        twitter_usernames = twitter_usernames if twitter_usernames else [None] * len(addresses)
        
        # 驗證 Twitter 資訊列表長度
        if len(twitter_names) != len(addresses):
            logger.warning(f"Twitter 名稱列表長度 ({len(twitter_names)}) 與地址列表長度 ({len(addresses)}) 不匹配")
            twitter_names = twitter_names[:len(addresses)] if len(twitter_names) > len(addresses) else twitter_names + [None] * (len(addresses) - len(twitter_names))
        
        if len(twitter_usernames) != len(addresses):
            logger.warning(f"Twitter 用戶名列表長度 ({len(twitter_usernames)}) 與地址列表長度 ({len(addresses)}) 不匹配")
            twitter_usernames = twitter_usernames[:len(addresses)] if len(twitter_usernames) > len(addresses) else twitter_usernames + [None] * (len(addresses) - len(twitter_usernames))
        
        # 創建地址到 Twitter 資訊的映射
        twitter_info = {
            addr: {
                'name': name,
                'username': username
            } for addr, name, username in zip(addresses, twitter_names, twitter_usernames)
        }
        
        import psutil
        cpu_count = psutil.cpu_count(logical=True)
        available_memory = psutil.virtual_memory().available / (1024 * 1024 * 1024)  # GB
        
        # 根據系統資源動態計算並行數
        adjusted_concurrent = min(
            max_concurrent,
            cpu_count * 2,  # CPU核心數的2倍
            max(5, int(available_memory * 2)),  # 每GB記憶體處理2個任務
            math.ceil(len(addresses) / 8) + 5  # 基於地址數量調整
        )
        logger.info(f"動態調整並行處理數為: {adjusted_concurrent}")
        
        start_time = time.time()
        total_wallets = len(addresses)
        logger.info(f"==== 開始批量分析 {total_wallets} 個錢包 ====")
        
        # Remove duplicate addresses
        unique_addresses = list(dict.fromkeys(addresses))
        if len(unique_addresses) < len(addresses):
            logger.info(f"Removed {len(addresses) - len(unique_addresses)} duplicate addresses")
            addresses = unique_addresses
            
        logger.info(f"批量分析 {len(addresses)} 個錢包，最大並行處理數: {max_concurrent}")
        
        # 動態調整並行數量 - 根據資料量調整
        adjusted_concurrent = min(max_concurrent, math.ceil(len(addresses) / 10) + 5)
        logger.info(f"調整並行處理數為: {adjusted_concurrent}")
        
        # Start the background worker if not running
        await self.start_worker()
        
        # 優先處理策略: 先從快取中獲取所有可用的結果
        results = {}
        remaining_addresses = []
        
        # 檢查快取
        cache_keys = [f"{self._cache_prefix}{addr}" for addr in addresses]
        cached_results = await cache_service.get_multi(cache_keys)
        
        # 從快取中提取結果
        for addr in addresses:
            cache_key = f"{self._cache_prefix}{addr}"
            if cache_key in cached_results:
                results[addr] = cached_results[cache_key]
            else:
                remaining_addresses.append(addr)
        
        if not remaining_addresses:
            logger.info(f"所有 {len(addresses)} 個錢包的分析結果都來自快取")
            return results
        
        logger.info(f"從快取獲取了 {len(results)} 個結果，還需分析 {len(remaining_addresses)} 個錢包")
        
        if prefetch:
            # Prefetch remaining wallet data in the background
            prefetch_task = asyncio.create_task(self.prefetch_wallet_cache(remaining_addresses))
            
        # 對剩餘地址進行分組處理
        address_groups = self.group_addresses_by_complexity(remaining_addresses)

        end_time = time.time()
        total_time = end_time - start_time
        processed_wallets = len(results)
        
        # 在終端機顯示統計信息
        logger.info(f"==== 錢包分析完成統計 ====")
        logger.info(f"總分析時間: {total_time:.2f} 秒")
        logger.info(f"處理錢包數: {processed_wallets}/{total_wallets}")
        logger.info(f"平均每個錢包耗時: {total_time/max(1, processed_wallets):.2f} 秒")
        logger.info(f"==============================")
        
        # 分組處理地址
        async def process_group(group_name: str, addrs: List[str]) -> None:
            logger.info(f"開始處理 {group_name} 組的 {len(addrs)} 個地址")
            
            # Create semaphore for this group
            group_semaphore = asyncio.Semaphore(adjusted_concurrent)
            
            # If group is large, prefetch token data for commonly used tokens
            token_prefetch_task = None
            if prefetch_tokens and len(addrs) > 10:
                # Sample a few addresses to get common tokens
                sample_size = min(5, len(addrs))
                sample_addresses = addrs[:sample_size]
                
                async def fetch_token_addresses():
                    all_token_addresses = []
                    for addr in sample_addresses:
                        try:
                            activities = await solscan_client.get_all_wallet_activities(addr)
                            if activities:
                                token_addresses = await self._get_token_addresses_from_activities(activities)
                                all_token_addresses.extend(token_addresses)
                        except Exception as e:
                            logger.error(f"Error fetching sample activities for {addr}: {str(e)}")
                    
                    # Prefetch unique token addresses
                    unique_tokens = list(set(all_token_addresses))
                    if unique_tokens:
                        await self._prefetch_token_info(unique_tokens)
                
                # Start token prefetching in background
                token_prefetch_task = asyncio.create_task(fetch_token_addresses())
            
            async def analyze_with_semaphore(addr: str) -> Tuple[str, Dict[str, Any]]:
                async with group_semaphore:
                    try:
                        return addr, await self.analyze_wallet(
                            addr, 
                            time_range=time_range,
                            include_metrics=include_metrics,
                            chain=chain,
                            twitter_name=twitter_info[addr]['name'],
                            twitter_username=twitter_info[addr]['username']
                        )
                    except Exception as e:
                        logger.exception(f"Error analyzing wallet {addr}: {str(e)}")
                        current_time = int(time.time())
                        return addr, {
                            "address": addr,
                            "status": "error",
                            "error_message": str(e),
                            "last_updated": current_time,
                            "is_mock_data": True
                        }
            
            # Wait for token prefetching to complete before processing the group
            if token_prefetch_task:
                try:
                    await asyncio.wait_for(asyncio.shield(token_prefetch_task), timeout=30)
                except (asyncio.TimeoutError, Exception) as e:
                    logger.warning(f"Token prefetching timed out or failed: {str(e)}")
            
            tasks = []
            for addr in addrs:
                # Skip addresses that are already being processed
                if addr in self._processing_tasks:
                    tasks.append(asyncio.ensure_future(
                        self._processing_tasks[addr].then(lambda result: (addr, result))
                    ))
                else:
                    tasks.append(asyncio.ensure_future(analyze_with_semaphore(addr)))
            
            # Use gather with return_exceptions to prevent one failure from affecting others
            group_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 處理結果
            for item in group_results:
                try:
                    if isinstance(item, Exception):
                        logger.error(f"Group task failed: {str(item)}")
                        continue
                        
                    addr, result = item
                    results[addr] = result
                except Exception as e:
                    logger.exception(f"Error processing group result: {str(e)}")
        
        try:
            # 首先處理簡單的地址，然後是中等複雜度，最後是複雜的地址
            for group_name in ["simple", "medium", "complex"]:
                if group_name in address_groups and address_groups[group_name]:
                    await process_group(group_name, address_groups[group_name])
            
            logger.info(f"完成批量分析 {len(addresses)} 個錢包")
            
            # Wait for prefetch task to complete if it's still running
            if prefetch and 'prefetch_task' in locals():
                try:
                    # Don't wait too long - this is just cleanup
                    await asyncio.wait_for(asyncio.shield(prefetch_task), timeout=1.0)
                except asyncio.TimeoutError:
                    # It's ok if it's still running
                    pass
                
            return results
            
        except Exception as e:
            logger.exception(f"批量分析錢包時發生錯誤: {str(e)}")
            # Return partial results if available
            return results
    
    def group_addresses_by_complexity(self, addresses: List[str]) -> Dict[str, List[str]]:
        """
        根據預估的複雜度將地址分組，用於優化批處理順序
        """
        # 將地址平均分為三組
        total = len(addresses)
        simple_count = total // 3
        medium_count = simple_count
        complex_count = total - simple_count - medium_count
        
        return {
            "simple": addresses[:simple_count],
            "medium": addresses[simple_count:simple_count + medium_count],
            "complex": addresses[simple_count + medium_count:]
        }
    
    async def close(self):
        """清理資源"""
        # 設置一個內部關閉標誌
        self._shutdown_event.set()  # 使所有循環退出
        self._running = False
        
        # 取消所有正在處理中的任務
        for future in list(self._processing_tasks.values()):
            if not future.done():
                future.cancel()
        
        await self.stop_worker()
        
        # 確保執行器正確關閉
        if self._executor:
            self._executor.shutdown(wait=False)
        
        try:
            await cache_service.close()
        except Exception as e:
            logger.exception(f"Error closing cache service: {str(e)}")

# 創建實例
wallet_analyzer = WalletAnalyzer()