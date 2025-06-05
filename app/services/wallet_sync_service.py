# app/services/wallet_sync_service.py

import asyncio
import logging
import time
import json
import aiohttp
from typing import Dict, List, Any, Set
from datetime import datetime, date
from sqlalchemy import select, event
from sqlalchemy.orm import Session

from app.models.models import WalletSummary
from app.core.db import get_session_factory
from app.core.config import settings

logger = logging.getLogger(__name__)

class WalletSyncService:
    """
    錢包同步服務 - 負責將錢包摘要更新推送到外部 API
    """
    
    def __init__(self):
        """初始化錢包同步服務"""
        self.running = False
        self.task = None
        self.lock = asyncio.Lock()
        self.updated_wallets = set()  # 待同步的錢包地址集合
        self.wallet_update_counts = {}  # 錢包更新計數
        self.wallet_update_times = {}  # 錢包最後更新時間
        self.last_sync_time = 0  # 上次同步時間
        
        # 配置參數
        self.sync_interval = 30  # 同步間隔（秒），從 60 減少到 30
        self.max_batch_size = 5  # 最大批次大小，從 10 減少到 5
        self.debounce_interval = 10  # 去抖動間隔（秒），從 60 減少到 10
        self.retry_intervals = [5, 15, 30]  # 重試間隔（秒）
        
        api_endpoint = settings.WALLET_SYNC_API_ENDPOINT
    
        # 確保 API 端點包含協議前綴
        if api_endpoint and not api_endpoint.startswith(('http://', 'https://')):
            api_endpoint = f"http://{api_endpoint}"
        
        self.api_endpoint = api_endpoint
        
        # 數據庫會話工廠
        self.session_factory = get_session_factory()
    
    async def start(self, interval: int = None):
        """啟動同步服務"""
        if self.running:
            logger.info("錢包同步服務已在運行中")
            return
        
        if interval:
            self.sync_interval = interval
            
        logger.info(f"啟動錢包同步服務，同步間隔: {self.sync_interval}秒")
        self.running = True
        self.task = asyncio.create_task(self._sync_loop())
        logger.info("錢包同步服務已啟動")
    
    async def stop(self):
        """停止同步服務"""
        if not self.running:
            return
            
        logger.info("停止錢包同步服務")
        self.running = False
        
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
                
        logger.info("錢包同步服務已停止")

    async def add_wallet(self, wallet_address: str):
        """添加待同步的錢包地址，帶去抖動機制"""
        
        current_time = time.time()
        
        # 檢查是否需要去抖動
        last_update = self.wallet_update_times.get(wallet_address, 0)
        if current_time - last_update < self.debounce_interval:
            self.wallet_update_times[wallet_address] = current_time
            self.wallet_update_counts[wallet_address] = self.wallet_update_counts.get(wallet_address, 0) + 1
            logger.debug(f"錢包 {wallet_address} 更新頻繁，應用去抖動")
            return
            
        self.wallet_update_times[wallet_address] = current_time
        self.wallet_update_counts[wallet_address] = 1
        
        async with self.lock:
            self.updated_wallets.add(wallet_address)
            
            # 檢查是否需要立即同步
            if len(self.updated_wallets) >= self.max_batch_size or current_time - self.last_sync_time >= self.sync_interval:
                logger.info(f"待同步錢包數量: {len(self.updated_wallets)}，立即觸發同步")
                
                sync_task = asyncio.create_task(self._sync_wallets())
                
                sync_task.add_done_callback(self._on_sync_task_done)
                
                logger.info(f"已創建同步任務 (ID: {id(sync_task)})，錢包 {wallet_address} 已添加到同步隊列")
                return
        
        logger.info(f"錢包 {wallet_address} 已成功添加到同步隊列")
    
    def _on_sync_task_done(self, task):
        """同步任務完成時的回調函數"""
        try:
            result = task.result()
            logger.info(f"同步任務 (ID: {id(task)}) 已完成")
        except asyncio.CancelledError:
            logger.warning(f"同步任務 (ID: {id(task)}) 被取消")
        except Exception as e:
            logger.exception(f"同步任務 (ID: {id(task)}) 發生錯誤: {e}")

    async def _sync_loop(self):
        """同步循環，帶計數器重置和每日全量同步"""
        reset_interval = 86400  # 每天重置一次計數器
        full_sync_interval = 86400  # 每天執行一次全量同步
        last_reset_time = time.time()
        last_full_sync_time = time.time() - 86000
        
        try:
            logger.info("錢包同步循環已啟動")
            while self.running:
                try:
                    current_time = time.time()
                    
                    # 檢查是否需要重置計數器
                    if current_time - last_reset_time >= reset_interval:
                        self.wallet_update_counts = {}
                        self.wallet_update_times = {}
                        last_reset_time = current_time
                        logger.info("已重置錢包更新計數器")
                    
                    # 檢查是否需要執行全量同步
                    if current_time - last_full_sync_time >= full_sync_interval:
                        logger.info("開始執行錢包資料全量同步...")
                        await self._sync_all_wallets()
                        last_full_sync_time = current_time
                        logger.info("錢包資料全量同步完成")
                    
                    # 檢查是否有待同步的錢包
                    async with self.lock:
                        pending_wallets = len(self.updated_wallets)
                    
                    if pending_wallets > 0:
                        logger.info(f"同步循環檢測到 {pending_wallets} 個待同步錢包")
                    
                    # 減少同步間隔
                    await asyncio.sleep(self.sync_interval)
                    
                    # 執行同步並捕獲任何錯誤
                    try:
                        await self._sync_wallets()
                    except Exception as e:
                        logger.exception(f"執行同步操作時發生錯誤: {e}")
                        
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.exception(f"錢包同步循環發生錯誤: {e}")
                    await asyncio.sleep(10)
        except asyncio.CancelledError:
            logger.info("錢包同步循環被取消")
            raise
    
    async def _sync_wallets(self):
        """同步錢包信息到外部 API"""
        async with self.lock:
            if not self.updated_wallets:
                logger.debug("沒有待同步的錢包，跳過同步")
                return
                    
            wallets_to_sync = list(self.updated_wallets)
            self.updated_wallets.clear()
            logger.info(f"從待同步集合中取出 {len(wallets_to_sync)} 個錢包地址")
        
        if not wallets_to_sync:
            return
                
        logger.info(f"開始同步 {len(wallets_to_sync)} 個錢包信息")
        self.last_sync_time = time.time()
        
        try:
            wallet_data = await self._get_wallet_data(wallets_to_sync)
            
            if not wallet_data:
                logger.warning("沒有找到需要同步的錢包信息")
                return
                    
            # logger.info(f"成功獲取錢包數據 {wallet_data}，準備推送到外部 API")
                
            success = await self._push_to_api(wallet_data)
            
            if success:
                logger.info(f"成功同步 {len(wallet_data)} 個錢包信息到外部 API")
            else:
                logger.error(f"同步錢包信息失敗，將重新嘗試")
                async with self.lock:
                    self.updated_wallets.update(wallets_to_sync)
                    logger.info(f"已將 {len(wallets_to_sync)} 個錢包地址重新加入待同步集合")
                
        except Exception as e:
            logger.exception(f"同步錢包信息時發生錯誤: {e}")
            async with self.lock:
                self.updated_wallets.update(wallets_to_sync)
                logger.info(f"發生錯誤，已將 {len(wallets_to_sync)} 個錢包地址重新加入待同步集合")
    
    async def _get_wallet_data(self, wallet_addresses: List[str]) -> List[Dict[str, Any]]:
        """獲取錢包信息並轉換為 API 所需格式"""
        result = []
        try:
            with self.session_factory() as session:
                wallet_summaries = session.execute(
                    select(WalletSummary).where(WalletSummary.wallet_address.in_(wallet_addresses))
                ).scalars().all()
                
                logger.info(f"從數據庫查詢到 {len(wallet_summaries)} 個錢包信息")
                
                for wallet in wallet_summaries:
                    try:
                        has_transactions = (
                            (wallet.total_transaction_num_30d or 0) > 0 or
                            (wallet.total_transaction_num_7d or 0) > 0 or
                            (wallet.total_transaction_num_1d or 0) > 0
                        )
                        
                        # 如果沒有交易記錄，跳過這個錢包
                        if not has_transactions:
                            logger.info(f"錢包 {wallet.wallet_address} 沒有交易記錄，跳過推送")
                            continue

                        # 創建新的字典，首先添加必填項
                        api_data = {
                            "address": wallet.wallet_address,
                            "chain": wallet.chain.upper() if wallet.chain else "SOLANA",
                            "last_transaction_time": wallet.last_transaction_time,
                            "isActive": wallet.is_active if wallet.is_active is not None else True,
                            "walletType": wallet.wallet_type if wallet.wallet_type is not None else 0
                        }
                        
                        # 添加餘額相關字段
                        if hasattr(wallet, "balance") and wallet.balance is not None:
                            api_data["balance"] = wallet.balance
                        
                        if hasattr(wallet, "balance_usd") and wallet.balance_usd is not None:
                            api_data["balanceUsd"] = wallet.balance_usd
                        
                        # 添加標籤和社交媒體信息
                        if hasattr(wallet, "tag") and wallet.tag:
                            api_data["tag"] = wallet.tag
                        
                        if hasattr(wallet, "twitter_name") and wallet.twitter_name:
                            api_data["twitterName"] = wallet.twitter_name
                        
                        if hasattr(wallet, "twitter_username") and wallet.twitter_username:
                            api_data["twitterUsername"] = wallet.twitter_username
                        
                        # 添加錢包類型信息
                        if hasattr(wallet, "is_smart_wallet") and wallet.is_smart_wallet is not None:
                            api_data["isSmartWallet"] = wallet.is_smart_wallet
                        
                        if hasattr(wallet, "asset_multiple") and wallet.asset_multiple is not None:
                            api_data["assetMultiple"] = wallet.asset_multiple
                        
                        if hasattr(wallet, "token_list") and wallet.token_list:
                            api_data["tokenList"] = wallet.token_list
                        
                        # 添加交易數據
                        if hasattr(wallet, "avg_cost_30d") and wallet.avg_cost_30d is not None:
                            api_data["avgCost30d"] = wallet.avg_cost_30d
                        
                        if hasattr(wallet, "avg_cost_7d") and wallet.avg_cost_7d is not None:
                            api_data["avgCost7d"] = wallet.avg_cost_7d
                        
                        if hasattr(wallet, "avg_cost_1d") and wallet.avg_cost_1d is not None:
                            api_data["avgCost1d"] = wallet.avg_cost_1d
                        
                        if hasattr(wallet, "total_transaction_num_30d") and wallet.total_transaction_num_30d is not None:
                            api_data["totalTransactionNum30d"] = wallet.total_transaction_num_30d
                        
                        if hasattr(wallet, "total_transaction_num_7d") and wallet.total_transaction_num_7d is not None:
                            api_data["totalTransactionNum7d"] = wallet.total_transaction_num_7d
                        
                        if hasattr(wallet, "total_transaction_num_1d") and wallet.total_transaction_num_1d is not None:
                            api_data["totalTransactionNum1d"] = wallet.total_transaction_num_1d
                        
                        if hasattr(wallet, "buy_num_30d") and wallet.buy_num_30d is not None:
                            api_data["buyNum30d"] = wallet.buy_num_30d
                        
                        if hasattr(wallet, "buy_num_7d") and wallet.buy_num_7d is not None:
                            api_data["buyNum7d"] = wallet.buy_num_7d
                        
                        if hasattr(wallet, "buy_num_1d") and wallet.buy_num_1d is not None:
                            api_data["buyNum1d"] = wallet.buy_num_1d
                        
                        if hasattr(wallet, "sell_num_30d") and wallet.sell_num_30d is not None:
                            api_data["sellNum30d"] = wallet.sell_num_30d
                        
                        if hasattr(wallet, "sell_num_7d") and wallet.sell_num_7d is not None:
                            api_data["sellNum7d"] = wallet.sell_num_7d
                        
                        if hasattr(wallet, "sell_num_1d") and wallet.sell_num_1d is not None:
                            api_data["sellNum1d"] = wallet.sell_num_1d
                        
                        if hasattr(wallet, "win_rate_30d") and wallet.win_rate_30d is not None:
                            api_data["winRate30d"] = wallet.win_rate_30d
                        
                        if hasattr(wallet, "win_rate_7d") and wallet.win_rate_7d is not None:
                            api_data["winRate7d"] = wallet.win_rate_7d
                        
                        if hasattr(wallet, "win_rate_1d") and wallet.win_rate_1d is not None:
                            api_data["winRate1d"] = wallet.win_rate_1d
                        
                        # 添加盈虧數據
                        if hasattr(wallet, "pnl_30d") and wallet.pnl_30d is not None:
                            api_data["pnl30d"] = wallet.pnl_30d
                        
                        if hasattr(wallet, "pnl_7d") and wallet.pnl_7d is not None:
                            api_data["pnl7d"] = wallet.pnl_7d
                        
                        if hasattr(wallet, "pnl_1d") and wallet.pnl_1d is not None:
                            api_data["pnl1d"] = wallet.pnl_1d
                        
                        if hasattr(wallet, "pnl_percentage_30d") and wallet.pnl_percentage_30d is not None:
                            api_data["pnlPercentage30d"] = wallet.pnl_percentage_30d
                        
                        if hasattr(wallet, "pnl_percentage_7d") and wallet.pnl_percentage_7d is not None:
                            api_data["pnlPercentage7d"] = wallet.pnl_percentage_7d
                        
                        if hasattr(wallet, "pnl_percentage_1d") and wallet.pnl_percentage_1d is not None:
                            api_data["pnlPercentage1d"] = wallet.pnl_percentage_1d
                        
                        if hasattr(wallet, "pnl_pic_30d") and wallet.pnl_pic_30d:
                            api_data["pnlPic30d"] = wallet.pnl_pic_30d
                        
                        if hasattr(wallet, "pnl_pic_7d") and wallet.pnl_pic_7d:
                            api_data["pnlPic7d"] = wallet.pnl_pic_7d
                        
                        if hasattr(wallet, "pnl_pic_1d") and wallet.pnl_pic_1d:
                            api_data["pnlPic1d"] = wallet.pnl_pic_1d
                        
                        if hasattr(wallet, "unrealized_profit_30d") and wallet.unrealized_profit_30d is not None:
                            api_data["unrealizedProfit30d"] = wallet.unrealized_profit_30d
                        
                        if hasattr(wallet, "unrealized_profit_7d") and wallet.unrealized_profit_7d is not None:
                            api_data["unrealizedProfit7d"] = wallet.unrealized_profit_7d
                        
                        if hasattr(wallet, "unrealized_profit_1d") and wallet.unrealized_profit_1d is not None:
                            api_data["unrealizedProfit1d"] = wallet.unrealized_profit_1d
                        
                        if hasattr(wallet, "total_cost_30d") and wallet.total_cost_30d is not None:
                            api_data["totalCost30d"] = wallet.total_cost_30d
                        
                        if hasattr(wallet, "total_cost_7d") and wallet.total_cost_7d is not None:
                            api_data["totalCost7d"] = wallet.total_cost_7d
                        
                        if hasattr(wallet, "total_cost_1d") and wallet.total_cost_1d is not None:
                            api_data["totalCost1d"] = wallet.total_cost_1d
                        
                        if hasattr(wallet, "avg_realized_profit_30d") and wallet.avg_realized_profit_30d is not None:
                            api_data["avgRealizedProfit30d"] = wallet.avg_realized_profit_30d
                        
                        if hasattr(wallet, "avg_realized_profit_7d") and wallet.avg_realized_profit_7d is not None:
                            api_data["avgRealizedProfit7d"] = wallet.avg_realized_profit_7d
                        
                        if hasattr(wallet, "avg_realized_profit_1d") and wallet.avg_realized_profit_1d is not None:
                            api_data["avgRealizedProfit1d"] = wallet.avg_realized_profit_1d
                        
                        # 添加收益分布數據
                        if hasattr(wallet, "distribution_gt500_30d") and wallet.distribution_gt500_30d is not None:
                            api_data["distribution_gt500_30d"] = wallet.distribution_gt500_30d
                        
                        if hasattr(wallet, "distribution_200to500_30d") and wallet.distribution_200to500_30d is not None:
                            api_data["distribution_200to500_30d"] = wallet.distribution_200to500_30d
                        
                        if hasattr(wallet, "distribution_0to200_30d") and wallet.distribution_0to200_30d is not None:
                            api_data["distribution_0to200_30d"] = wallet.distribution_0to200_30d
                        
                        if hasattr(wallet, "distribution_0to50_30d") and wallet.distribution_0to50_30d is not None:
                            api_data["distribution_0to50_30d"] = wallet.distribution_0to50_30d
                        
                        if hasattr(wallet, "distribution_lt50_30d") and wallet.distribution_lt50_30d is not None:
                            api_data["distribution_lt50_30d"] = wallet.distribution_lt50_30d
                        
                        if hasattr(wallet, "distribution_gt500_percentage_30d") and wallet.distribution_gt500_percentage_30d is not None:
                            api_data["distribution_gt500_percentage_30d"] = wallet.distribution_gt500_percentage_30d
                        
                        if hasattr(wallet, "distribution_200to500_percentage_30d") and wallet.distribution_200to500_percentage_30d is not None:
                            api_data["distribution_200to500_percentage_30d"] = wallet.distribution_200to500_percentage_30d
                        
                        if hasattr(wallet, "distribution_0to200_percentage_30d") and wallet.distribution_0to200_percentage_30d is not None:
                            api_data["distribution_0to200_percentage_30d"] = wallet.distribution_0to200_percentage_30d
                        
                        if hasattr(wallet, "distribution_0to50_percentage_30d") and wallet.distribution_0to50_percentage_30d is not None:
                            api_data["distribution_0to50_percentage_30d"] = wallet.distribution_0to50_percentage_30d
                        
                        if hasattr(wallet, "distribution_lt50_percentage_30d") and wallet.distribution_lt50_percentage_30d is not None:
                            api_data["distribution_lt50_percentage_30d"] = wallet.distribution_lt50_percentage_30d
                        
                        if hasattr(wallet, "distribution_gt500_7d") and wallet.distribution_gt500_7d is not None:
                            api_data["distribution_gt500_7d"] = wallet.distribution_gt500_7d
                        
                        if hasattr(wallet, "distribution_200to500_7d") and wallet.distribution_200to500_7d is not None:
                            api_data["distribution_200to500_7d"] = wallet.distribution_200to500_7d
                        
                        if hasattr(wallet, "distribution_0to200_7d") and wallet.distribution_0to200_7d is not None:
                            api_data["distribution_0to200_7d"] = wallet.distribution_0to200_7d
                        
                        if hasattr(wallet, "distribution_0to50_7d") and wallet.distribution_0to50_7d is not None:
                            api_data["distribution_0to50_7d"] = wallet.distribution_0to50_7d
                        
                        if hasattr(wallet, "distribution_lt50_7d") and wallet.distribution_lt50_7d is not None:
                            api_data["distribution_lt50_7d"] = wallet.distribution_lt50_7d
                        
                        if hasattr(wallet, "distribution_gt500_percentage_7d") and wallet.distribution_gt500_percentage_7d is not None:
                            api_data["distribution_gt500_percentage_7d"] = wallet.distribution_gt500_percentage_7d
                        
                        if hasattr(wallet, "distribution_200to500_percentage_7d") and wallet.distribution_200to500_percentage_7d is not None:
                            api_data["distribution_200to500_percentage_7d"] = wallet.distribution_200to500_percentage_7d
                        
                        if hasattr(wallet, "distribution_0to200_percentage_7d") and wallet.distribution_0to200_percentage_7d is not None:
                            api_data["distribution_0to200_percentage_7d"] = wallet.distribution_0to200_percentage_7d
                        
                        if hasattr(wallet, "distribution_0to50_percentage_7d") and wallet.distribution_0to50_percentage_7d is not None:
                            api_data["distribution_0to50_percentage_7d"] = wallet.distribution_0to50_percentage_7d
                        
                        if hasattr(wallet, "distribution_lt50_percentage_7d") and wallet.distribution_lt50_percentage_7d is not None:
                            api_data["distribution_lt50_percentage_7d"] = wallet.distribution_lt50_percentage_7d
                        
                        # 添加更新時間
                        # if hasattr(wallet, "update_time") and wallet.update_time is not None:
                        #     api_data["update_time"] = wallet.update_time.isoformat() if isinstance(wallet.update_time, (datetime, date)) else wallet.update_time
                        
                        # 添加到結果列表
                        result.append(api_data)
                        
                    except Exception as e:
                        logger.exception(f"處理錢包 {wallet.wallet_ddress} 數據時發生錯誤: {e}")
                        # 如果處理單個錢包數據失敗，仍然添加必填項
                        result.append({
                            "address": wallet.wallet_address,
                            "chain": wallet.chain.upper() if wallet.chain else "SOLANA",
                            "last_transaction_time": wallet.last_transaction_time,
                            "isActive": wallet.is_active if wallet.is_active is not None else True,
                            "walletType": wallet.wallet_type if wallet.wallet_type is not None else 0
                        })
                
                logger.info(f"已將 {len(result)} 個有交易記錄的錢包數據轉換為 API 所需格式")
                
                # 記錄樣本數據
                if result and len(result) > 0:
                    sample_data = {k: result[0][k] for k in ['address', 'chain', 'isActive', 'walletType'] if k in result[0]}
                    logger.info(f"樣本數據（部分字段）: {sample_data}")
                    
                return result
                    
        except Exception as e:
            logger.exception(f"獲取錢包信息時發生錯誤: {e}")
            return []
    
    async def _push_to_api(self, wallet_data: List[Dict[str, Any]]) -> bool:
        """推送錢包信息到外部 API"""
        if not self.api_endpoint:
            logger.error("未配置錢包同步 API 端點，無法推送數據")
            return False
                
        headers = {"Content-Type": "application/json"}
        logger.info(f"準備推送 {len(wallet_data)} 個錢包數據到 API: {self.api_endpoint}")
        
        try:
            import json
            logger.debug(f"請求數據: {json.dumps(wallet_data[:2])}")
        except Exception as e:
            logger.error(f"序列化請求數據時發生錯誤: {e}")
        
        if self.api_endpoint.startswith('https://') and ('127.0.0.1' in self.api_endpoint or 'localhost' in self.api_endpoint):
            self.api_endpoint = self.api_endpoint.replace('https://', 'http://')
            logger.info(f"檢測到本地服務器，已將 HTTPS 轉換為 HTTP: {self.api_endpoint}")
        
        for retry, delay in enumerate(self.retry_intervals):
            try:
                logger.info(f"嘗試推送數據到 API (嘗試 {retry+1}/{len(self.retry_intervals)})")
                
                connector = aiohttp.TCPConnector(ssl=False)
                
                timeout = aiohttp.ClientTimeout(total=120)
                
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    start_time = time.time()
                    logger.info(f"開始 API 請求，超時設置為 120 秒")
                    
                    async with session.post(
                        self.api_endpoint,
                        headers=headers,
                        json=wallet_data,
                    ) as response:
                        elapsed_time = time.time() - start_time
                        logger.info(f"API 請求完成，耗時 {elapsed_time:.2f} 秒，狀態碼: {response.status}")
                        
                        response_text = await response.text()
                        
                        if response.status == 200:
                            logger.info(f"成功推送數據到 API，響應: {response_text[:100]}...")
                            return True
                        else:
                            logger.error(f"API 請求失敗，狀態碼: {response.status}, 錯誤: {response_text}")
                            
                            if retry == len(self.retry_intervals) - 1:
                                return False
                                
                            logger.info(f"將在 {delay} 秒後重試 (嘗試 {retry+1}/{len(self.retry_intervals)})")
                            await asyncio.sleep(delay)
                            
            except asyncio.TimeoutError:
                logger.error(f"API 請求超時 (120秒)")
                if retry == len(self.retry_intervals) - 1:
                    return False
                logger.info(f"將在 {delay} 秒後重試 (嘗試 {retry+1}/{len(self.retry_intervals)})")
                await asyncio.sleep(delay)
                
            except aiohttp.client_exceptions.ClientConnectorError as e:
                logger.error(f"連接 API 失敗: {e}")
                
                # 檢查 API 端點格式
                if not self.api_endpoint.startswith(('http://', 'https://')):
                    self.api_endpoint = f"http://{self.api_endpoint}"
                    logger.info(f"已修復 API 端點格式: {self.api_endpoint}")
                    continue
                    
                if retry == len(self.retry_intervals) - 1:
                    return False
                logger.info(f"將在 {delay} 秒後重試 (嘗試 {retry+1}/{len(self.retry_intervals)})")
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.exception(f"推送到 API 時發生錯誤: {e}")
                if retry == len(self.retry_intervals) - 1:
                    return False
                logger.info(f"將在 {delay} 秒後重試 (嘗試 {retry+1}/{len(self.retry_intervals)})")
                await asyncio.sleep(delay)
                
        return False

    async def _sync_all_wallets(self):
        """同步所有活躍錢包到外部 API（每日全量同步）"""
        try:
            logger.info("開始全量同步所有錢包")
            
            with self.session_factory() as session:
                # 分批查詢所有活躍錢包
                batch_size = 50
                offset = 0
                total_synced = 0
                
                while True:
                    # 查詢一批錢包地址
                    wallets = session.execute(
                        select(WalletSummary.wallet_address)
                        .limit(batch_size)
                        .offset(offset)
                    ).scalars().all()
                    
                    if not wallets:
                        logger.info(f"所有批次已處理完畢，共同步 {total_synced} 個錢包")
                        break
                        
                    logger.info(f"全量同步：查詢到第 {offset}-{offset+len(wallets)} 批錢包，共 {len(wallets)} 個")
                    
                    # 將這批錢包地址轉換為列表
                    wallet_addresses = list(wallets)
                    
                    # 直接獲取這批錢包的數據並推送到 API
                    wallet_data = await self._get_wallet_data(wallet_addresses)
                    
                    if wallet_data:
                        logger.info(f"全量同步：處理第 {offset+1}-{offset+len(wallet_data)} 批，準備推送 {len(wallet_data)} 個錢包數據")
                        success = await self._push_to_api(wallet_data)
                        
                        if success:
                            logger.info(f"全量同步：成功推送第 {offset+1}-{offset+len(wallet_data)} 批錢包數據")
                            total_synced += len(wallet_data)
                        else:
                            logger.error(f"全量同步：推送第 {offset+1}-{offset+len(wallet_data)} 批錢包數據失敗")
                            # 失敗時將地址添加到待更新集合，以便後續重試
                            async with self.lock:
                                self.updated_wallets.update(wallet_addresses)
                    else:
                        logger.warning(f"全量同步：第 {offset+1}-{offset+len(wallets)} 批無有效錢包數據")
                    
                    # 增加偏移量
                    offset += batch_size
                    
                    # 簡短等待，避免數據庫和 API 服務器壓力
                    await asyncio.sleep(2)
                
            logger.info(f"錢包資料全量同步完成，共同步 {total_synced} 個錢包")
        except Exception as e:
            logger.exception(f"全量同步錢包時發生錯誤: {e}")

# 創建單例實例
wallet_sync_service = WalletSyncService()