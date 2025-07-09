from celery import Celery
from typing import List, Dict, Any, Optional
import asyncio
import logging
import time
import math
import os
from app.core.config import settings
from app.services.wallet_analyzer import wallet_analyzer
from app.services.cache_service import cache_service
from celery.signals import worker_process_init, worker_process_shutdown
from sqlalchemy import update, func

# 確保日誌目錄存在
os.makedirs("app/logs", exist_ok=True)

logger = logging.getLogger(__name__)

# 使用环境变量或配置，保持一致性
celery_app = Celery(
    "wallet_analyzer",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    task_track_started=True,
    task_time_limit=600,  # 设置任务超时时间（10分钟）
    worker_max_tasks_per_child=100,  # 每个worker处理完100个任务后重启
    worker_concurrency=4,  # 允许多个并发worker
    # 日誌配置
    worker_log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    worker_task_log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    worker_log_file='app/logs/worker.log',
    worker_log_level='INFO'
)

@worker_process_init.connect
def init_worker(**kwargs):
    """Worker初始化时创建事件循环和設置日誌"""
    # 設置環境變量，標識當前在 Celery worker 環境中運行
    import os
    os.environ['CELERY_WORKER_RUNNING'] = '1'
    
    # 簡單的日誌配置
    import logging
    import os
    
    # 確保日誌目錄存在
    os.makedirs("app/logs", exist_ok=True)
    
    # 配置根 logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('app/logs/worker.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    logger.info("Worker已初始化，日誌配置已設置")

@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """Worker关闭时清理事件循环"""
    logger.info("Worker已关闭")

# 异步任务包装器
def run_async_task(coro):
    """在 Celery task 中安全地執行 asyncio coroutine"""
    try:
        # 首先尝试获取当前事件循环
        try:
            loop = asyncio.get_running_loop()
            # 如果已经有运行中的循环，创建一个新任务
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        except RuntimeError:
            # 没有运行中的循环，直接使用asyncio.run
            return asyncio.run(coro)
    except RuntimeError as e:
        if "event loop is closed" in str(e) or "attached to a different loop" in str(e):
            # 创建新的事件循环
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(coro)
                finally:
                    loop.close()
            except Exception as nested_e:
                logger.error(f"创建新事件循环失败: {str(nested_e)}")
                # 最后的回退方案：使用线程池
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, coro)
                    return future.result()
        else:
            raise

# 处理单个钱包的函数（非Celery任务版本）
def _process_single_wallet_internal(
    address, 
    time_range=7, 
    include_metrics=None, 
    request_id=None, 
    chain="SOLANA",
    twitter_name=None,  # 新增 Twitter 名稱參數
    twitter_username=None,  # 新增 Twitter 用戶名參數
    is_smart_wallet=False
):
    """处理单个钱包分析的内部函数（非Celery任务）"""
    from app.services.wallet_analyzer import wallet_analyzer
    from app.services.cache_service import cache_service
    from app.models.models import WalletSummary
    from app.core.db import get_session_factory
    
    try:
        logger.info(f"开始处理钱包任务: {address}, 链: {chain}, 请求ID: {request_id}, is_smart_wallet: {is_smart_wallet}")
        start_time = time.time()
        
        # 運行異步分析函數
        result = run_async_task(wallet_analyzer.analyze_wallet(
            address, 
            time_range=time_range,
            include_metrics=include_metrics,
            chain=chain,
            twitter_name=twitter_name,
            twitter_username=twitter_username,
            is_smart_wallet=is_smart_wallet
        ))
        
        # 如果提供了 Twitter 資訊或 is_smart_wallet 標記，更新錢包摘要表
        if twitter_name or twitter_username or is_smart_wallet:
            session_factory = get_session_factory()
            with session_factory() as session:
                # 檢查是否已存在記錄
                stmt = update(WalletSummary).where(
                    WalletSummary.wallet_address == address,
                    WalletSummary.chain == chain
                ).values(
                    twitter_name=twitter_name,
                    twitter_username=twitter_username,
                    is_smart_wallet=is_smart_wallet,
                    is_active=True
                )
                session.execute(stmt)
                session.commit()
                logger.info(f"已更新錢包 {address} 的資訊 (Twitter: {twitter_name}, is_smart_wallet: {is_smart_wallet})")
        
        # 更新缓存
        run_async_task(cache_service.set(f"wallet:{chain}:{address}", result, expiry=3600))
        
        # 如果有请求ID，更新请求状态
        if request_id:
            def update_req_status():
                req_status = run_async_task(cache_service.get(f"req:{request_id}"))
                if req_status:
                    if address in req_status.get("pending_addresses", []):
                        req_status["pending_addresses"].remove(address)
                    if "ready_results" not in req_status:
                        req_status["ready_results"] = {}
                    req_status["ready_results"][address] = {
                        "address": address,
                        "metrics": result,
                        "last_updated": int(time.time())
                    }
                    run_async_task(cache_service.set(f"req:{request_id}", req_status, expiry=3600))
            update_req_status()
        
        processing_time = time.time() - start_time
        logger.info(f"钱包 {address} 处理完成，耗时 {processing_time:.2f}秒")
        return {"status": "success", "address": address, "processing_time": processing_time}
        
    except Exception as e:
        logger.exception(f"处理钱包 {address} 时出错: {str(e)}")
        return {"status": "error", "address": address, "error": str(e)}
    finally:
        run_async_task(cache_service.delete(f"processing:{chain}:{address}"))

# 处理单个钱包的任务
@celery_app.task(name="app.workers.tasks.process_single_wallet")
def process_single_wallet(
    address, 
    time_range=7, 
    include_metrics=None, 
    request_id=None, 
    chain="SOLANA",
    twitter_name=None,  # 新增 Twitter 名稱參數
    twitter_username=None,  # 新增 Twitter 用戶名參數
    is_smart_wallet=False
):
    """处理单个钱包分析的Celery任务"""
    return _process_single_wallet_internal(
        address=address,
        time_range=time_range,
        include_metrics=include_metrics,
        request_id=request_id,
        chain=chain,
        twitter_name=twitter_name,
        twitter_username=twitter_username,
        is_smart_wallet=is_smart_wallet
    )

# 批量处理钱包的任务
@celery_app.task(name="app.workers.tasks.process_wallet_batch", bind=True)
def process_wallet_batch(
    self, 
    request_id, 
    addresses, 
    time_range=7, 
    include_metrics=None, 
    batch_index=0, 
    chain="SOLANA",
    twitter_names=None,
    twitter_usernames=None,
    is_smart_wallet=False
):
    """處理一批錢包地址的Celery任務"""
    start_time = time.time()
    total_wallets = len(addresses)
    logger.info(f"==== 開始處理批次 {batch_index}, 包含 {total_wallets} 個地址，請求ID: {request_id} ====")
    
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
    
    results = []
    # 直接處理每個地址，而不是創建新的任務
    for i, address in enumerate(addresses):
        try:
            # 直接調用內部函數，而不是創建 Celery 任務
            result = _process_single_wallet_internal(
                address=address,
                time_range=time_range,
                include_metrics=include_metrics,
                request_id=request_id,
                chain=chain,
                twitter_name=twitter_names[i],
                twitter_username=twitter_usernames[i],
                is_smart_wallet=is_smart_wallet
            )
            results.append({"address": address, "status": "success", "result": result})
        except Exception as e:
            logger.exception(f"處理錢包 {address} 時出錯: {str(e)}")
            results.append({"address": address, "status": "error", "error": str(e)})
    
    end_time = time.time()
    total_time = end_time - start_time
    
    logger.info(f"==== 批次 {batch_index} 處理完成 ====")
    logger.info(f"總處理時間: {total_time:.2f} 秒")
    logger.info(f"處理錢包數: {total_wallets}")
    logger.info(f"平均每個錢包耗時: {total_time/max(1, total_wallets):.2f} 秒")
    logger.info(f"==============================")

    return {"batch_index": batch_index, "results": results}

@celery_app.task(name="app.workers.tasks.remove_wallet_data_batch", bind=True)
def remove_wallet_data_batch(self, request_id, addresses, batch_index=0, chain="SOLANA"):
    """处理一批钱包地址的删除任务"""
    from sqlalchemy import delete
    from app.models.models import WalletSummary, Holding, Transaction, TokenBuyData
    from app.core.db import get_session_factory
    from app.services.cache_service import cache_service
    
    start_time = time.time()
    total_wallets = len(addresses)
    logger.info(f"==== 開始處理批次 {batch_index} 的刪除任務, 包含 {total_wallets} 個地址，請求ID: {request_id} ====")
    
    # 确保链名称是大写的
    chain = chain.upper()
    
    # 获取数据库会话
    session_factory = get_session_factory()
    
    results = []
    for address in addresses:
        try:
            with session_factory() as session:
                # 删除 TokenBuyData 记录
                token_buy_deleted = session.execute(
                    delete(TokenBuyData).where(
                        TokenBuyData.wallet_address == address
                    )
                ).rowcount
                
                # 删除 Transaction 记录
                transaction_deleted = session.execute(
                    delete(Transaction).where(
                        Transaction.wallet_address == address,
                        Transaction.chain == chain
                    )
                ).rowcount
                
                # 删除 Holding 记录
                holding_deleted = session.execute(
                    delete(Holding).where(
                        Holding.wallet_address == address,
                        Holding.chain == chain
                    )
                ).rowcount
                
                # 删除 WalletSummary 记录
                wallet_summary_deleted = session.execute(
                    delete(WalletSummary).where(
                        WalletSummary.address == address,
                        WalletSummary.chain == chain
                    )
                ).rowcount
                
                # 提交事务
                session.commit()
                
                # 删除缓存
                run_async_task(cache_service.delete(f"wallet:{chain}:{address}"))
                run_async_task(cache_service.delete(f"processing:{chain}:{address}"))
                
                logger.info(f"已删除钱包 {address} 的数据: WalletSummary={wallet_summary_deleted}, Holding={holding_deleted}, Transaction={transaction_deleted}, TokenBuyData={token_buy_deleted}")
                
                # 更新请求状态
                def update_req_status():
                    req_status = run_async_task(cache_service.get(f"req:{request_id}"))
                    if req_status:
                        # 更新状态
                        if address in req_status.get("pending_addresses", []):
                            req_status["pending_addresses"].remove(address)
                        
                        # 确保ready_results存在
                        if "ready_results" not in req_status:
                            req_status["ready_results"] = {}
                        
                        # 添加结果
                        req_status["ready_results"][address] = {
                            "address": address,
                            "status": "removed",
                            "last_updated": int(time.time())
                        }
                        
                        # 保存更新后的状态
                        run_async_task(cache_service.set(f"req:{request_id}", req_status, expiry=3600))
                
                update_req_status()
                
                results.append({
                    "address": address, 
                    "status": "success",
                    "deleted_records": {
                        "wallet_summary": wallet_summary_deleted,
                        "holding": holding_deleted,
                        "transaction": transaction_deleted,
                        "token_buy_data": token_buy_deleted
                    }
                })
                
        except Exception as e:
            logger.exception(f"删除钱包 {address} 数据时出错: {str(e)}")
            results.append({"address": address, "status": "error", "error": str(e)})
    
    end_time = time.time()
    total_time = end_time - start_time
    
    logger.info(f"==== 批次 {batch_index} 刪除處理完成 ====")
    logger.info(f"總處理時間: {total_time:.2f} 秒")
    logger.info(f"處理錢包數: {total_wallets}")
    logger.info(f"平均每個錢包耗時: {total_time/max(1, total_wallets):.2f} 秒")
    logger.info(f"==============================")
    
    return {"batch_index": batch_index, "results": results}

@celery_app.task
def clean_expired_cache():
    """
    清理過期的快取數據
    """
    try:
        # 獲取所有請求狀態鍵
        request_keys = run_async_task(cache_service.keys_pattern("req:*"))
        
        for key in request_keys:
            req_status = run_async_task(cache_service.get(key))
            if req_status:
                # 檢查請求是否已經完成或過期
                if "start_time" in req_status:
                    start_time = req_status["start_time"]
                    # 如果請求開始時間超過2小時，且仍有待處理地址，則清理
                    if (time.time() - start_time) > 7200 and req_status.get("pending_addresses"):
                        logger.info(f"Cleaning expired request {key}")
                        run_async_task(cache_service.delete(key))
    except Exception as e:
        logger.exception(f"Error in clean_expired_cache: {str(e)}")

# 定期任務
@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # 每小時清理過期快取
    sender.add_periodic_task(3600.0, clean_expired_cache.s(), name='clean expired cache')

@celery_app.task
def test_redis_connection(x):
    print(f"Test task received: {x}")