from celery import Celery
from typing import List, Dict, Any, Optional
import asyncio
import logging
import time
import math
from app.core.config import settings
from app.services.wallet_analyzer import wallet_analyzer
from app.services.cache_service import cache_service
from celery.signals import worker_process_init, worker_process_shutdown
from sqlalchemy import update, func

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
    worker_concurrency=4  # 允许多个并发worker
)

# 设置全局事件循环
loop = None

@worker_process_init.connect
def init_worker(**kwargs):
    """Worker初始化时创建事件循环"""
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    logger.info("Worker已初始化事件循环")

@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """Worker关闭时清理事件循环"""
    global loop
    if loop:
        loop.close()
    logger.info("Worker已关闭事件循环")

# 异步任务包装器
def run_async_task(coro):
    """运行异步任务的包装函数"""
    global loop
    if loop is None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# 处理单个钱包的任务
@celery_app.task(name="process_single_wallet")
def process_single_wallet(
    address, 
    time_range=7, 
    include_metrics=None, 
    request_id=None, 
    chain="SOLANA",
    twitter_name=None,  # 新增 Twitter 名稱參數
    twitter_username=None  # 新增 Twitter 用戶名參數
):
    """处理单个钱包分析的Celery任务"""
    from app.services.wallet_analyzer import wallet_analyzer
    from app.services.cache_service import cache_service
    from app.models.models import WalletSummary
    from app.core.db import get_session_factory
    
    try:
        logger.info(f"开始处理钱包任务: {address}, 链: {chain}, 请求ID: {request_id}")
        start_time = time.time()
        
        # 運行異步分析函數
        result = run_async_task(wallet_analyzer.analyze_wallet(
            address, 
            time_range=time_range,
            include_metrics=include_metrics,
            chain=chain,
            twitter_name=twitter_name,
            twitter_username=twitter_username
        ))
        
        # 如果提供了 Twitter 資訊，更新錢包摘要表
        if twitter_name or twitter_username:
            session_factory = get_session_factory()
            with session_factory() as session:
                # 檢查是否已存在記錄
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
        
        # 更新缓存
        run_async_task(cache_service.set(f"wallet:{chain}:{address}", result, expiry=3600))
        
        # 如果有请求ID，更新请求状态
        if request_id:
            async def update_req_status():
                req_status = await cache_service.get(f"req:{request_id}")
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
                    await cache_service.set(f"req:{request_id}", req_status, expiry=3600)
            run_async_task(update_req_status())
        
        processing_time = time.time() - start_time
        logger.info(f"钱包 {address} 处理完成，耗时 {processing_time:.2f}秒")
        return {"status": "success", "address": address, "processing_time": processing_time}
        
    except Exception as e:
        logger.exception(f"处理钱包 {address} 时出错: {str(e)}")
        return {"status": "error", "address": address, "error": str(e)}
    finally:
        run_async_task(cache_service.delete(f"processing:{chain}:{address}"))

# 批量处理钱包的任务
@celery_app.task(name="process_wallet_batch", bind=True)
def process_wallet_batch(
    self, 
    request_id, 
    addresses, 
    time_range=7, 
    include_metrics=None, 
    batch_index=0, 
    chain="SOLANA",
    twitter_names=None,  # 改為複數形式
    twitter_usernames=None  # 改為複數形式
):
    """處理一批錢包地址的Celery任務"""
    start_time = time.time()
    total_wallets = len(addresses)
    logger.info(f"==== 開始處理批次 {batch_index}, 包含 {total_wallets} 個地址，請求ID: {request_id} ====")
    logger.info(f"處理鏈：{chain}")
    
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
    # 为每个地址创建单独的任务
    for i, address in enumerate(addresses):
        # 使用apply_async而不是delay，以便指定队列和其他参数
        task = process_single_wallet.apply_async(
            args=[address], 
            kwargs={
                "time_range": time_range,
                "include_metrics": include_metrics,
                "request_id": request_id,
                "chain": chain,
                "twitter_name": twitter_names[i],  # 使用對應索引的 Twitter 名稱
                "twitter_username": twitter_usernames[i]  # 使用對應索引的 Twitter 用戶名
            },
            queue="wallet_tasks"
        )
        results.append({"address": address, "task_id": task.id})
    
    end_time = time.time()
    total_time = end_time - start_time
    
    logger.info(f"==== 批次 {batch_index} 處理完成 ====")
    logger.info(f"總處理時間: {total_time:.2f} 秒")
    logger.info(f"處理錢包數: {total_wallets}")
    logger.info(f"平均每個錢包耗時: {total_time/max(1, total_wallets):.2f} 秒")
    logger.info(f"==============================")

    return {"batch_index": batch_index, "tasks": results}

@celery_app.task(name="remove_wallet_data_batch", bind=True)
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
                async def update_req_status():
                    # 获取当前请求状态
                    req_status = await cache_service.get(f"req:{request_id}")
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
                        await cache_service.set(f"req:{request_id}", req_status, expiry=3600)
                
                run_async_task(update_req_status())
                
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