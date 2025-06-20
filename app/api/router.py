import os
import uuid
import time
import json
import asyncio
import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query, Request
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from app.services.wallet_analyzer import WalletAnalyzer
from app.services.cache_service import CacheService
from app.workers.tasks import process_wallet_batch
from app.core.config import settings
from app.services.solscan import solscan_client
# from app.services.kafka_consumer import kafka_consumer
from decimal import Decimal
from datetime import datetime, timezone
import httpx
import dotenv

logger = logging.getLogger(__name__)
# logger.setLevel(logging.WARNING)

dotenv.load_dotenv()

router = APIRouter(prefix="/wallets")
cache_service = CacheService()
wallet_analyzer = WalletAnalyzer()

SMARTMONEY_BSC = os.getenv("SMARTMONEY_BSC", "http://127.0.0.1:5000/robots/smartmoney/webhook/update-addresses/BSC")

# 請求與響應模型
class WalletAnalysisRequest(BaseModel):
    addresses: List[str]
    include_metrics: Optional[List[str]] = None
    time_range: Optional[int] = 7
    chain: str
    type: str = "add"
    twitter_names: Optional[List[str]] = None  # 修改為 twitter_names
    twitter_usernames: Optional[List[str]] = None  # 修改為 twitter_usernames

class WalletMetrics(BaseModel):
    address: str
    metrics: Dict[str, Any]
    last_updated: int

class WalletBatchResponse(BaseModel):
    request_id: str
    ready_results: Dict[str, WalletMetrics]
    pending_addresses: List[str]

class BatchResultResponse(BaseModel):
    request_id: str
    status: str
    progress: float
    ready_results: Dict[str, WalletMetrics]
    pending_addresses: List[str]

# 新增請求模型
class TokenAnalysisRequest(BaseModel):
    wallet_address: str
    token_address: str
    time_range: Optional[int] = 30  # 默認30天
    chain: str

class TokenAnalysisResponse(BaseModel):
    wallet_address: str
    token_address: str
    holding_amount: str
    total_buy_value: str
    total_buy_amount: str
    total_buy_count: str
    total_sell_value: str
    total_sell_amount: str
    total_sell_count: str
    realized_pnl: str
    total_holding_seconds: int

@router.post("/analyze", response_model=WalletBatchResponse)
async def analyze_wallets(
    request: WalletAnalysisRequest,
    background_tasks: BackgroundTasks
):
    """批量錢包分析端點 - 立即返回结果与任务ID，使用Celery进行后台处理"""
    logger = logging.getLogger(__name__)
    logger.info(f"接收到批量分析請求：{len(request.addresses)} 個地址，鏈：{request.chain}，操作類型：{request.type}")
    
    # 验证 chain 参数
    valid_chains = ["SOLANA", "BSC", "BASE", "ETH", "TRON"]
    chain = request.chain.upper()
    if chain not in valid_chains:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的區塊鏈類型。支持的類型為: {', '.join(valid_chains)}"
        )
    
    # 验证 type 参数
    valid_types = ["add", "remove"]
    if request.type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的操作類型。支持的類型為: {', '.join(valid_types)}"
        )
    
    # 限制單次請求的最大地址數量
    max_addresses = getattr(settings, "MAX_ADDRESSES_PER_REQUEST", 300)
    if len(request.addresses) > max_addresses:
        raise HTTPException(
            status_code=400, 
            detail=f"地址數量超過限制。最大允許 {max_addresses} 個地址，請求包含 {len(request.addresses)} 個地址"
        )
    
    # 生成唯一請求 ID
    request_id = str(uuid.uuid4())
    logger.info(f"生成請求 ID: {request_id}")
    
    # 檢查重複地址並移除
    unique_addresses = list(set(request.addresses))
    if len(unique_addresses) < len(request.addresses):
        logger.info(f"已移除 {len(request.addresses) - len(unique_addresses)} 個重複地址")
    
    # 如果是 remove 操作，执行删除逻辑
    if request.type == "remove":
        logger.info(f"執行移除操作，將刪除 {len(unique_addresses)} 個錢包地址的數據")
        
        # 创建后台任务执行删除操作
        async def remove_wallet_data():
            try:
                from app.workers.tasks import remove_wallet_data_batch
                
                # 分批删除，避免一次性操作过多数据
                batch_size = 20
                address_groups = [unique_addresses[i:i+batch_size] 
                                for i in range(0, len(unique_addresses), batch_size)]
                
                for batch_idx, address_batch in enumerate(address_groups):
                    logger.info(f"提交第 {batch_idx+1}/{len(address_groups)} 批刪除任務 ({len(address_batch)} 個地址)")
                    
                    remove_wallet_data_batch.delay(
                        addresses=address_batch,
                        chain=chain,
                        request_id=request_id
                    )
                    
                    if batch_idx < len(address_groups) - 1:
                        await asyncio.sleep(0.1)
                
                logger.info(f"所有 {len(unique_addresses)} 個地址的刪除任務已提交")
            except Exception as e:
                logger.exception(f"提交刪除任務失敗: {str(e)}")
        
        background_tasks.add_task(remove_wallet_data)
        
        req_status = {
            "total_addresses": len(unique_addresses),
            "pending_addresses": unique_addresses,
            "ready_results": {},
            "start_time": int(time.time()),
            "operation_type": "remove",
            "chain": chain
        }
        
        # 保存请求状态到缓存
        await cache_service.set(f"req:{request_id}", req_status, expiry=3600)
        
        # 立即返回结果
        return WalletBatchResponse(
            request_id=request_id,
            ready_results={},
            pending_addresses=unique_addresses
        )
    
    # 以下是原有的 add 操作逻辑
    # 确保processing集合存在
    if not hasattr(wallet_analyzer, '_processing_addresses'):
        wallet_analyzer._processing_addresses = set()
    if not hasattr(wallet_analyzer, '_processing_lock'):
        wallet_analyzer._processing_lock = asyncio.Lock()
    
    cached_results = {}
    in_progress_addresses = []
    addresses_to_analyze = []
    
    cache_keys = [f"wallet:{chain}:{addr}" for addr in unique_addresses]
    
    try:
        batch_cache = await cache_service.get_multi(cache_keys)
        
        # 分類每個地址
        async with wallet_analyzer._processing_lock:
            for address in unique_addresses:
                cache_key = f"wallet:{chain}:{address}"
                is_processing = await cache_service.get(f"processing:{chain}:{address}")  # 添加链信息
                if cache_key in batch_cache and batch_cache[cache_key]:
                    # 已緩存結果
                    logger.debug(f"找到 {address} 在 {chain} 鏈上的緩存結果")
                    cached_results[address] = WalletMetrics(
                        address=address,
                        metrics=batch_cache[cache_key],
                        last_updated=int(time.time())
                    )
                elif is_processing:
                    # 正在處理中
                    logger.debug(f"地址 {address} 在 {chain} 鏈上已在處理中")
                    in_progress_addresses.append(address)
                else:
                    # 設置處理中標記（帶過期時間，防止死鎖）
                    await cache_service.set(f"processing:{chain}:{address}", "1", expiry=1800)
                    addresses_to_analyze.append(address)
        
        logger.info(f"地址分類: {len(cached_results)} 個緩存, {len(in_progress_addresses)} 個處理中, {len(addresses_to_analyze)} 個需分析")
    
    except Exception as e:
        logger.exception(f"檢查緩存時發生錯誤: {str(e)}")
        # 保持安全，回退到简单处理
        cached_results = {}
        all_pending = unique_addresses
    
    # 将缓存结果转换为可序列化格式
    ready_results_serializable = {}
    for addr, metrics in cached_results.items():
        ready_results_serializable[addr] = {
            "address": metrics.address,
            "metrics": metrics.metrics,
            "last_updated": metrics.last_updated
        }
    
    # 将所有待处理地址汇总
    all_pending = in_progress_addresses + addresses_to_analyze
    
    # 準備請求狀態
    req_status = {
        "total_addresses": len(unique_addresses),
        "pending_addresses": all_pending,
        "ready_results": ready_results_serializable,
        "start_time": int(time.time()),
        "operation_type": "add",
        "chain": chain
    }
    
    # 保存請求狀態到緩存
    await cache_service.set(f"req:{request_id}", req_status, expiry=3600)  # 1小時過期
    
    # 如果有地址需要分析，使用Celery处理
    if addresses_to_analyze:
        # 优化：决定如何分批
        batch_size = 10  # 默认批次大小
        if len(addresses_to_analyze) > 100:
            batch_size = 20
        elif len(addresses_to_analyze) <= 20:
            batch_size = 5
        
        # 分批创建Celery任务
        address_groups = [addresses_to_analyze[i:i+batch_size] 
                         for i in range(0, len(addresses_to_analyze), batch_size)]
        
        logger.info(f"创建 {len(address_groups)} 个Celery批处理任务")
        
        # 异步提交Celery任务
        async def submit_celery_tasks():
            try:
                from app.workers.tasks import process_wallet_batch
                
                for batch_idx, address_batch in enumerate(address_groups):
                    logger.info(f"提交第 {batch_idx+1}/{len(address_groups)} 批 ({len(address_batch)} 个地址)")
                    
                    # 提交批处理任务到Celery
                    process_wallet_batch.delay(
                        request_id=request_id,
                        addresses=address_batch,
                        time_range=request.time_range,
                        include_metrics=request.include_metrics,
                        batch_index=batch_idx,
                        chain=chain  # 添加链信息
                    )
                    
                    # 简短等待，避免一次提交过多任务
                    if batch_idx < len(address_groups) - 1:
                        await asyncio.sleep(0.1)
                
                logger.info(f"所有 {len(addresses_to_analyze)} 个地址已提交到Celery")
            except Exception as e:
                logger.exception(f"提交Celery任务失败: {str(e)}")
                # 出错时清理处理中标记
                async with wallet_analyzer._processing_lock:
                    for addr in addresses_to_analyze:
                        await cache_service.delete(f"processing:{chain}:{addr}")  # 使用 delete 方法清理标记
        
        # 添加背景任务以提交Celery任务
        background_tasks.add_task(submit_celery_tasks)
    
    # 立即返回結果
    return WalletBatchResponse(
        request_id=request_id,
        ready_results=cached_results,
        pending_addresses=all_pending
    )

async def update_request_status(request_id: str, address: str, result: dict):
    """更新請求狀態的輔助函數"""
    logger = logging.getLogger(__name__)
    try:
        req_status = await cache_service.get(f"req:{request_id}")
        if req_status:
            if address in req_status["pending_addresses"]:
                req_status["pending_addresses"].remove(address)
            if "ready_results" not in req_status:
                req_status["ready_results"] = {}
            req_status["ready_results"][address] = {
                "address": address,
                "metrics": result,
                "last_updated": int(time.time())
            }
            await cache_service.set(f"req:{request_id}", req_status, expiry=3600)
            logger.info(f"已更新 {address} 的請求狀態")
    except Exception as e:
        logger.error(f"更新請求狀態時發生錯誤: {str(e)}")

# 檢查批量分析狀態端點
@router.get("/analyze-wallets/{request_id}", response_model=BatchResultResponse)
async def check_analysis_status(
    request_id: str
):
    """
    檢查批量分析狀態端點
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Checking status for request: {request_id}")
    
    # 從快取獲取請求狀態
    req_status = await cache_service.get(f"req:{request_id}")
    
    if not req_status:
        logger.warning(f"Request ID {request_id} not found in cache")
        raise HTTPException(status_code=404, detail=f"Request ID {request_id} not found")
    
    logger.info(f"Found request status: {len(req_status.get('pending_addresses', []))} pending, {len(req_status.get('ready_results', {}))} ready")
    
    # 計算完成進度
    total = len(req_status["pending_addresses"]) + len(req_status["ready_results"])
    completed = len(req_status["ready_results"])
    progress = completed / total if total > 0 else 1.0
    
    # 確定狀態
    status = "completed" if progress >= 1.0 else "processing"
    logger.info(f"Status: {status}, Progress: {progress:.2f}")
    
    # 獲取最新結果
    updated_results = {}
    pending_addresses = req_status["pending_addresses"].copy()
    new_pending = []
    
    for address in pending_addresses:
        try:
            result = await cache_service.get(f"wallet:{address}")
            if result:
                logger.info(f"Found new result for {address}")
                updated_results[address] = WalletMetrics(
                    address=address,
                    metrics=result,
                    last_updated=int(time.time())
                )
            else:
                logger.info(f"No result yet for {address}")
                new_pending.append(address)
        except Exception as e:
            logger.error(f"Error checking result for {address}: {str(e)}")
            new_pending.append(address)
    
    # 合併已有結果
    for addr, data in req_status["ready_results"].items():
        if addr not in updated_results:
            updated_results[addr] = data
    
    logger.info(f"Returning response with {len(updated_results)} results and {len(new_pending)} pending addresses")
    return BatchResultResponse(
        request_id=request_id,
        status=status,
        progress=progress,
        ready_results=updated_results,
        pending_addresses=new_pending
    )

# 獲取單個錢包分析
@router.get("/wallet/{address}")
async def get_wallet_analysis(
    address: str,
    force_refresh: bool = False,
    time_range: int = 7
):
    """
    獲取單個錢包分析
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Received request for wallet analysis: {address}")
    
    try:
        # 檢查快取
        if not force_refresh:
            cached = await cache_service.get(f"wallet:{address}")
            if cached:
                logger.info(f"Returning cached result for {address}")
                return {
                    "address": address,
                    "metrics": cached,
                    "from_cache": True,
                    "last_updated": cached.get("last_updated", int(time.time()))
                }
        
        # 直接執行分析
        logger.info(f"Starting analysis for {address}")
        result = await wallet_analyzer.analyze_wallet(address, time_range=time_range)
        logger.info(f"Analysis completed for {address}")
        
        # 更新快取
        await cache_service.set(f"wallet:{address}", result, expiry=3600)
        
        return {
            "address": address,
            "metrics": result,
            "from_cache": False,
            "last_updated": int(time.time())
        }
    except Exception as e:
        logger.exception(f"Error analyzing wallet {address}: {str(e)}")
        # 返回基本的錯誤回應
        return {
            "address": address,
            "error": str(e),
            "status": "error",
            "metrics": {
                "is_mock_data": True,
                "total_transactions": 0,
                "message": "Failed to analyze wallet"
            },
            "last_updated": int(time.time())
        }

@router.post("/analyze-kol-wallets", response_model=WalletBatchResponse)
async def analyze_kol_wallets(
    request: WalletAnalysisRequest,
    background_tasks: BackgroundTasks
):
    """批量錢包分析端點 - 立即返回结果与任务ID，使用Celery进行后台处理"""
    logger.info(f"接收到批量kol錢包分析請求：{len(request.addresses)} 個地址，鏈：{request.chain}，操作類型：{request.type}")
    
    # 验证 chain 参数
    valid_chains = ["SOLANA", "BSC", "BASE", "ETH", "TRON"]
    chain = request.chain.upper()
    twitter_names = request.twitter_names if request.twitter_names else [None] * len(request.addresses)
    twitter_usernames = request.twitter_usernames if request.twitter_usernames else [None] * len(request.addresses)
    
    # 驗證 twitter 資料長度是否匹配
    if len(twitter_names) != len(request.addresses):
        raise HTTPException(
            status_code=400,
            detail=f"Twitter 名稱列表長度 ({len(twitter_names)}) 與地址列表長度 ({len(request.addresses)}) 不匹配"
        )
    if len(twitter_usernames) != len(request.addresses):
        raise HTTPException(
            status_code=400,
            detail=f"Twitter 用戶名列表長度 ({len(twitter_usernames)}) 與地址列表長度 ({len(request.addresses)}) 不匹配"
        )
    
    if chain not in valid_chains:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的區塊鏈類型。支持的類型為: {', '.join(valid_chains)}"
        )
    
    # 验证 type 参数
    valid_types = ["add", "remove"]
    if request.type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的操作類型。支持的類型為: {', '.join(valid_types)}"
        )
    
    # 限制單次請求的最大地址數量
    max_addresses = getattr(settings, "MAX_ADDRESSES_PER_REQUEST", 300)
    if len(request.addresses) > max_addresses:
        raise HTTPException(
            status_code=400, 
            detail=f"地址數量超過限制。最大允許 {max_addresses} 個地址，請求包含 {len(request.addresses)} 個地址"
        )
    
    # 生成唯一請求 ID
    request_id = str(uuid.uuid4())
    logger.info(f"生成請求 ID: {request_id}")
    
    # BSC 轉發邏輯
    if chain == "BSC" and request.type == "add":
        logger.info(f"開始BSC轉發邏輯，目標URL: {SMARTMONEY_BSC}")
        logger.info(f"請求參數: chain={chain}, type={request.type}, addresses_count={len(request.addresses)}")
        
        payload = {
            "chain": chain,
            "type": request.type,
            "addresses": request.addresses,
            "twitter_names": request.twitter_names if request.twitter_names else [None] * len(request.addresses),
            "twitter_usernames": request.twitter_usernames if request.twitter_usernames else [None] * len(request.addresses)
        }
        
        logger.info(f"準備發送的payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        async with httpx.AsyncClient() as client:
            try:
                logger.info(f"開始發送HTTP請求到 {SMARTMONEY_BSC}")
                logger.info(f"請求方法: POST, 超時時間: 30秒")
                
                resp = await client.post(SMARTMONEY_BSC, json=payload, timeout=30)
                
                logger.info(f"HTTP請求完成，狀態碼: {resp.status_code}")
                logger.info(f"響應頭: {dict(resp.headers)}")
                
                # 嘗試記錄響應內容
                try:
                    response_text = resp.text
                    logger.info(f"響應內容長度: {len(response_text)} 字符")
                    if len(response_text) < 1000:  # 只記錄較短的響應內容
                        logger.info(f"響應內容: {response_text}")
                    else:
                        logger.info(f"響應內容前500字符: {response_text[:500]}")
                except Exception as e:
                    logger.warning(f"無法讀取響應內容: {str(e)}")
                
                resp.raise_for_status()
                logger.info("BSC webhook請求成功")
                
                # 直接返回 webhook 的 json 結果
                return JSONResponse(status_code=resp.status_code, content=resp.json())
                
            except httpx.HTTPStatusError as e:
                logger.error(f"BSC webhook HTTP錯誤: 狀態碼={e.response.status_code}")
                logger.error(f"錯誤響應內容: {e.response.text}")
                logger.error(f"請求URL: {SMARTMONEY_BSC}")
                logger.error(f"請求payload: {json.dumps(payload, ensure_ascii=False)}")
                raise HTTPException(status_code=e.response.status_code, detail=f"BSC webhook error: {e.response.text}")
                
            except httpx.TimeoutException as e:
                logger.error(f"BSC webhook請求超時: {str(e)}")
                logger.error(f"請求URL: {SMARTMONEY_BSC}")
                logger.error(f"超時設置: 30秒")
                raise HTTPException(status_code=408, detail=f"BSC webhook timeout: {str(e)}")
                
            except httpx.ConnectError as e:
                logger.error(f"BSC webhook連接錯誤: {str(e)}")
                logger.error(f"請求URL: {SMARTMONEY_BSC}")
                raise HTTPException(status_code=503, detail=f"BSC webhook connection error: {str(e)}")
                
            except Exception as e:
                logger.error(f"BSC webhook未知異常: {str(e)}")
                logger.error(f"異常類型: {type(e).__name__}")
                logger.error(f"請求URL: {SMARTMONEY_BSC}")
                logger.error(f"請求payload: {json.dumps(payload, ensure_ascii=False)}")
                raise HTTPException(status_code=500, detail=f"BSC webhook exception: {str(e)}")

    # 檢查重複地址並移除
    unique_addresses = list(set(request.addresses))
    if len(unique_addresses) < len(request.addresses):
        logger.info(f"已移除 {len(request.addresses) - len(unique_addresses)} 個重複地址")
    
    # 如果是 remove 操作，执行删除逻辑
    if request.type == "remove":
        logger.info(f"執行移除操作，將刪除 {len(unique_addresses)} 個錢包地址的數據")
        
        # 创建后台任务执行删除操作
        async def remove_wallet_data():
            try:
                from app.workers.tasks import remove_wallet_data_batch
                
                # 分批删除，避免一次性操作过多数据
                batch_size = 20
                address_groups = [unique_addresses[i:i+batch_size] 
                                for i in range(0, len(unique_addresses), batch_size)]
                
                for batch_idx, address_batch in enumerate(address_groups):
                    logger.info(f"提交第 {batch_idx+1}/{len(address_groups)} 批刪除任務 ({len(address_batch)} 個地址)")
                    
                    remove_wallet_data_batch.delay(
                        addresses=address_batch,
                        chain=chain,
                        request_id=request_id
                    )
                    
                    if batch_idx < len(address_groups) - 1:
                        await asyncio.sleep(0.1)
                
                logger.info(f"所有 {len(unique_addresses)} 個地址的刪除任務已提交")
            except Exception as e:
                logger.exception(f"提交刪除任務失敗: {str(e)}")
        
        background_tasks.add_task(remove_wallet_data)
        
        req_status = {
            "total_addresses": len(unique_addresses),
            "pending_addresses": unique_addresses,
            "ready_results": {},
            "start_time": int(time.time()),
            "operation_type": "remove",
            "chain": chain
        }
        
        # 保存请求状态到缓存
        await cache_service.set(f"req:{request_id}", req_status, expiry=3600)
        
        # 立即返回结果
        return WalletBatchResponse(
            request_id=request_id,
            ready_results={},
            pending_addresses=unique_addresses
        )
    
    # 以下是原有的 add 操作逻辑
    # 确保processing集合存在
    if not hasattr(wallet_analyzer, '_processing_addresses'):
        wallet_analyzer._processing_addresses = set()
    if not hasattr(wallet_analyzer, '_processing_lock'):
        wallet_analyzer._processing_lock = asyncio.Lock()
    
    cached_results = {}
    in_progress_addresses = []
    addresses_to_analyze = []
    
    cache_keys = [f"wallet:{chain}:{addr}" for addr in unique_addresses]
    
    try:
        batch_cache = await cache_service.get_multi(cache_keys)
        
        # 分類每個地址
        async with wallet_analyzer._processing_lock:
            for address in unique_addresses:
                cache_key = f"wallet:{chain}:{address}"
                is_processing = await cache_service.get(f"processing:{chain}:{address}")  # 添加链信息
                if cache_key in batch_cache and batch_cache[cache_key]:
                    # 已緩存結果
                    logger.debug(f"找到 {address} 在 {chain} 鏈上的緩存結果")
                    cached_results[address] = WalletMetrics(
                        address=address,
                        metrics=batch_cache[cache_key],
                        last_updated=int(time.time())
                    )
                elif is_processing:
                    # 正在處理中
                    logger.debug(f"地址 {address} 在 {chain} 鏈上已在處理中")
                    in_progress_addresses.append(address)
                else:
                    # 設置處理中標記（帶過期時間，防止死鎖）
                    await cache_service.set(f"processing:{chain}:{address}", "1", expiry=1800)
                    addresses_to_analyze.append(address)
        
        logger.info(f"地址分類: {len(cached_results)} 個緩存, {len(in_progress_addresses)} 個處理中, {len(addresses_to_analyze)} 個需分析")
    
    except Exception as e:
        logger.exception(f"檢查緩存時發生錯誤: {str(e)}")
        # 保持安全，回退到简单处理
        cached_results = {}
        all_pending = unique_addresses
    
    # 将缓存结果转换为可序列化格式
    ready_results_serializable = {}
    for addr, metrics in cached_results.items():
        ready_results_serializable[addr] = {
            "address": metrics.address,
            "metrics": metrics.metrics,
            "last_updated": metrics.last_updated
        }
    
    # 将所有待处理地址汇总
    all_pending = in_progress_addresses + addresses_to_analyze
    
    # 準備請求狀態
    req_status = {
        "total_addresses": len(unique_addresses),
        "pending_addresses": all_pending,
        "ready_results": ready_results_serializable,
        "start_time": int(time.time()),
        "operation_type": "add",
        "chain": chain
    }
    
    # 保存請求狀態到緩存
    await cache_service.set(f"req:{request_id}", req_status, expiry=3600)  # 1小時過期
    
    # 如果有地址需要分析，使用Celery处理
    if addresses_to_analyze:
        # 优化：决定如何分批
        batch_size = 10  # 默认批次大小
        if len(addresses_to_analyze) > 100:
            batch_size = 20
        elif len(addresses_to_analyze) <= 20:
            batch_size = 5
        
        # 分批创建Celery任务
        address_groups = [addresses_to_analyze[i:i+batch_size] 
                         for i in range(0, len(addresses_to_analyze), batch_size)]
        
        logger.info(f"创建 {len(address_groups)} 个Celery批处理任务")
        
        # 异步提交Celery任务
        async def submit_celery_tasks():
            try:
                from app.workers.tasks import process_wallet_batch
                
                for batch_idx, address_batch in enumerate(address_groups):
                    logger.info(f"提交第 {batch_idx+1}/{len(address_groups)} 批 ({len(address_batch)} 个地址)")
                    
                    # 提交批处理任务到Celery
                    process_wallet_batch.apply_async(
                        kwargs={
                            "request_id": request_id,
                            "addresses": address_batch,
                            "time_range": request.time_range,
                            "include_metrics": request.include_metrics,
                            "batch_index": batch_idx,
                            "chain": chain,  # 添加链信息
                            "twitter_names": [twitter_names[request.addresses.index(addr)] for addr in address_batch],
                            "twitter_usernames": [twitter_usernames[request.addresses.index(addr)] for addr in address_batch]
                        },
                        queue="batch_processing"  # 明确指定队列
                    )
                    
                    # 简短等待，避免一次提交过多任务
                    if batch_idx < len(address_groups) - 1:
                        await asyncio.sleep(0.1)
                
                logger.info(f"所有 {len(addresses_to_analyze)} 个地址已提交到Celery")
            except Exception as e:
                logger.exception(f"提交Celery任务失败: {str(e)}")
                # 出错时清理处理中标记
                async with wallet_analyzer._processing_lock:
                    for addr in addresses_to_analyze:
                        await cache_service.delete(f"processing:{chain}:{addr}")  # 使用 delete 方法清理标记
        
        # 添加背景任务以提交Celery任务
        background_tasks.add_task(submit_celery_tasks)
    
    # 立即返回結果
    return WalletBatchResponse(
        request_id=request_id,
        ready_results=cached_results,
        pending_addresses=all_pending
    )

# 最基本的測試端點
@router.post("/raw-test")
async def raw_test(request: Request):
    """
    最基本的測試端點，接收原始請求
    """
    logger = logging.getLogger(__name__)
    logger.info("Raw test endpoint called")
    
    # 嘗試獲取原始請求體
    try:
        body = await request.body()
        if body:
            body_str = body.decode('utf-8')
            logger.info(f"Received request body: {body_str}")
            try:
                body_json = json.loads(body_str)
                return {
                    "status": "success",
                    "received_raw_body": body_str,
                    "parsed_body": body_json
                }
            except json.JSONDecodeError:
                return {
                    "status": "success",
                    "received_raw_body": body_str,
                    "error": "Could not parse as JSON"
                }
        else:
            return {
                "status": "success",
                "message": "No request body received"
            }
    except Exception as e:
        logger.exception(f"Error processing raw request: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }
    
@router.get("/ping")
def ping():
    """
    簡單的測試端點，確認 API 服務器工作正常
    """
    print("Ping endpoint called")
    return {"status": "ok", "message": "API is running"}

@router.get("/test-solscan")
async def test_solscan(address: str = "4t9bWuZsXXKGMgmd96nFD4KWxyPNTsPm4q9jEMH4jD2i"):
    """
    測試 Solscan API 連接和查詢
    """
    logger = logging.getLogger(__name__)
    logger.info(f"測試 Solscan API，地址: {address}")
    
    try:
        # 使用新的直接 API 測試方法
        api_test_result = await solscan_client.test_api_direct(address)
        
        # 如果直接 API 測試成功
        if api_test_result.get("status") == 200:
            logger.info("Solscan API 測試成功")
            
            # 同時也測試原本的封裝方法
            activities = await solscan_client.get_all_wallet_activities(address)
            
            return {
                "status": "success",
                "api_test": api_test_result,
                "activities_count": len(activities),
                "first_activities": activities[:2] if activities else [] 
            }
        else:
            return {
                "status": "error",
                "api_test": api_test_result,
                "message": "Solscan API 測試失敗"
            }
    except Exception as e:
        logger.exception(f"測試 Solscan API 時發生錯誤: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@router.post("/analyze-token", response_model=TokenAnalysisResponse)
async def analyze_wallet_token(
    request: TokenAnalysisRequest
):
    logger = logging.getLogger(__name__)
    logger.info(f"接收到錢包代幣分析請求：錢包 {request.wallet_address}，代幣 {request.token_address}")
    valid_chains = ["SOLANA", "BSC", "ETH", "BASE", "TRON"]
    chain = getattr(request, "chain", "SOLANA") if hasattr(request, "chain") else "SOLANA"
    chain = chain.upper()
    if chain not in valid_chains:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的區塊鏈類型。支持的類型為: {', '.join(valid_chains)}"
        )
    try:
        from app.services.wallet_token_analyzer import wallet_token_analyzer
        result, from_cache = await wallet_token_analyzer.get_analysis(
            wallet_address=request.wallet_address,
            token_address=request.token_address,
            chain=request.chain,
            force_refresh=False
        )
        logger.info(f"分析完成，{'從緩存獲取' if from_cache else '重新計算'}")
        return TokenAnalysisResponse(**result)
    except Exception as e:
        logger.exception(f"分析錢包代幣交易時發生錯誤: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"分析錢包代幣交易時發生錯誤: {str(e)}"
        )

@router.on_event("startup")
async def startup_event():
    """應用啟動時初始化服務"""
    logger = logging.getLogger(__name__)
    logger.info("正在初始化服務...")
    
    # 初始化 WalletTokenAnalyzer 服務
    from app.services.wallet_token_analyzer import wallet_token_analyzer
    await wallet_token_analyzer.initialize()
    
    logger.info("服務初始化完成")