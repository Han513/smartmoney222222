import asyncio
import time
import sys
import logging
import argparse
from datetime import datetime
import aiohttp
import base58
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("rpc_test_results.log")
    ]
)
logger = logging.getLogger(__name__)

# 測試用的錢包地址
TEST_WALLET = "4t9bWuZsXXKGMgmd96nFD4KWxyPNTsPm4q9jEMH4jD2i"  # 一個公開的Solana錢包地址

# 定義要測試的RPC端點
DEFAULT_RPC_ENDPOINTS = [
    "https://methodical-capable-firefly.solana-mainnet.quiknode.pro/f660ad44a1d7512bb5f81c93144712e8ddc5c2dc",
    "https://patient-fabled-needle.solana-mainnet.quiknode.pro/befd34a7534b2733f326b0df7cf2fb89b979cbb7/",
    "https://api.mainnet-beta.solana.com"
]

async def test_rpc_connection(url: str) -> dict:
    """測試RPC連接並返回結果"""
    result = {
        "url": url,
        "status": "error",
        "message": "",
        "response_time": 0,
        "block_height": None,
        "slot": None,
        "balance_test": False,
        "get_block_test": False,
        "timestamp": datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    try:
        # 1. 基本連接測試
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
                timeout=10
            ) as response:
                health_data = await response.json()
                result["health_response"] = health_data
                
                if response.status == 200:
                    if "result" in health_data and health_data["result"] == "ok":
                        result["status"] = "ok"
                        result["message"] = "RPC健康狀態: 正常"
                    else:
                        result["status"] = "warning"
                        result["message"] = f"RPC回應成功但健康檢查失敗: {health_data}"
                else:
                    result["status"] = "error"
                    result["message"] = f"HTTP錯誤: {response.status}"
                    return result
        
        # 2. 使用Solana客戶端測試
        client = AsyncClient(url)
        
        # 2.1 獲取最新區塊高度/slot
        slot_response = await client.get_slot()
        result["slot"] = slot_response.value
        result["get_block_test"] = True
        
        # 2.2 獲取版本信息
        version = await client.get_version()
        result["solana_version"] = version.to_json()
        
        # 2.3 測試餘額查詢
        try:
            pubkey = Pubkey(base58.b58decode(TEST_WALLET))
            balance = await client.get_balance(pubkey=pubkey)
            result["balance_test"] = True
            result["test_wallet_balance"] = balance.value / 10**9
        except Exception as e:
            result["balance_test_error"] = str(e)
        
        # 關閉客戶端
        await client.close()
        
    except asyncio.TimeoutError:
        result["status"] = "error"
        result["message"] = "連接超時"
    except Exception as e:
        result["status"] = "error"
        result["message"] = f"連接錯誤: {str(e)}"
    finally:
        result["response_time"] = round(time.time() - start_time, 2)
        return result

async def test_multiple_endpoints(endpoints: list) -> list:
    """測試多個RPC端點"""
    results = []
    for url in endpoints:
        logger.info(f"測試RPC端點: {url}")
        result = await test_rpc_connection(url)
        
        status_emoji = "✅" if result["status"] == "ok" else "⚠️" if result["status"] == "warning" else "❌"
        logger.info(f"{status_emoji} {url}: {result['status']} - 回應時間: {result['response_time']}秒")
        
        if result["status"] == "ok":
            logger.info(f"  Slot: {result['slot']}")
            logger.info(f"  餘額測試: {'成功' if result['balance_test'] else '失敗'}")
        else:
            logger.info(f"  錯誤信息: {result['message']}")
        
        results.append(result)
    
    return results

def generate_report(results: list):
    """生成測試報告"""
    logger.info("\n" + "="*50)
    logger.info("Solana RPC端點測試報告")
    logger.info("="*50)
    
    working_endpoints = [r for r in results if r["status"] == "ok"]
    failed_endpoints = [r for r in results if r["status"] != "ok"]
    
    logger.info(f"測試時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"測試端點總數: {len(results)}")
    logger.info(f"正常運作的端點: {len(working_endpoints)}")
    logger.info(f"問題端點: {len(failed_endpoints)}")
    
    if working_endpoints:
        # 按回應時間排序
        working_endpoints.sort(key=lambda x: x["response_time"])
        logger.info("\n正常運作的端點 (按回應時間排序):")
        for i, result in enumerate(working_endpoints, 1):
            logger.info(f"{i}. {result['url']} - 回應時間: {result['response_time']}秒, Slot: {result['slot']}")
    
    if failed_endpoints:
        logger.info("\n問題端點:")
        for i, result in enumerate(failed_endpoints, 1):
            logger.info(f"{i}. {result['url']} - 狀態: {result['status']}")
            logger.info(f"   錯誤信息: {result['message']}")
    
    logger.info("="*50)
    
    # 推薦最佳端點
    if working_endpoints:
        best_endpoint = working_endpoints[0]
        logger.info(f"\n推薦使用的端點: {best_endpoint['url']}")
        logger.info(f"回應時間: {best_endpoint['response_time']}秒")
    else:
        logger.info("\n沒有可用的端點!")

async def main():
    parser = argparse.ArgumentParser(description="測試Solana RPC端點連接")
    parser.add_argument("--urls", nargs="+", help="要測試的RPC URL列表")
    parser.add_argument("--timeout", type=int, default=10, help="連接超時時間(秒)")
    args = parser.parse_args()
    
    endpoints = args.urls if args.urls else DEFAULT_RPC_ENDPOINTS
    logger.info(f"將測試 {len(endpoints)} 個Solana RPC端點")
    
    results = await test_multiple_endpoints(endpoints)
    generate_report(results)
    
    # 將結果保存為JSON以便進一步分析
    import json
    with open("rpc_test_results.json", "w") as f:
        # 將datetime對象轉換為字符串
        json_results = []
        for result in results:
            json_result = {k: (v if not isinstance(v, datetime) else v.isoformat()) for k, v in result.items()}
            json_results.append(json_result)
        json.dump(json_results, f, indent=2)
    
    logger.info("測試結果已保存到 rpc_test_results.json")
    
    # 返回代碼：如果至少有一個端點正常，則返回0，否則返回1
    return 0 if any(r["status"] == "ok" for r in results) else 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
