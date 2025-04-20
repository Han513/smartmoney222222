"""
測試 API 連接的腳本
"""
import requests
import json
import logging
import time

# 配置日誌
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API URL
BASE_URL = "http://localhost:8000"
API_PREFIX = "/api"  # API 前綴

def test_ping():
    """測試 ping 端點"""
    try:
        logger.info("Testing /ping endpoint...")
        response = requests.get(f"{BASE_URL}/ping")  # 注意: ping 端點在根路徑，不在 API 前綴下
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        return response
    except Exception as e:
        logger.error(f"Error testing /ping: {str(e)}")
        return None

def test_health():
    """測試 health 端點"""
    try:
        logger.info("Testing /health endpoint...")
        response = requests.get(f"{BASE_URL}/health")  # 注意: health 端點在根路徑，不在 API 前綴下
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        return response
    except Exception as e:
        logger.error(f"Error testing /health: {str(e)}")
        return None

def test_analyze_simple():
    """測試簡化版分析端點"""
    try:
        logger.info("Testing /api/wallets/analyze-simple endpoint...")
        data = {
            "addresses": ["EXQoMqYqoHKG1mNpV8NnzjtJkef8SwcUYAw69mZFnZA"],  # 修正參數名稱
            "chain": "solana"
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/wallets/analyze-simple", 
            data=json.dumps(data),
            headers=headers
        )
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        return response
    except Exception as e:
        logger.error(f"Error testing /api/wallets/analyze-simple: {str(e)}")
        return None

def test_analyze():
    """測試完整分析端點"""
    try:
        logger.info("Testing /api/wallets/analyze endpoint...")
        data = {
            "addresses": ["EXQoMqYqoHKG1mNpV8NnzjtJkef8SwcUYAw69mZFnZA"],  # 修正參數名稱
            "chain": "solana"
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/wallets/analyze", 
            data=json.dumps(data),
            headers=headers
        )
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response body: {response.text}")
        
        # 如果成功，檢查請求 ID 並查詢狀態
        if response.status_code == 200:
            try:
                result = response.json()
                if "request_id" in result:
                    request_id = result["request_id"]
                    logger.info(f"Got request ID: {request_id}")
                    
                    # 等待幾秒讓後台處理
                    logger.info("Waiting 3 seconds...")
                    time.sleep(3)
                    
                    # 查詢分析狀態
                    status_url = f"{BASE_URL}{API_PREFIX}/wallets/analyze-wallets/{request_id}"
                    logger.info(f"Checking status at: {status_url}")
                    status_response = requests.get(status_url)
                    logger.info(f"Status Response: {status_response.status_code}")
                    logger.info(f"Status Body: {status_response.text}")
            except Exception as e:
                logger.error(f"Error checking analysis status: {str(e)}")
        
        return response
    except Exception as e:
        logger.error(f"Error testing /api/wallets/analyze: {str(e)}")
        return None

def test_raw_request():
    """測試原始請求端點，使用不同格式的數據"""
    try:
        logger.info("Testing /api/wallets/raw-test endpoint with JSON data...")
        # 測試有效的JSON
        json_data = {
            "wallet_address": "EXQoMqYqoHKG1mNpV8NnzjtJkef8SwcUYAw69mZFnZA",
            "chain": "solana"
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/wallets/raw-test", 
            data=json.dumps(json_data),
            headers=headers
        )
        logger.info(f"JSON Response status: {response.status_code}")
        logger.info(f"JSON Response body: {response.text}")
        
        # 測試普通文本
        logger.info("Testing /api/wallets/raw-test endpoint with plain text...")
        plain_text = "This is a plain text request"
        headers = {"Content-Type": "text/plain"}
        response = requests.post(
            f"{BASE_URL}{API_PREFIX}/wallets/raw-test", 
            data=plain_text,
            headers=headers
        )
        logger.info(f"Text Response status: {response.status_code}")
        logger.info(f"Text Response body: {response.text}")
        
        # 測試無內容請求
        logger.info("Testing /api/wallets/raw-test endpoint with empty request...")
        response = requests.post(f"{BASE_URL}{API_PREFIX}/wallets/raw-test")
        logger.info(f"Empty Response status: {response.status_code}")
        logger.info(f"Empty Response body: {response.text}")
        
        return response
    except Exception as e:
        logger.error(f"Error testing /api/wallets/raw-test: {str(e)}")
        return None

def test_wallet_analysis():
    """測試單個錢包分析端點，強制重新分析"""
    try:
        # 測試有問題的錢包地址
        address = "3m213yY4n12wuY8P3L5nRDy5erkCibhkz9fbQDQ4Nuz7"
        logger.info(f"Testing /api/wallets/wallet/{address} endpoint with force_refresh=true...")
        
        # 帶有 force_refresh 參數
        response = requests.get(f"{BASE_URL}{API_PREFIX}/wallets/wallet/{address}?force_refresh=true")
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response body: {response.text[:1500]}...") # 只顯示前500個字符
        
        return response
    except Exception as e:
        logger.error(f"Error testing wallet analysis: {str(e)}")
        return None

if __name__ == "__main__":
    # 延遲一下等待服務器啟動
    time.sleep(1)
    
    logger.info("=== Starting API Tests ===")
    
    # 測試基本健康端點
    health_response = test_health()
    
    # 測試單個錢包分析端點 (強制重新分析)
    wallet_analysis_response = test_wallet_analysis()
    
    logger.info("=== API Tests Completed ===") 