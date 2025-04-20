import requests
import json

BASE_URL = "http://127.0.0.1:8070"  # 請根據您的API實際運行地址調整

def test_json_request():
    print("測試JSON格式請求...")
    payload = {
        "key1": "value1",
        "key2": 123,
        "nested": {
            "inner_key": "inner_value"
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/api/wallets/raw-test", 
        json=payload
    )
    
    print(f"狀態碼: {response.status_code}")
    print(f"回應內容: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print("-" * 50)

def test_form_data_request():
    print("測試表單數據請求...")
    data = {
        "field1": "value1",
        "field2": "value2"
    }
    
    response = requests.post(
        f"{BASE_URL}/api/wallets/raw-test", 
        data=data
    )
    
    print(f"狀態碼: {response.status_code}")
    print(f"回應內容: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print("-" * 50)

def test_raw_string_request():
    print("測試原始字符串請求...")
    payload = "這是一個純文本請求"

    response = requests.post(
        f"{BASE_URL}/api/wallets/raw-test", 
        data=payload
    )

    print(f"狀態碼: {response.status_code}")
    print(f"回應內容: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print("-" * 50)

def test_binary_data_request():
    print("測試二進制數據請求...")
    # 創建一個簡單的二進制數據
    binary_data = b'\x00\x01\x02\x03\x04\x05'

    response = requests.post(
        f"{BASE_URL}/api/wallets/raw-test", 
        data=binary_data
    )
 
    print(f"狀態碼: {response.status_code}")
    print(f"回應內容: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print("-" * 50)

def test_empty_request():
    print("測試空請求...")

    response = requests.post(
        f"{BASE_URL}/api/wallets/raw-test"
    )

    print(f"狀態碼: {response.status_code}")
    print(f"回應內容: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print("-" * 50)

if __name__ == "__main__":
    print("開始測試API接收不同類型請求的能力...")
    test_json_request()
    test_form_data_request()
    test_raw_string_request()
    test_binary_data_request()
    test_empty_request()
    print("測試完成!") 