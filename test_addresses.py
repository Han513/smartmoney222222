import asyncio
from app.services.solscan import solscan_client

async def test_addresses():
    addresses = [
        'CQZdF5toSJhexCMp1AR9b5Kk1WJEJCzuDN6uQJ8fUdtG',
        'HtucFepgUkMpHdrYsxMqjBNN6qVBdjmFaLZneNXopuJm'
    ]
    
    for addr in addresses:
        print(f'\n測試地址: {addr}')
        try:
            result = await solscan_client.test_api_direct(addr)
            print(f'狀態: {result.get("status")}')
            print(f'活動數量: {result.get("activity_count", 0)}')
            if result.get('error'):
                print(f'錯誤: {result["error"]}')
        except Exception as e:
            print(f'測試失敗: {e}')
    
    await solscan_client.close_session()

if __name__ == "__main__":
    asyncio.run(test_addresses()) 