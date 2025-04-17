#!/usr/bin/env python
# tests/manage_messages.py

import asyncio
import argparse
import sys
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

# 添加父目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 導入必要的模塊
from app.services.cache_service import cache_service
from app.core.config import settings

# 消息狀態顏色
STATUS_COLORS = {
    "pending": "\033[33m",  # 黃色
    "processing": "\033[36m",  # 青色
    "completed": "\033[32m",  # 綠色
    "failed": "\033[31m",  # 紅色
    "permanent_failure": "\033[41m\033[37m",  # 白字紅底
}
RESET_COLOR = "\033[0m"

# 常量
MESSAGE_PREFIX = "kafka_msg:"
MESSAGE_QUEUE = "kafka_message_queue"
PROCESSING_QUEUE = "kafka_processing_queue"

async def get_message_count() -> Dict[str, int]:
    """獲取各種狀態的消息數量"""
    await cache_service.connect()
    
    # 獲取所有消息鍵
    keys = await cache_service.keys_pattern(f"{MESSAGE_PREFIX}*")
    
    # 獲取隊列長度
    pending_count = await cache_service.get_list_length(MESSAGE_QUEUE)
    processing_count = await cache_service.get_list_length(PROCESSING_QUEUE)
    
    # 統計各種狀態的消息
    status_counts = {
        "pending": pending_count,
        "processing": processing_count,
        "total": len(keys)
    }
    
    # 加載前100個消息獲取更詳細的統計
    if keys:
        sample = keys[:min(100, len(keys))]
        messages = await cache_service.get_multi(sample)
        
        for msg_data in messages.values():
            if isinstance(msg_data, dict) and "status" in msg_data:
                status = msg_data.get("status")
                if status not in status_counts:
                    status_counts[status] = 0
                status_counts[status] += 1
    
    return status_counts

async def list_failed_messages(limit: int = 20) -> List[Dict[str, Any]]:
    """列出所有失敗的消息"""
    await cache_service.connect()
    
    # 獲取所有消息鍵
    keys = await cache_service.keys_pattern(f"{MESSAGE_PREFIX}*")
    
    # 收集失敗的消息
    failed_messages = []
    for key in keys:
        message = await cache_service.get(key)
        if message and message.get("status") in ["failed", "permanent_failure"]:
            failed_messages.append(message)
            if len(failed_messages) >= limit:
                break
    
    # 按最後處理時間排序
    failed_messages.sort(key=lambda x: x.get("last_processed", 0), reverse=True)
    
    return failed_messages

async def retry_message(message_id: str) -> bool:
    """重試一個失敗的消息"""
    await cache_service.connect()
    
    # 獲取消息
    message = await cache_service.get(f"{MESSAGE_PREFIX}{message_id}")
    if not message:
        print(f"消息 {message_id} 不存在")
        return False
    
    if message.get("status") not in ["failed", "permanent_failure"]:
        print(f"消息 {message_id} 狀態為 {message.get('status')}，不需要重試")
        return False
    
    # 重置消息狀態
    message["status"] = "pending"
    message["retries"] = 0
    message["last_error"] = None
    message["last_processed"] = None
    message["next_retry"] = None
    
    # 保存更新後的消息
    await cache_service.set(f"{MESSAGE_PREFIX}{message_id}", message, expiry=7 * 24 * 3600)
    
    # 將消息添加到待處理隊列
    await cache_service.add_to_list(MESSAGE_QUEUE, message_id)
    
    print(f"消息 {message_id} 已重新加入待處理隊列")
    return True

async def retry_all_failed() -> int:
    """重試所有失敗的消息"""
    await cache_service.connect()
    
    # 獲取所有消息鍵
    keys = await cache_service.keys_pattern(f"{MESSAGE_PREFIX}*")
    
    # 統計重試的消息數量
    retry_count = 0
    
    # 處理每一個鍵
    for key in keys:
        message = await cache_service.get(key)
        if message and message.get("status") in ["failed", "permanent_failure"]:
            # 獲取消息ID
            message_id = message.get("id")
            
            # 重置消息狀態
            message["status"] = "pending"
            message["retries"] = 0
            message["last_error"] = None
            message["last_processed"] = None
            message["next_retry"] = None
            
            # 保存更新後的消息
            await cache_service.set(key, message, expiry=7 * 24 * 3600)
            
            # 將消息添加到待處理隊列
            await cache_service.add_to_list(MESSAGE_QUEUE, message_id)
            
            retry_count += 1
    
    return retry_count

async def purge_completed(days: int = 7) -> int:
    """清除超過指定天數的已完成消息"""
    await cache_service.connect()
    
    # 獲取所有消息鍵
    keys = await cache_service.keys_pattern(f"{MESSAGE_PREFIX}*")
    
    # 當前時間戳
    current_time = int(datetime.now().timestamp())
    cutoff_time = current_time - (days * 24 * 3600)
    
    # 刪除計數
    delete_count = 0
    
    # 處理每一個鍵
    for key in keys:
        message = await cache_service.get(key)
        if not message:
            continue
            
        # 檢查是否為已完成的消息且已過期
        if (message.get("status") == "completed" and 
            message.get("completed_at", 0) < cutoff_time):
            
            # 刪除消息
            await cache_service.delete(key)
            delete_count += 1
    
    return delete_count

async def view_message(message_id: str) -> Optional[Dict[str, Any]]:
    """查看特定消息的詳情"""
    await cache_service.connect()
    
    # 獲取消息
    message = await cache_service.get(f"{MESSAGE_PREFIX}{message_id}")
    if not message:
        print(f"消息 {message_id} 不存在")
        return None
    
    return message

def print_message(message: Dict[str, Any], verbose: bool = False):
    """美化打印消息"""
    try:
        msg_id = message.get("id", "未知")
        status = message.get("status", "未知")
        created_at = message.get("created_at", 0)
        retries = message.get("retries", 0)
        last_error = message.get("last_error", None)
        
        # 格式化時間戳
        created_time = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        
        # 取得交易信息
        event_data = message.get("data", {}).get("event", {})
        wallet_address = event_data.get("address", "未知")
        token_address = event_data.get("tokenAddress", "未知")
        side = event_data.get("side", "未知")
        amount = event_data.get("txnValue", 0)
        price = event_data.get("price", 0)
        tx_hash = event_data.get("hash", "未知")
        
        # 獲取狀態顏色
        status_color = STATUS_COLORS.get(status, "")
        
        # 打印基本信息
        print(f"ID: {msg_id}")
        print(f"狀態: {status_color}{status}{RESET_COLOR}")
        print(f"創建時間: {created_time}")
        print(f"重試次數: {retries}")
        
        # 打印交易信息
        print(f"錢包地址: {wallet_address}")
        print(f"代幣地址: {token_address}")
        print(f"交易類型: {side}")
        print(f"數量: {amount}")
        print(f"價格: {price}")
        print(f"交易哈希: {tx_hash}")
        
        # 如果存在錯誤信息且是詳細模式
        if last_error and verbose:
            print("\n最後錯誤:")
            print(f"{last_error}")
        
        # 詳細模式下打印完整消息
        if verbose:
            print("\n完整消息:")
            print(json.dumps(message, indent=2))
            
    except Exception as e:
        print(f"打印消息時出錯: {e}")

async def main():
    """主函數"""
    parser = argparse.ArgumentParser(description="Kafka消息管理工具")
    subparsers = parser.add_subparsers(dest="command", help="命令")
    
    # status命令
    subparsers.add_parser("status", help="顯示消息隊列狀態")
    
    # list命令
    list_parser = subparsers.add_parser("list", help="列出失敗的消息")
    list_parser.add_argument("-l", "--limit", type=int, default=20, help="限制返回的消息數量")
    
    # view命令
    view_parser = subparsers.add_parser("view", help="查看消息詳情")
    view_parser.add_argument("message_id", help="消息ID")
    view_parser.add_argument("-v", "--verbose", action="store_true", help="顯示更多信息")
    
    # retry命令
    retry_parser = subparsers.add_parser("retry", help="重試失敗的消息")
    retry_parser.add_argument("message_id", help="消息ID")
    
    # retry-all命令
    subparsers.add_parser("retry-all", help="重試所有失敗的消息")
    
    # purge命令
    purge_parser = subparsers.add_parser("purge", help="清除舊的已完成消息")
    purge_parser.add_argument("-d", "--days", type=int, default=7, help="保留天數")
    
    args = parser.parse_args()
    
    try:
        if args.command == "status":
            counts = await get_message_count()
            print("消息隊列狀態:")
            for status, count in counts.items():
                if status == "total":
                    print(f"總消息數: {count}")
                else:
                    status_color = STATUS_COLORS.get(status, "")
                    print(f"{status_color}{status}{RESET_COLOR}: {count}")
                
        elif args.command == "list":
            failed_messages = await list_failed_messages(args.limit)
            print(f"失敗的消息 (共 {len(failed_messages)} 條):")
            for i, msg in enumerate(failed_messages, 1):
                msg_id = msg.get("id", "未知")
                status = msg.get("status", "未知")
                retries = msg.get("retries", 0)
                created_at = msg.get("created_at", 0)
                
                # 格式化時間戳
                created_time = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
                
                # 獲取狀態顏色
                status_color = STATUS_COLORS.get(status, "")
                
                # 獲取事件數據
                event_data = msg.get("data", {}).get("event", {})
                wallet = event_data.get("address", "未知")[:10] + "..."
                token = event_data.get("tokenAddress", "未知")[:10] + "..."
                
                print(f"{i}. ID: {msg_id}")
                print(f"   狀態: {status_color}{status}{RESET_COLOR}, 重試: {retries}, 時間: {created_time}")
                print(f"   錢包: {wallet}, 代幣: {token}")
                print()
                
        elif args.command == "view":
            message = await view_message(args.message_id)
            if message:
                print_message(message, args.verbose)
                
        elif args.command == "retry":
            result = await retry_message(args.message_id)
            if result:
                print(f"消息 {args.message_id} 已成功加入重試隊列")
            else:
                print(f"重試消息 {args.message_id} 失敗")
                
        elif args.command == "retry-all":
            count = await retry_all_failed()
            print(f"已將 {count} 條失敗消息加入重試隊列")
            
        elif args.command == "purge":
            count = await purge_completed(args.days)
            print(f"已清除 {count} 條過期已完成消息")
            
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"執行命令時出錯: {e}")
    finally:
        await cache_service.close()

if __name__ == "__main__":
    asyncio.run(main())