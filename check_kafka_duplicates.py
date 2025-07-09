#!/usr/bin/env python3
"""
Kafka 重複消息檢查工具
用於檢查 topic 中是否存在重複的 signature
"""

import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
import argparse

def run_kafka_consumer(topic, bootstrap_server, timeout_seconds=300):
    """運行 kafka-console-consumer 命令"""
    try:
        cmd = [
            'kafka-console-consumer.sh',
            '--bootstrap-server', bootstrap_server,
            '--topic', topic,
            '--from-beginning',
            '--timeout-ms', str(timeout_seconds * 1000)
        ]
        
        print(f"執行命令: {' '.join(cmd)}")
        print(f"正在消費 topic: {topic}，超時時間: {timeout_seconds} 秒")
        print("=" * 60)
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_seconds + 10)
        
        if result.returncode != 0:
            print(f"命令執行失敗: {result.stderr}")
            return None
            
        return result.stdout
        
    except subprocess.TimeoutExpired:
        print(f"命令執行超時 ({timeout_seconds} 秒)")
        return None
    except Exception as e:
        print(f"執行命令時發生錯誤: {e}")
        return None

def analyze_messages(messages_text):
    """分析消息內容，統計重複的 signature"""
    if not messages_text:
        print("沒有收到任何消息")
        return
    
    signature_count = defaultdict(list)
    total_messages = 0
    valid_messages = 0
    
    print("正在分析消息...")
    
    for line_num, line in enumerate(messages_text.strip().split('\n'), 1):
        if not line.strip():
            continue
            
        total_messages += 1
        
        try:
            # 解析 JSON 消息
            message = json.loads(line)
            
            # 提取 signature
            if 'event' in message and 'hash' in message['event']:
                signature = message['event']['hash']
                timestamp = message['event'].get('timestamp', 0)
                
                signature_count[signature].append({
                    'line': line_num,
                    'timestamp': timestamp,
                    'message': message
                })
                valid_messages += 1
            else:
                print(f"第 {line_num} 行: 消息格式異常，缺少 event.hash")
                
        except json.JSONDecodeError as e:
            print(f"第 {line_num} 行: JSON 解析失敗 - {e}")
        except Exception as e:
            print(f"第 {line_num} 行: 處理失敗 - {e}")
    
    print(f"\n分析完成:")
    print(f"總消息數: {total_messages}")
    print(f"有效消息數: {valid_messages}")
    print(f"唯一 signature 數: {len(signature_count)}")
    
    # 找出重複的 signature
    duplicates = {sig: msgs for sig, msgs in signature_count.items() if len(msgs) > 1}
    
    if duplicates:
        print(f"\n發現 {len(duplicates)} 個重複的 signature:")
        print("=" * 60)
        
        for signature, messages in sorted(duplicates.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"\nSignature: {signature}")
            print(f"重複次數: {len(messages)}")
            
            for i, msg_info in enumerate(messages[:5]):  # 只顯示前5個
                timestamp = msg_info['timestamp']
                if timestamp > 10**12:  # 毫秒轉秒
                    timestamp = timestamp // 1000
                dt = datetime.fromtimestamp(timestamp)
                print(f"  {i+1}. 行 {msg_info['line']}, 時間: {dt}")
            
            if len(messages) > 5:
                print(f"  ... 還有 {len(messages) - 5} 個重複")
    else:
        print("\n✅ 沒有發現重複的 signature")
    
    # 顯示統計信息
    print(f"\n統計信息:")
    print(f"重複 signature 數: {len(duplicates)}")
    print(f"重複消息總數: {sum(len(msgs) for msgs in duplicates.values())}")
    
    if duplicates:
        print(f"重複率: {sum(len(msgs) for msgs in duplicates.values()) / valid_messages * 100:.2f}%")

def main():
    parser = argparse.ArgumentParser(description='檢查 Kafka topic 中的重複消息')
    parser.add_argument('--topic', default='smart_token_events', help='Kafka topic 名稱')
    parser.add_argument('--bootstrap-server', default='172.25.183.205:9092', help='Kafka 服務器地址')
    parser.add_argument('--timeout', type=int, default=300, help='消費超時時間（秒）')
    
    args = parser.parse_args()
    
    print(f"開始檢查 Kafka topic: {args.topic}")
    print(f"服務器: {args.bootstrap_server}")
    print(f"超時時間: {args.timeout} 秒")
    print("=" * 60)
    
    # 運行 kafka consumer
    messages = run_kafka_consumer(args.topic, args.bootstrap_server, args.timeout)
    
    if messages:
        # 分析消息
        analyze_messages(messages)
    else:
        print("無法獲取消息，請檢查 Kafka 連接和 topic 名稱")

if __name__ == "__main__":
    main() 