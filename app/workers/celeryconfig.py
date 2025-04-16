"""
Celery 配置文件 - 確保使用正確的 Redis URL
"""

# Redis/Broker 配置
broker_url = "redis://localhost:6379/1"
result_backend = "redis://localhost:6379/1"

# 序列化配置
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'UTC'
enable_utc = True

# 隊列配置
task_routes = {
    "app.workers.tasks.analyze_wallet": {"queue": "wallet_analysis"},
    "app.workers.tasks.process_wallet_batch": {"queue": "batch_processing"},
}

task_queues = {
    "wallet_analysis": {"rate_limit": "100/m"},
    "batch_processing": {"rate_limit": "10/m"},
}

# 任務行為配置
task_acks_late = True
task_reject_on_worker_lost = True
broker_transport_options = {
    'visibility_timeout': 3600,  # 1小時
    'socket_connect_timeout': 5,  # 連接超時
    'socket_timeout': 5,  # 操作超時
    'retry_on_timeout': True,  # 超時重試
    'max_connections': 10,  # 最大連接數
}
result_expires = 3600  # 1小時
broker_connection_retry = True  # 啟用連接重試
broker_connection_retry_on_startup = True  # 啟動時重試
broker_connection_max_retries = 10  # 最大重試次數
broker_pool_limit = 10  # 連接池大小
broker_heartbeat = 10  # 心跳間隔

# 啟用任務事件
worker_send_task_events = True
task_send_sent_event = True 