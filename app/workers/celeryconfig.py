"""
Celery 配置文件 - 確保使用正確的 Redis URL
"""

# Redis/Broker 配置 - 使用环境变量
import os
import json

# 直接从环境变量读取，避免依赖pydantic_settings
broker_url = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/1')
result_backend = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/1')

# 读取环境变量中的transport_options
broker_transport_options_str = os.getenv('CELERY_BROKER_TRANSPORT_OPTIONS', '{}')
result_backend_transport_options_str = os.getenv('CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS', '{}')

try:
    broker_transport_options_env = json.loads(broker_transport_options_str)
    result_backend_transport_options_env = json.loads(result_backend_transport_options_str)
except json.JSONDecodeError:
    broker_transport_options_env = {}
    result_backend_transport_options_env = {}

# 序列化配置
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'UTC'
enable_utc = True

# 日誌配置
worker_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
worker_task_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
worker_log_file = 'app/logs/worker.log'
worker_log_level = 'INFO'

# 隊列配置 - 简化队列配置，避免集群模式下的哈希槽问题
task_routes = {
    "app.workers.tasks.analyze_wallet": {"queue": "celery"},
    "app.workers.tasks.process_wallet_batch": {"queue": "celery"},
    "app.workers.tasks.process_single_wallet": {"queue": "celery"},
    "app.workers.tasks.remove_wallet_data_batch": {"queue": "celery"},
}

# 使用默认队列，避免多队列导致的哈希槽问题
task_default_queue = 'celery'
task_default_exchange = 'celery'
task_default_routing_key = 'celery'

# 任務行為配置
task_acks_late = True
task_reject_on_worker_lost = True

# Redis集群兼容配置 - 合并环境变量和默认配置
broker_transport_options = {
    'visibility_timeout': 3600,  # 1小時
    'socket_connect_timeout': 5,  # 連接超時
    'socket_timeout': 5,  # 操作超時
    'retry_on_timeout': True,  # 超時重試
    'max_connections': 10,  # 最大連接數
    # Redis集群兼容配置
    'global_keyprefix': 'celery',  # 全局key前缀，确保所有key在同一哈希槽
}
# 合并环境变量中的配置
broker_transport_options.update(broker_transport_options_env)

# 结果后端配置 - 合并环境变量和默认配置
result_backend_transport_options = {
    'visibility_timeout': 3600,
    'socket_connect_timeout': 5,
    'socket_timeout': 5,
    'retry_on_timeout': True,
    'max_connections': 10,
    # Redis集群兼容配置
    'global_keyprefix': 'celery',
}
# 合并环境变量中的配置
result_backend_transport_options.update(result_backend_transport_options_env)

result_expires = 3600  # 1小時
broker_connection_retry = True  # 啟用連接重試
broker_connection_retry_on_startup = True  # 啟動時重試
broker_connection_max_retries = 10  # 最大重試次數
broker_pool_limit = 10  # 連接池大小
broker_heartbeat = 10  # 心跳間隔

# 禁用任務事件 - 避免Redis集群问题
worker_send_task_events = False
task_send_sent_event = False

# 集群模式下的特殊配置
broker_use_ssl = False  # 根据实际情况调整
result_backend_use_ssl = False  # 根据实际情况调整

# 禁用一些在集群模式下可能有问题的功能
worker_disable_rate_limits = True  # 禁用速率限制
worker_prefetch_multiplier = 1  # 减少预取数量 