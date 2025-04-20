# smartmoney222222# SmartMoney 錢包分析服務部署指南

## 專案概述

SmartMoney是一個區塊鏈錢包分析服務，主要分析Solana錢包的交易活動與指標。本專案採用FastAPI框架構建RESTful API，並使用Celery處理異步任務，Redis作為快取和消息代理，以及PostgreSQL存儲數據。

## 系統要求

- Docker & Docker Compose
- Python 3.9 (推薦使用，保證與aiokafka兼容)
- Redis
- PostgreSQL
- Celery

## 快速開始

### 使用Docker Compose部署

1. 複製本項目到您的伺服器

```bash
git clone <您的倉庫地址> smartmoney
cd smartmoney
```

2. 設定環境變數

複製 `.env.example` 文件為 `.env`，並根據您的環境進行配置：

```bash
cp .env.example .env
```

編輯 `.env` 文件，設置必要的環境變數：

```
# 數據庫配置
DATABASE_URL=postgresql://postgres:password@db:5432/smartmoney
DB_ENABLED=true

# Redis配置
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/1
CELERY_RESULT_BACKEND=redis://redis:6379/1

# API配置
API_TITLE=SmartMoney API
API_DESCRIPTION=智能錢包分析API
API_VERSION=1.0.0
API_PREFIX=/api/v1
```

3. 啟動服務

```bash
docker-compose up -d
```

4. 檢查服務狀態

```bash
docker-compose ps
```

應該能看到以下容器正在運行：
- smartmoney-api (FastAPI 應用)
- smartmoney-worker (Celery worker)
- redis (Redis伺服器)
- postgres (PostgreSQL資料庫)

5. 訪問API文檔

API文檔可通過瀏覽器訪問：`http://<您的服務器IP>:8070/docs`

## 服務組成

- **API服務 (run.py)**: FastAPI應用，提供錢包分析的REST API接口
- **Worker服務 (start_worker.py)**: Celery worker，處理異步的錢包分析任務
- **Redis**: 用於緩存和Celery消息代理
- **PostgreSQL**: 存儲錢包分析數據和結果

## 手動部署指南

如果您不想使用Docker，也可以手動部署：

1. 安裝Python 3.9

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.9 python3.9-dev python3.9-venv

# CentOS/RHEL
sudo yum install python39 python39-devel
```

2. 創建虛擬環境

```bash
python3.9 -m venv .venv_py39
source .venv_py39/bin/activate
```

3. 安裝依賴

```bash
pip install -r requirements.txt
```

4. 啟動API服務

```bash
python run.py
```

5. 在另一個終端中啟動Worker服務

```bash
python start_worker.py
```

## 監控

服務狀態可以通過以下方式監控：

- **API健康檢查**: `GET /health` 端點
- **Docker日誌**:
  ```bash
  docker-compose logs -f smartmoney-api
  docker-compose logs -f smartmoney-worker
  ```

## 常見問題

### Python版本兼容性

本項目使用的aiokafka版本與Python 3.10及以上版本不兼容，建議使用Python 3.9。

### Redis連接問題

如果遇到Redis連接問題，請確保Redis服務正在運行，並且`.env`文件中的Redis配置正確。

### Worker無法連接Celery代理

確保Celery broker URL配置正確，並且Redis服務可用。

## 聯繫與支持

如有問題，請聯繫項目維護者或提交Issue至代碼倉庫。