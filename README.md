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
git clone <您的倉庫地址> smart_money_import 
cd smart_money_import 
```

2. 設定環境變數

複製 `.env.example` 文件為 `.env`，並根據您的環境進行配置：

```bash
cp .env.example .env
```

### 主要環境變量說明

現在系統使用`.env`文件進行集中配置，部署人員只需修改此文件即可。以下是關鍵配置項：

```bash
# 數據庫連接串 - 最重要的配置，必須正確設置
DATABASE_URL=postgresql+asyncpg://username:password@hostname:5432/database_name

# 數據庫基本信息
POSTGRES_USER=username
POSTGRES_PASSWORD=password
POSTGRES_DB=database_name

# 後端IP配置
WALLET_SYNC_API_ENDPOINT=http://your.backend:port/endpoint
```

完整配置項請參考`.env.example`文件。

3. 啟動服務

```bash
docker-compose up --build -d
```

4. 檢查服務狀態

```bash
docker-compose ps
```

## 服務管理

### 使用 restart.sh 腳本


#### 預設重建流程
```bash
# 預設執行重建流程：完全重建服務 + 檢查狀態（不拉取代碼）
./restart.sh
```

#### 完全重建流程
```bash
# 執行完全重建流程：完全重建服務 + 檢查狀態（不拉取代碼）
./restart.sh rebuild
```

#### 單獨操作選項
```bash
# 僅重啟 API 服務
./restart.sh api

# 僅重啟 Worker 服務
./restart.sh worker

# 僅檢查服務日誌
./restart.sh logs

# 僅檢查服務狀態
./restart.sh status

# 僅拉取最新代碼
./restart.sh pull

# 查看幫助信息
./restart.sh help
```

#### 腳本功能特點
- 🔄 **自動代碼更新**：僅在 pull 指令中自動拉取最新代碼
- 🚀 **智能重啟**：分別重啟 API 和 Worker 服務
- 🏗️ **完全重建**：支持完全重建服務（適用於依賴更新、配置變更）
- 📊 **狀態檢查**：自動檢查容器狀態和 API 健康狀態
- 📝 **日誌監控**：檢查服務日誌，確認啟動成功
- 🎨 **彩色輸出**：提供清晰的彩色日誌輸出
- ⏱️ **等待機制**：服務重啟後自動等待啟動完成

#### 使用場景建議
| 場景 | 推薦指令 | 說明 |
|------|----------|------|
| 日常重啟服務 | `./restart.sh` | 預設重建流程 |
| 代碼更新後重啟 | `./restart.sh rebuild` | 完全重建，確保代碼更新生效 |
| 依賴包更新 | `./restart.sh rebuild` | 完全重建，確保新依賴生效 |
| 配置變更 | `./restart.sh rebuild` | 完全重建，確保新配置生效 |
| 服務異常 | `./restart.sh rebuild` | 完全重建，解決容器問題 |
| 僅檢查狀態 | `./restart.sh status` | 快速檢查服務運行狀態 |
| 僅查看日誌 | `./restart.sh logs` | 查看服務運行日誌 |
| 僅拉取代碼 | `./restart.sh pull` | 只拉取最新代碼，不重啟服務 |

應該能看到以下容器正在運行：
- smartmoney-api (FastAPI 應用)
- smartmoney-worker (Celery worker)
- redis (Redis伺服器)
- postgres (PostgreSQL資料庫)

## 部署注意事項

### 環境變量與Docker Compose

我們已將docker-compose.yml中的硬編碼配置改為使用環境變量，所有配置現在集中在`.env`文件中管理。部署時，只需修改`.env`文件，無需直接編輯docker-compose.yml。

### Redis配置

服務之間的通信使用Docker Compose網絡自動解析服務名稱，所以 `redis://redis:6379` 這樣的配置無需修改，Docker會自動處理服務名稱到IP的解析。

### 關鍵環境變量檢查

請特別確認以下環境變量已正確設置：
- `DATABASE_URL` - 數據庫連接串
- `WALLET_SYNC_API_ENDPOINT` - 後端API端點

### 腳本權限設置

首次使用 restart.sh 腳本前，請確保腳本有執行權限：

```bash
chmod +x restart.sh
```
