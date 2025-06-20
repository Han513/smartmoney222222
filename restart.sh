#!/bin/bash
set -e

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日誌函數
log_info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ❌ $1${NC}"
}

# 檢查Docker和docker-compose是否可用
check_dependencies() {
    log_info "檢查依賴項..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安裝或不在 PATH 中"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "docker-compose 未安裝或不在 PATH 中"
        exit 1
    fi
    
    log_success "依賴項檢查完成"
}

# 拉取最新代碼
pull_latest_code() {
    log_info "Step 1: 拉取最新代碼..."
    
    # 設定 GitLab 倉庫 URL
    GITLAB_URL="http://gitlab.codetech.pro/robot/chain/smart_money_import.git"
    
    if [ ! -d ".git" ]; then
        log_warning "當前目錄不是 Git 倉庫，跳過代碼拉取"
        return 0
    fi
    
    # 檢查當前遠程倉庫 URL
    CURRENT_REMOTE_URL=$(git remote get-url origin 2>/dev/null || echo "")
    
    if [ -z "$CURRENT_REMOTE_URL" ]; then
        log_warning "未找到遠程倉庫配置，嘗試添加 origin..."
        git remote add origin "$GITLAB_URL"
    elif [ "$CURRENT_REMOTE_URL" != "$GITLAB_URL" ]; then
        log_info "更新遠程倉庫 URL 為: $GITLAB_URL"
        git remote set-url origin "$GITLAB_URL"
    fi
    
    log_info "從 GitLab 拉取最新代碼: $GITLAB_URL"
    git pull origin main > /tmp/git_pull_output.txt 2>&1
    
    # 如果 main 分支不存在，嘗試 master 分支
    if grep -q "couldn't find remote ref main" /tmp/git_pull_output.txt; then
        log_info "main 分支不存在，嘗試 master 分支..."
        git pull origin master > /tmp/git_pull_output.txt 2>&1
    fi
    
    if grep -q "Already up to date" /tmp/git_pull_output.txt; then
        log_success "沒有變更，跳過重啟流程"
        exit 0
    else
        log_success "有變更，繼續重啟服務"
    fi
}

# 重啟Docker服務
restart_services() {
    log_info "Step 2: 重啟 Docker 服務..."
    
    # 檢查docker-compose.yml是否存在
    if [ ! -f "docker-compose.yml" ]; then
        log_error "找不到 docker-compose.yml 文件"
        exit 1
    fi
    
    # 重啟API服務
    log_info "正在重啟 API 服務..."
    docker-compose restart api
    if [ $? -eq 0 ]; then
        log_success "API 服務重啟成功"
    else
        log_error "API 服務重啟失敗"
        exit 1
    fi
    
    # 重啟Worker服務
    log_info "正在重啟 Worker 服務..."
    docker-compose restart worker
    if [ $? -eq 0 ]; then
        log_success "Worker 服務重啟成功"
    else
        log_error "Worker 服務重啟失敗"
        exit 1
    fi
    
    # 等待服務啟動
    log_info "等待服務完全啟動..."
    sleep 10
}

# 完全重建Docker服務
rebuild_services() {
    log_info "Step 2: 完全重建 Docker 服務..."
    
    # 檢查docker-compose.yml是否存在
    if [ ! -f "docker-compose.yml" ]; then
        log_error "找不到 docker-compose.yml 文件"
        exit 1
    fi
    
    # 停止並移除所有容器
    log_info "正在停止並移除所有容器..."
    docker-compose down
    if [ $? -eq 0 ]; then
        log_success "所有容器已停止並移除"
    else
        log_error "停止容器失敗"
        exit 1
    fi
    
    # 重新構建並啟動服務
    log_info "正在重新構建並啟動服務..."
    docker-compose up --build -d
    if [ $? -eq 0 ]; then
        log_success "服務重新構建並啟動成功"
    else
        log_error "服務重新構建失敗"
        exit 1
    fi
    
    # 等待服務啟動
    log_info "等待服務完全啟動..."
    sleep 15
}

# 檢查服務狀態
check_service_status() {
    log_info "Step 3: 檢查服務狀態..."
    
    # 檢查容器狀態
    log_info "檢查容器狀態..."
    docker-compose ps
    
    # 檢查API健康狀態
    log_info "檢查 API 健康狀態..."
    if curl -f http://localhost:8070/health > /dev/null 2>&1; then
        log_success "API 健康檢查通過"
    else
        log_warning "API 健康檢查失敗"
    fi
}

# 檢查日誌
check_logs() {
    log_info "Step 4: 檢查服務日誌..."
    
    # 檢查API日誌
    log_info "====== API 服務日誌檢查 ======"
    api_logs=$(docker-compose logs --tail=50 api 2>/dev/null)
    if echo "$api_logs" | grep -iE "error|exception|failed|fatal|critical" > /dev/null; then
        log_warning "API 日誌中發現錯誤信息"
        echo "$api_logs" | grep -iE "error|exception|failed|fatal|critical" | tail -5
    else
        log_success "API 日誌中無錯誤信息"
    fi
    
    # 檢查Worker日誌
    log_info "====== Worker 服務日誌檢查 ======"
    worker_logs=$(docker-compose logs --tail=50 worker 2>/dev/null)
    if echo "$worker_logs" | grep -iE "error|exception|failed|fatal|critical" > /dev/null; then
        log_warning "Worker 日誌中發現錯誤信息"
        echo "$worker_logs" | grep -iE "error|exception|failed|fatal|critical" | tail -5
    else
        log_success "Worker 日誌中無錯誤信息"
    fi
    
    # 檢查本地日誌文件（如果存在）
    if [ -d "app/logs" ]; then
        log_info "====== 本地日誌文件檢查 ======"
        find app/logs -type f -name "*.log" | while read -r file; do
            echo "檢查文件: $file"
            file_logs=$(tail -n 20 "$file" 2>/dev/null)
            if echo "$file_logs" | grep -iE "error|exception|failed|fatal|critical" > /dev/null; then
                log_warning "文件 $file 中發現錯誤信息"
                echo "$file_logs" | grep -iE "error|exception|failed|fatal|critical" | tail -3
            else
                log_success "文件 $file 中無錯誤信息"
            fi
        done
    fi
}

# 主函數
main() {
    log_info "開始重啟流程..."
    
    check_dependencies
    pull_latest_code
    restart_services
    check_service_status
    check_logs
    
    log_success "重啟流程完成！"
    
    # 清理臨時文件
    rm -f /tmp/git_pull_output.txt
}

# 重建主函數
rebuild_main() {
    log_info "開始重建流程..."
    
    check_dependencies
    rebuild_services
    check_service_status
    check_logs
    
    log_success "重建流程完成！"
    
    # 清理臨時文件
    rm -f /tmp/git_pull_output.txt
}

# 處理命令行參數
case "${1:-}" in
    "api")
        log_info "僅重啟 API 服務..."
        docker-compose restart api
        log_success "API 服務重啟完成"
        ;;
    "worker")
        log_info "僅重啟 Worker 服務..."
        docker-compose restart worker
        log_success "Worker 服務重啟完成"
        ;;
    "logs")
        log_info "僅檢查日誌..."
        check_logs
        ;;
    "status")
        log_info "檢查服務狀態..."
        docker-compose ps
        ;;
    "pull")
        log_info "僅拉取代碼..."
        pull_latest_code
        ;;
    "rebuild")
        rebuild_main
        ;;
    "help"|"-h"|"--help")
        echo "用法: $0 [選項]"
        echo ""
        echo "選項:"
        echo "  (無參數)    預設執行重建流程（完全重建 + 檢查，不拉取代碼）"
        echo "  rebuild     完全重建流程（完全重建 + 檢查，不拉取代碼）"
        echo "  api         僅重啟 API 服務"
        echo "  worker      僅重啟 Worker 服務"
        echo "  logs        僅檢查日誌"
        echo "  status      檢查服務狀態"
        echo "  pull        僅拉取代碼"
        echo "  help        顯示此幫助信息"
        ;;
    "")
        log_info "未指定參數，預設執行重建流程..."
        rebuild_main
        ;;
    *)
        log_error "未知選項: $1"
        echo "使用 '$0 help' 查看可用選項"
        exit 1
        ;;
esac 