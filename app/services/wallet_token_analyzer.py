import logging
import asyncio
import time
from typing import Dict, Any, Tuple
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, update, func, and_, select, text
from app.core.db import get_session_factory
from app.models.models import TokenBuyData, TempTokenBuyData
from app.services.solscan import solscan_client
from app.services.cache_service import cache_service

logger = logging.getLogger(__name__)

# 添加一個格式化數字為整數字符串的函數
def format_number_as_integer_string(value):
    """
    將數字格式化為整數字符串，避免科學符號表示法
    處理小數時，將它們四捨五入為最接近的整數
    """
    if value is None:
        return "0"
        
    # 如果是字符串，嘗試轉換為浮點數
    if isinstance(value, str):
        try:
            value = float(value)
        except (ValueError, TypeError):
            return "0"
    
    # 確保是數字類型
    if not isinstance(value, (int, float, Decimal)):
        return "0"
    
    # 如果接近於零的非常小的數值，直接返回"0"
    if abs(float(value)) < 0.1:
        return "0"
    
    # 使用四捨五入並轉換為整數字符串
    try:
        return str(round(float(value)))
    except:
        return "0"

# 改為更合適的函數名
def format_number_string(value):
    """
    格式化數字為字符串，保留小數但避免科學符號表示法
    1. 如果值為0或接近0，返回"0"
    2. 否則保留小數並避免科學符號
    """
    if value is None:
        return "0"
        
    # 如果是字符串，嘗試轉換為浮點數
    if isinstance(value, str):
        try:
            value = float(value)
        except (ValueError, TypeError):
            return "0"
    
    # 確保是數字類型
    if not isinstance(value, (int, float, Decimal)):
        return "0"
    
    # 如果接近於零的非常小的數值，直接返回"0"
    if abs(float(value)) < 0.000001:
        return "0"
    
    # 轉換為字符串，保留小數但避免科學符號
    try:
        # 使用格式化字符串避免科學符號
        num_str = f"{float(value):.10f}".rstrip('0').rstrip('.') if '.' in f"{float(value):.10f}" else f"{float(value):.0f}"
        return num_str
    except:
        return "0"

class WalletTokenAnalyzer:
    """錢包代幣分析服務"""
    
    def __init__(self):
        self.session_factory = get_session_factory()
        self.cache_ttl = 600  # 緩存過期時間（10分鐘）
        self.refresh_interval = 600  # 刷新間隔（10分鐘）
        self._refresh_task = None
        self._initialized = False
        self._cache_lock = asyncio.Lock()  # 添加緩存鎖
        self._batch_size = 1000  # 批量處理大小
    
    async def initialize(self):
        """初始化服務，加載數據到緩存並啟動定期刷新任務"""
        if self._initialized:
            return
            
        logger.info("開始初始化 WalletTokenAnalyzer 服務...")
        
        self._initialized = True
        logger.info("WalletTokenAnalyzer 服務初始化完成")
    
    async def _load_data_to_cache(self):
        """從數據庫加載所有數據到緩存"""
        logger.info("開始從數據庫加載數據到緩存...")

        try:
            with self.session_factory() as session:
                stmt = select(TokenBuyData).where(TokenBuyData.chain == "SOLANA")
                results = session.execute(stmt).scalars().all()

                for data in results:
                    cache_key = f"wallet_token:{data.wallet_address}:{data.token_address}"
                    analysis_data = self._convert_to_dict(data)
                    await cache_service.set(cache_key, analysis_data, expiry=self.cache_ttl)

                logger.info(f"已加載 {len(results)} 條記錄到緩存")
        except Exception as e:
            logger.error(f"加載數據到緩存時發生錯誤: {str(e)}")
            pass
    
    async def _update_cache_batch(self, data_list):
        """批量更新緩存"""
        async with self._cache_lock:
            for data in data_list:
                cache_key = f"wallet_token:{data.wallet_address}:{data.token_address}:{data.chain}"
                cache_data = {
                    "wallet_address": data.wallet_address,
                    "token_address": data.token_address,
                    "holding_amount": format_number_string(float(data.total_amount or 0)),
                    "total_buy_value": format_number_string(float(data.total_cost or 0)),
                    "total_buy_amount": format_number_string(float(data.historical_total_buy_amount or 0)),
                    "total_buy_count": format_number_as_integer_string(int(data.total_buy_count or 0)),
                    "total_sell_value": format_number_string(float(data.historical_total_sell_value or 0)),
                    "total_sell_amount": format_number_string(float(data.historical_total_sell_amount or 0)),
                    "total_sell_count": format_number_as_integer_string(int(data.total_sell_count or 0)),
                    "realized_pnl": format_number_string(float(data.realized_profit or 0)),
                    "total_holding_seconds": int(data.total_holding_seconds or 0)
                }
                await cache_service.set(cache_key, cache_data, expiry=self.cache_ttl)
    
    async def _periodic_refresh(self):
        """定期刷新緩存數據"""
        while True:
            try:
                logger.info("開始定期刷新緩存數據...")
                with self.session_factory() as session:
                    # 分批處理數據
                    offset = 0
                    while True:
                        stmt = select(TokenBuyData).offset(offset).limit(self._batch_size)
                        results = session.execute(stmt).scalars().all()
                        
                        if not results:
                            break
                            
                        # 批量更新緩存
                        await self._update_cache_batch(results)
                        
                        offset += self._batch_size
                        logger.info(f"已處理 {offset} 條記錄")
                        
                        # 短暫休息，避免過度消耗資源
                        await asyncio.sleep(0.1)
                
                logger.info("緩存數據刷新完成")
            except Exception as e:
                logger.error(f"刷新緩存數據時發生錯誤: {str(e)}")
            
            await asyncio.sleep(self.refresh_interval)
    
    async def _refresh_cache_data(self):
        """刷新緩存數據"""
        with self.session_factory() as session:
            stmt = select(TokenBuyData)
            results = session.execute(stmt).scalars().all()
            
            for data in results:
                try:
                    # 異步更新數據
                    asyncio.create_task(self._update_analysis_data(
                        data.wallet_address,
                        data.token_address
                    ))
                except Exception as e:
                    logger.error(f"更新分析數據時發生錯誤: {str(e)}")
    
    def _filter_db_fields(self, data: dict) -> dict:
        db_fields = {
            "wallet_address", "token_address", "chain", "total_amount", "total_cost", "avg_buy_price",
            "position_opened_at", "historical_total_buy_amount", "historical_total_buy_cost",
            "historical_total_sell_amount", "historical_total_sell_value", "historical_avg_buy_price",
            "historical_avg_sell_price", "last_active_position_closed_at", "last_transaction_time",
            "realized_profit", "realized_profit_percentage", "updated_at",
            "total_buy_count", "total_sell_count"
        }
        return {k: v for k, v in data.items() if k in db_fields}

    async def _update_analysis_data(self, wallet_address: str, token_address: str):
        """更新分析數據"""
        try:
            # 從資料庫查 chain
            with self.session_factory() as session:
                stmt = select(TokenBuyData).where(
                    TokenBuyData.wallet_address == wallet_address,
                    TokenBuyData.token_address == token_address
                )
                result = session.execute(stmt).scalar_one_or_none()
                chain = result.chain if result and hasattr(result, 'chain') and result.chain else "SOLANA"

            # 計算30天前的日期
            thirty_days_ago = datetime.now() - timedelta(days=30)
            with self.session_factory() as session:
                subquery = select(TokenBuyData).where(
                    and_(
                        TokenBuyData.wallet_address == wallet_address,
                        TokenBuyData.token_address == token_address,
                        TokenBuyData.chain == chain,
                        TokenBuyData.date >= thirty_days_ago
                    )
                ).subquery()
                stmt = select(
                    func.sum(subquery.c.total_amount).label('total_amount'),
                    func.sum(subquery.c.total_cost).label('total_cost'),
                    func.sum(subquery.c.historical_total_buy_amount).label('historical_total_buy_amount'),
                    func.sum(subquery.c.historical_total_buy_cost).label('historical_total_buy_cost'),
                    func.sum(subquery.c.historical_total_sell_amount).label('historical_total_sell_amount'),
                    func.sum(subquery.c.historical_total_sell_value).label('historical_total_sell_value'),
                    func.sum(subquery.c.realized_profit).label('realized_profit'),
                    func.sum(subquery.c.total_buy_count).label('total_buy_count'),
                    func.sum(subquery.c.total_sell_count).label('total_sell_count'),
                    func.sum(subquery.c.total_holding_seconds).label('total_holding_seconds')
                )
                result = session.execute(stmt).first()
                
                # 確保所有數值都正確轉換，避免Decimal序列化問題
                analysis_data = {
                    "wallet_address": wallet_address,
                    "token_address": token_address,
                    "holding_amount": format_number_string(max(0, float(result.total_amount or 0))),
                    "total_buy_value": format_number_string(float(result.historical_total_buy_cost or 0)),
                    "total_buy_amount": format_number_string(float(result.historical_total_buy_amount or 0)),
                    "total_buy_count": format_number_as_integer_string(int(result.total_buy_count or 0)),
                    "total_sell_value": format_number_string(float(result.historical_total_sell_value or 0)),
                    "total_sell_amount": format_number_string(float(result.historical_total_sell_amount or 0)),
                    "total_sell_count": format_number_as_integer_string(int(result.total_sell_count or 0)),
                    "realized_pnl": format_number_string(float(result.realized_profit or 0)),
                    "total_holding_seconds": int(result.total_holding_seconds or 0)
                }

            # 更新緩存（key包含chain）
            cache_key = f"wallet_token:{wallet_address}:{token_address}:{chain}"
            await cache_service.set(cache_key, analysis_data, expiry=self.cache_ttl)
            logger.info(f"已更新錢包 {wallet_address} 代幣 {token_address}（{chain}）的分析數據")
        except Exception as e:
            logger.error(f"更新分析數據時發生錯誤: {str(e)}")
    
    def _fill_default_fields(self, data: dict) -> dict:
        # 只針對 API 需要的欄位做填補
        fields = {
            "wallet_address": "",
            "token_address": "",
            "holding_amount": "0",
            "total_buy_value": "0",
            "total_buy_amount": "0",
            "total_buy_count": "0",
            "total_sell_value": "0",
            "total_sell_amount": "0",
            "total_sell_count": "0",
            "realized_pnl": "0",
            "total_holding_seconds": 0
        }
        for k, v in fields.items():
            if data.get(k) is None:
                data[k] = v
            elif k != "total_holding_seconds" and not isinstance(data[k], str):
                # 將非 total_holding_seconds 的數值欄位轉為字串
                data[k] = str(data[k])
        return data

    async def get_analysis(
        self,
        wallet_address: str,
        token_address: str,
        chain: str,
        force_refresh: bool = False,
    ) -> Tuple[Dict[str, Any], bool]:
        """
        獲取錢包代幣分析結果
        
        Args:
            wallet_address: 錢包地址
            token_address: 代幣地址
            force_refresh: 是否強制刷新
            chain: 區塊鏈類型 (SOLANA, BSC等)
            
        Returns:
            (分析結果, 是否從緩存獲取)
        """
        # 修改緩存key，加入chain參數
        cache_key = f"wallet_token:{wallet_address}:{token_address}:{chain}"
        cached = await cache_service.get(cache_key)
        if cached and not force_refresh:
            logger.info("從緩存獲取")
            # 異步更新數據，不阻塞當前請求
            asyncio.create_task(self._update_analysis_data(wallet_address, token_address))
            return self._fill_default_fields(cached), True

        # 計算30天前的日期
        thirty_days_ago = datetime.now() - timedelta(days=30)
        
        with self.session_factory() as session:
            session.execute(text("SET search_path TO dex_query_v1;"))
            # 優化查詢：使用子查詢先過濾數據，再進行聚合
            subquery = select(TempTokenBuyData).where(
                and_(
                    TempTokenBuyData.wallet_address == wallet_address,
                    TempTokenBuyData.token_address == token_address,
                    TempTokenBuyData.chain == chain,
                    TempTokenBuyData.date >= thirty_days_ago
                )
            ).subquery()
            
            # 在過濾後的數據上進行聚合
            stmt = select(
                func.sum(subquery.c.total_amount).label('total_amount'),
                func.sum(subquery.c.total_cost).label('total_cost'),
                func.sum(subquery.c.historical_total_buy_amount).label('historical_total_buy_amount'),
                func.sum(subquery.c.historical_total_buy_cost).label('historical_total_buy_cost'),
                func.sum(subquery.c.historical_total_sell_amount).label('historical_total_sell_amount'),
                func.sum(subquery.c.historical_total_sell_value).label('historical_total_sell_value'),
                func.sum(subquery.c.realized_profit).label('realized_profit'),
                func.sum(subquery.c.total_buy_count).label('total_buy_count'),
                func.sum(subquery.c.total_sell_count).label('total_sell_count'),
                func.sum(subquery.c.total_holding_seconds).label('total_holding_seconds')
            )
            
            result = session.execute(stmt).first()
            
            if result:
                # 將查詢結果轉換為字典，確保所有數值都正確轉換
                data = {
                    "wallet_address": wallet_address,
                    "token_address": token_address,
                    "holding_amount": format_number_string(max(0, float(result.total_amount or 0))),
                    "total_buy_value": format_number_string(float(result.historical_total_buy_cost or 0)),
                    "total_buy_amount": format_number_string(float(result.historical_total_buy_amount or 0)),
                    "total_buy_count": format_number_as_integer_string(int(result.total_buy_count or 0)),
                    "total_sell_value": format_number_string(float(result.historical_total_sell_value or 0)),
                    "total_sell_amount": format_number_string(float(result.historical_total_sell_amount or 0)),
                    "total_sell_count": format_number_as_integer_string(int(result.total_sell_count or 0)),
                    "realized_pnl": format_number_string(float(result.realized_profit or 0)),
                    "total_holding_seconds": int(result.total_holding_seconds or 0)
                }
                
                # 寫入緩存
                await cache_service.set(cache_key, data, expiry=self.cache_ttl)
                return self._fill_default_fields(data), False
            
            # 如果沒有找到數據，返回空數據
            empty_data = {
                "wallet_address": wallet_address,
                "token_address": token_address,
                "holding_amount": "0",
                "total_buy_value": "0",
                "total_buy_amount": "0",
                "total_buy_count": "0",
                "total_sell_value": "0",
                "total_sell_amount": "0",
                "total_sell_count": "0",
                "realized_pnl": "0",
                "total_holding_seconds": 0
            }
            await cache_service.set(cache_key, empty_data, expiry=self.cache_ttl)
            return self._fill_default_fields(empty_data), False
    
    async def _save_to_db_and_cache(self, analysis_data):
        db_data = self._filter_db_fields(analysis_data)
        with self.session_factory() as session:
            stmt = select(TokenBuyData).where(
                TokenBuyData.wallet_address == analysis_data['wallet_address'],
                TokenBuyData.token_address == analysis_data['token_address']
            )
            result = session.execute(stmt).scalar_one_or_none()
            if result:
                # 更新
                session.execute(
                    update(TokenBuyData)
                    .where(TokenBuyData.wallet_address == analysis_data['wallet_address'],
                           TokenBuyData.token_address == analysis_data['token_address'])
                    .values(**db_data)
                )
            else:
                # 新增
                session.add(TokenBuyData(**db_data))
            session.commit()
        # 只保留指定欄位寫入緩存
        cache_data = {
            "wallet_address": analysis_data.get("wallet_address"),
            "token_address": analysis_data.get("token_address"),
            "holding_amount": format_number_string(analysis_data.get("holding_amount", 0)),
            "total_buy_value": format_number_string(analysis_data.get("total_buy_value", 0)),
            "total_buy_amount": format_number_string(analysis_data.get("total_buy_amount", 0)),
            "total_buy_count": format_number_as_integer_string(analysis_data.get("total_buy_count", 0)),
            "total_sell_value": format_number_string(analysis_data.get("total_sell_value", 0)),
            "total_sell_amount": format_number_string(analysis_data.get("total_sell_amount", 0)),
            "total_sell_count": format_number_as_integer_string(analysis_data.get("total_sell_count", 0)),
            "realized_pnl": format_number_string(analysis_data.get("realized_pnl", 0)),
            "total_holding_seconds": analysis_data.get("total_holding_seconds", 0)
        }
        await cache_service.set(
            f"wallet_token:{analysis_data['wallet_address']}:{analysis_data['token_address']}",
            cache_data, expiry=self.cache_ttl
        )
    
    async def _analyze_transactions(
        self,
        wallet_address: str,
        token_address: str,
        chain: str
    ) -> Dict[str, Any]:
        """分析交易記錄"""
        logger.info(f"開始分析錢包 {wallet_address} 代幣 {token_address} 的交易記錄")
        # 計算 from_time/to_time（預設查詢過去 30 天）
        to_time = int(time.time())
        from_time = to_time - 30 * 24 * 60 * 60
        # 初始化結果
        result = {
            "wallet_address": wallet_address,
            "token_address": token_address,
            "chain": chain,
            # 下面這些欄位是 TokenAnalysisResponse 需要的
            "total_buy_amount": 0.0,
            "total_buy_value": 0.0,
            "avg_buy_price": 0.0,
            "total_sell_amount": 0.0,
            "total_sell_value": 0.0,
            "avg_sell_price": 0.0,
            "holding_amount": 0.0,
            "holding_cost": 0.0,
            "realized_pnl": 0.0,
            "is_runner": False,
            "buy_transactions": [],
            "sell_transactions": [],
            "total_holding_seconds": 0,
            "buy_count": 0,
            "sell_count": 0,
            "pnl_percent": 0.0,
            "last_updated": int(datetime.now(timezone.utc).timestamp()),
        }
        all_transactions = []
        page = 1
        page_size = 100
        while True:
            logger.info(f"正在獲取第 {page} 頁交易記錄...")
            transactions = await solscan_client.get_wallet_token_transactions(
                wallet_address=wallet_address,
                token_address=token_address,
                page=page,
                page_size=page_size,
                from_time=from_time,
                to_time=to_time
            )
            if not transactions:
                logger.info(f"第 {page} 頁沒有更多交易記錄，結束獲取。")
                break
            all_transactions.extend(transactions)
            logger.info(f"- 獲取到 {len(transactions)} 條交易記錄")
            if len(transactions) < page_size:
                logger.info(f"第 {page} 頁已是最後一頁，提前結束。")
                break
            page += 1
            await asyncio.sleep(0.1)
        if not all_transactions:
            logger.info("未找到任何交易記錄")
            return result
        logger.info(f"總共找到 {len(all_transactions)} 條交易記錄")
        
        # 分析交易記錄
        total_buy_amount = Decimal(0)
        total_buy_value = Decimal(0)
        total_sell_amount = Decimal(0)
        total_sell_value = Decimal(0)
        holding_lots = []
        last_transaction_time = 0
        position_opened_at = None
        last_active_position_closed_at = None
        buy_count = 0
        sell_count = 0
        total_holding_seconds = 0
        buy_transactions = []
        sell_transactions = []
        
        # 嚴謹 FIFO 已實現損益計算
        fifo_lots = []  # 每一批買入: {'amount': 數量, 'cost': 總成本}
        realized_pnl = Decimal(0)
        for transaction in all_transactions:
            block_time = transaction.get('block_time')
            routers = transaction.get("routers", {})
            token2 = routers.get("token2", "")
            token1 = routers.get("token1", "")
            if token2 == token_address:
                # 買入
                amount = Decimal(routers.get("amount2", 0)) / Decimal(10 ** routers.get("token2_decimals", 0))
                value = Decimal(transaction.get("value", 0))
                fifo_lots.append({'amount': amount, 'cost': value})
                total_buy_amount += amount
                total_buy_value += value
                buy_count += 1
                buy_transactions.append(transaction)
                if block_time:
                    if not position_opened_at or block_time < position_opened_at:
                        position_opened_at = block_time
                    holding_lots.append({'amount': amount, 'start_time': block_time})
            elif token1 == token_address:
                # 賣出
                sell_amount = Decimal(routers.get("amount1", 0)) / Decimal(10 ** routers.get("token1_decimals", 0))
                sell_value = Decimal(transaction.get("value", 0))
                remain = sell_amount
                cost_sum = Decimal(0)
                # FIFO 扣除買入批次
                while remain > 0 and fifo_lots:
                    lot = fifo_lots[0]
                    if remain >= lot['amount']:
                        cost_sum += lot['cost']
                        remain -= lot['amount']
                        fifo_lots.pop(0)
                    else:
                        cost_sum += lot['cost'] * (remain / lot['amount'])
                        lot['amount'] -= remain
                        lot['cost'] -= lot['cost'] * (remain / (lot['amount'] + remain))
                        remain = Decimal(0)
                realized_pnl += sell_value - cost_sum
                total_sell_amount += sell_amount
                total_sell_value += sell_value
                sell_count += 1
                sell_transactions.append(transaction)
                sell_left = sell_amount
                while sell_left > 0 and holding_lots:
                    lot = holding_lots[0]
                    if sell_left >= lot['amount']:
                        if block_time:
                            last_active_position_closed_at = block_time
                        sell_left -= lot['amount']
                        holding_lots.pop(0)
                    else:
                        lot['amount'] -= sell_left
                        sell_left = Decimal(0)
        
        holding_amount = total_buy_amount - total_sell_amount
        avg_buy_price = total_buy_value / total_buy_amount if total_buy_amount > 0 else Decimal(0)
        avg_sell_price = total_sell_value / total_sell_amount if total_sell_amount > 0 else Decimal(0)
        total_cost = (total_buy_value / total_buy_amount) * holding_amount if holding_amount > 0 else Decimal(0)
        pnl_percent = (realized_pnl / total_buy_value * 100) if total_buy_value > 0 else 0.0
        # 持有時間計算
        dt_open = _to_datetime(position_opened_at)
        dt_close = _to_datetime(last_active_position_closed_at)
        if dt_open and dt_close:
            total_holding_seconds = int((dt_close - dt_open).total_seconds())
        elif dt_open and last_transaction_time:
            dt_last = _to_datetime(last_transaction_time)
            if dt_last:
                total_holding_seconds = int((dt_last - dt_open).total_seconds())
            else:
                total_holding_seconds = 0
        else:
            total_holding_seconds = 0
        # 更新結果
        result.update({
            "total_buy_amount": float(total_buy_amount),
            "total_buy_value": float(total_buy_value),
            "avg_buy_price": float(avg_buy_price),
            "total_sell_amount": float(total_sell_amount),
            "total_sell_value": float(total_sell_value),
            "avg_sell_price": float(avg_sell_price),
            "holding_amount": float(holding_amount),
            "holding_cost": float(total_cost),
            "realized_pnl": float(realized_pnl),
            "is_runner": False,  # 你可以根據邏輯決定
            "buy_transactions": buy_transactions,
            "sell_transactions": sell_transactions,
            "total_holding_seconds": total_holding_seconds,
            "buy_count": int(buy_count),
            "sell_count": int(sell_count),
            "pnl_percent": float(pnl_percent),
            "last_updated": int(datetime.now(timezone.utc).timestamp()),
            "position_opened_at": _to_datetime(position_opened_at),
            "last_active_position_closed_at": _to_datetime(last_active_position_closed_at),
        })

        # 1. 找出所有 buy/sell 交易，並依照 block_time 排序
        buy_txs = sorted([tx for tx in buy_transactions], key=lambda x: x.get("block_time", 0))
        sell_txs = sorted([tx for tx in sell_transactions], key=lambda x: x.get("block_time", 0))

        # 2. 計算清倉點
        holding_amount = float(total_buy_amount - total_sell_amount)
        if holding_amount <= 0:
            total_amount = 0
            total_cost = 0
            position_opened_at = None
        else:
            # 只保留目前持有的那一批
            # 這裡用 FIFO 法，模擬賣出後剩下的買入批次
            lots = []
            for tx in buy_txs:
                amount = float(tx["routers"].get("amount2", 0)) / (10 ** int(tx["routers"].get("token2_decimals", 0)))
                lots.append({"amount": amount, "block_time": tx["block_time"], "cost": float(tx["value"])})
            sell_left = float(total_sell_amount)
            while sell_left > 0 and lots:
                lot = lots[0]
                if sell_left >= lot["amount"]:
                    sell_left -= lot["amount"]
                    lots.pop(0)
                else:
                    lot["amount"] -= sell_left
                    sell_left = 0
            # 剩下的 lots 就是目前持有的
            total_amount = sum(lot["amount"] for lot in lots)
            total_cost = sum(lot["cost"] for lot in lots)
            position_opened_at = lots[0]["block_time"] if lots else None

        # 3. 其他欄位
        historical_total_buy_amount = float(total_buy_amount)
        historical_total_buy_cost = float(total_buy_value)
        historical_total_sell_amount = float(total_sell_amount)
        historical_total_sell_value = float(total_sell_value)
        last_active_position_closed_at = sell_txs[-1]["block_time"] if sell_txs else None
        historical_avg_buy_price = float(avg_buy_price)
        historical_avg_sell_price = float(avg_sell_price)
        last_transaction_time = max([tx.get("block_time", 0) for tx in all_transactions]) if all_transactions else 0
        realized_profit = float(realized_pnl)
        realized_profit_percentage = (realized_pnl / Decimal(str(historical_total_buy_cost)) * 100) if historical_total_buy_cost > 0 else 0

        # 4. 更新 result
        result.update({
            "total_amount": total_amount,
            "total_cost": total_cost,
            "position_opened_at": _to_datetime(position_opened_at),
            "historical_total_buy_amount": historical_total_buy_amount,
            "historical_total_buy_cost": historical_total_buy_cost,
            "historical_total_sell_amount": historical_total_sell_amount,
            "historical_total_sell_value": historical_total_sell_value,
            "last_active_position_closed_at": _to_datetime(last_active_position_closed_at),
            "historical_avg_buy_price": historical_avg_buy_price,
            "historical_avg_sell_price": historical_avg_sell_price,
            "last_transaction_time": last_transaction_time,
            "realized_profit": realized_profit,
            "realized_profit_percentage": float(realized_profit_percentage),
            "total_buy_count": int(buy_count),
            "total_sell_count": int(sell_count),
        })
        return result
    
    def _convert_to_dict(self, data: TokenBuyData) -> Dict[str, Any]:
        """將數據庫模型轉換為字典"""
        return {
            "wallet_address": data.wallet_address,
            "token_address": data.token_address,
            "total_amount": data.total_amount,
            "total_cost": data.total_cost,
            "avg_buy_price": data.avg_buy_price,
            "position_opened_at": data.position_opened_at,
            "historical_total_buy_amount": data.historical_total_buy_amount,
            "historical_total_buy_cost": data.historical_total_buy_cost,
            "historical_total_sell_amount": data.historical_total_sell_amount,
            "historical_total_sell_value": data.historical_total_sell_value,
            "historical_avg_buy_price": data.historical_avg_buy_price,
            "historical_avg_sell_price": data.historical_avg_sell_price,
            "last_active_position_closed_at": data.last_active_position_closed_at,
            "last_transaction_time": data.last_transaction_time,
            "realized_profit": data.realized_profit,
            "chain": data.chain,
            "updated_at": data.updated_at.isoformat(),
            "total_buy_count": getattr(data, "total_buy_count", 0),
            "total_sell_count": getattr(data, "total_sell_count", 0),
        }

def _to_datetime(ts):
    if ts is None:
        return None
    if isinstance(ts, int) or isinstance(ts, float):
        return datetime.utcfromtimestamp(ts)
    if isinstance(ts, str) and ts.isdigit():
        return datetime.utcfromtimestamp(int(ts))
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None
    return ts  # already datetime

# 創建單例實例
wallet_token_analyzer = WalletTokenAnalyzer() 