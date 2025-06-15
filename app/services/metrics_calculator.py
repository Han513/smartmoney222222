import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict

logger = logging.getLogger(__name__)

class MetricsCalculator:
    """
    指標計算器 - 計算錢包分析指標
    """
    
    def __init__(self):
        # 基礎代幣定義 (SOL 和穩定幣)
        self.base_tokens = {
            "So11111111111111111111111111111111111111112": "SOL",  # SOL
            "So11111111111111111111111111111111111111111": "SOL",  # 另一種 SOL 表示
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC", # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT"  # USDT
        }
    
    def calculate_token_stats(self, activities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        計算代幣交易統計數據
        
        Args:
            activities: 活動列表
            
        Returns:
            代幣統計數據
        """
        logger.info(f"計算 {len(activities)} 筆活動的代幣統計數據")
        
        token_data = defaultdict(lambda: {
            "total_transactions": 0,
            "buy_count": 0,
            "sell_count": 0,
            "transfer_in_count": 0, 
            "transfer_out_count": 0,
            "total_buy_amount": 0.0,
            "total_sell_amount": 0.0,
            "total_buy_cost": 0.0,
            "total_sell_value": 0.0,
            "balance": 0.0,
            "avg_buy_price": 0.0,
            "avg_sell_price": 0.0,
            "last_transaction_time": 0,
            "symbol": None,
            "marketcap": 0.0
        })
        
        # 處理每筆活動
        for activity in activities:
            activity_type = activity.get("activity_type", "")
            
            if activity_type == "token_swap":
                from_token = activity.get("from_token", {})
                to_token = activity.get("to_token", {})
                
                if not from_token or not to_token:
                    continue
                    
                from_token_address = from_token.get("address", "")
                to_token_address = to_token.get("address", "")
                from_amount = from_token.get("amount", 0)
                to_amount = to_token.get("amount", 0)
                timestamp = activity.get("block_time", 0)
                
                # 判斷買入還是賣出
                is_buy = self._is_token_buy(from_token_address, to_token_address)
                
                if is_buy:
                    # 買入 to_token
                    token_address = to_token_address
                    amount = float(to_amount)  # 確保是 float 類型
                    cost = float(from_amount)  # 確保是 float 類型
                    
                    token_data[token_address]["buy_count"] += 1
                    token_data[token_address]["total_buy_amount"] += amount
                    token_data[token_address]["total_buy_cost"] += cost
                    token_data[token_address]["balance"] += amount
                else:
                    # 賣出 from_token
                    token_address = from_token_address
                    amount = float(from_amount)  # 確保是 float 類型
                    value = float(to_amount)    # 確保是 float 類型
                    
                    token_data[token_address]["sell_count"] += 1
                    token_data[token_address]["total_sell_amount"] += amount
                    token_data[token_address]["total_sell_value"] += value
                    token_data[token_address]["balance"] -= amount
                
                token_data[token_address]["total_transactions"] += 1
                token_data[token_address]["last_transaction_time"] = max(
                    token_data[token_address]["last_transaction_time"], 
                    timestamp
                )
                
            elif activity_type == "token_transfer":
                token_address = activity.get("token_address", "")
                amount = float(activity.get("amount", 0))  # 確保是 float 類型
                is_incoming = activity.get("is_incoming", False)
                timestamp = activity.get("block_time", 0)
                
                if not token_address:
                    continue
                
                if is_incoming:
                    token_data[token_address]["transfer_in_count"] += 1
                    token_data[token_address]["balance"] += amount
                else:
                    token_data[token_address]["transfer_out_count"] += 1
                    token_data[token_address]["balance"] -= amount
                
                token_data[token_address]["total_transactions"] += 1
                token_data[token_address]["last_transaction_time"] = max(
                    token_data[token_address]["last_transaction_time"], 
                    timestamp
                )
        
        # 計算平均價格
        for token_address, data in token_data.items():
            if data["total_buy_amount"] > 0:
                data["avg_buy_price"] = data["total_buy_cost"] / data["total_buy_amount"]
                
            if data["total_sell_amount"] > 0:
                data["avg_sell_price"] = data["total_sell_value"] / data["total_sell_amount"]
            
            # 設定代幣符號
            if token_address in self.base_tokens:
                data["symbol"] = self.base_tokens[token_address]
            else:
                data["symbol"] = None
        
        # 計算總代幣價值 (需要實時價格來計算真實價值)
        # 這裡僅作為示例，返回一個估計值
        total_value = sum(data["balance"] * data["avg_buy_price"] for data in token_data.values() if data["avg_buy_price"] > 0)
        
        return {
            "token_count": len(token_data),
            "total_value": total_value,
            "tokens": dict(token_data)
        }
    
    def calculate_profit_metrics(
        self, 
        activities: List[Dict[str, Any]], 
        time_ranges: Dict[str, int]
    ) -> Dict[str, Any]:
        """
        計算收益指標
        
        Args:
            activities: 活動列表
            time_ranges: 時間範圍字典 (例如 {"1d": 現在 - 1天, "7d": 現在 - 7天})
            
        Returns:
            收益指標
        """
        logger.info(f"計算 {len(activities)} 筆活動的收益指標")
        
        metrics = {}
        current_time = int(datetime.now().timestamp())
        
        # 按時間範圍計算每個代幣的收益指標
        for period, start_time in time_ranges.items():
            # 篩選時間範圍內的活動
            period_activities = [a for a in activities if a.get("block_time", 0) >= start_time]
            logger.info(f"{period} 時間範圍內有 {len(period_activities)} 筆活動")
            
            # 統計每個代幣的買入、賣出資訊
            token_balance = defaultdict(float)  # 目前餘額
            token_cost = defaultdict(float)     # 總成本
            token_realized_profit = defaultdict(float)  # 已實現收益
            
            # 處理每個活動
            for activity in period_activities:
                activity_type = activity.get("activity_type", "")
                
                if activity_type == "token_swap":
                    from_token = activity.get("from_token", {})
                    to_token = activity.get("to_token", {})
                    
                    if not from_token or not to_token:
                        continue
                        
                    from_token_address = from_token.get("address", "")
                    to_token_address = to_token.get("address", "")
                    from_amount = from_token.get("amount", 0)
                    to_amount = to_token.get("amount", 0)
                    
                    # 判斷買入還是賣出
                    is_buy = self._is_token_buy(from_token_address, to_token_address)
                    
                    if is_buy:
                        # 買入 to_token
                        token_address = to_token_address
                        amount = float(to_amount)  # 確保是 float 類型
                        cost = float(from_amount)  # 確保是 float 類型
                        
                        token_balance[token_address] += amount
                        token_cost[token_address] += cost
                    else:
                        # 賣出 from_token
                        token_address = from_token_address
                        amount = float(from_amount)  # 確保是 float 類型
                        value = float(to_amount)    # 確保是 float 類型
                        
                        # 計算賣出的成本和收益
                        if token_balance[token_address] > 0:
                            avg_cost = token_cost[token_address] / token_balance[token_address]
                            sell_cost = avg_cost * amount
                            profit = value - sell_cost
                            
                            # 更新餘額和成本
                            old_balance = token_balance[token_address]
                            new_balance = max(0, old_balance - amount)
                            
                            if new_balance > 0:
                                # 按比例減少成本
                                token_cost[token_address] = (token_cost[token_address] / old_balance) * new_balance
                            else:
                                # 全部賣出
                                token_cost[token_address] = 0
                                
                            token_balance[token_address] = new_balance
                            token_realized_profit[token_address] += profit
                
                elif activity_type == "token_transfer":
                    token_address = activity.get("token_address", "")
                    amount = float(activity.get("amount", 0))  # 確保是 float 類型
                    is_incoming = activity.get("is_incoming", False)
                    
                    if not token_address:
                        continue
                    
                    if is_incoming:
                        # 收到代幣，需要估算成本 (這裡假設成本為0，實際應根據獲取方式計算)
                        token_balance[token_address] += amount
                        # 不計入成本，通常為空投或轉帳
                    else:
                        # 轉出代幣
                        if token_balance[token_address] > 0:
                            old_balance = token_balance[token_address]
                            new_balance = max(0, old_balance - amount)
                            
                            if new_balance > 0:
                                # 按比例減少成本
                                token_cost[token_address] = (token_cost[token_address] / old_balance) * new_balance
                            else:
                                # 全部轉出
                                token_cost[token_address] = 0
                                
                            token_balance[token_address] = new_balance
            
            # 計算平均成本
            avg_cost = {}
            for token, balance in token_balance.items():
                if balance > 0:
                    avg_cost[token] = token_cost[token] / balance
                else:
                    avg_cost[token] = 0
            
            # 計算總收益 (需要實時代幣價格)
            # 這裡僅作為示例返回已實現收益
            total_realized_profit = sum(token_realized_profit.values())
            
            # 保存結果
            metrics[f"{period}_token_balance"] = dict(token_balance)
            metrics[f"{period}_token_cost"] = dict(token_cost)
            metrics[f"{period}_avg_cost"] = dict(avg_cost)
            metrics[f"{period}_realized_profit"] = total_realized_profit
            
            # 計算收益率 (假設初始投資為總成本)
            total_cost = sum(token_cost.values())
            if total_cost > 0:
                profit_percentage = (total_realized_profit / total_cost) * 100
            else:
                profit_percentage = 0
                
            metrics[f"{period}_profit"] = total_realized_profit
            metrics[f"{period}_profit_percentage"] = profit_percentage
        
        # 計算所有時間的收益
        all_time_activities = activities
        token_balance_all = defaultdict(float)
        token_cost_all = defaultdict(float)
        token_realized_profit_all = defaultdict(float)
        
        # 處理所有活動
        for activity in all_time_activities:
            activity_type = activity.get("activity_type", "")
            
            if activity_type == "token_swap":
                from_token = activity.get("from_token", {})
                to_token = activity.get("to_token", {})
                
                if not from_token or not to_token:
                    continue
                    
                from_token_address = from_token.get("address", "")
                to_token_address = to_token.get("address", "")
                from_amount = from_token.get("amount", 0)
                to_amount = to_token.get("amount", 0)
                
                # 判斷買入還是賣出
                is_buy = self._is_token_buy(from_token_address, to_token_address)
                
                if is_buy:
                    # 買入 to_token
                    token_address = to_token_address
                    amount = float(to_amount)  # 確保是 float 類型
                    cost = float(from_amount)  # 確保是 float 類型
                    
                    token_balance_all[token_address] += amount
                    token_cost_all[token_address] += cost
                else:
                    # 賣出 from_token
                    token_address = from_token_address
                    amount = float(from_amount)  # 確保是 float 類型
                    value = float(to_amount)    # 確保是 float 類型
                    
                    # 計算賣出的成本和收益
                    if token_balance_all[token_address] > 0:
                        avg_cost = token_cost_all[token_address] / token_balance_all[token_address]
                        sell_cost = avg_cost * amount
                        profit = value - sell_cost
                        
                        # 更新餘額和成本
                        old_balance = token_balance_all[token_address]
                        new_balance = max(0, old_balance - amount)
                        
                        if new_balance > 0:
                            # 按比例減少成本
                            token_cost_all[token_address] = (token_cost_all[token_address] / old_balance) * new_balance
                        else:
                            # 全部賣出
                            token_cost_all[token_address] = 0
                            
                        token_balance_all[token_address] = new_balance
                        token_realized_profit_all[token_address] += profit
        
        # 計算總收益和勝率
        total_realized_profit_all = sum(token_realized_profit_all.values())
        total_cost_all = sum(token_cost_all.values())
        
        if total_cost_all > 0:
            profit_percentage_all = (total_realized_profit_all / total_cost_all) * 100
        else:
            profit_percentage_all = 0
            
        # 計算勝率 - 有收益的代幣數量除以總交易代幣數量
        win_tokens = sum(1 for profit in token_realized_profit_all.values() if profit > 0)
        total_traded_tokens = len(token_realized_profit_all)
        
        win_rate = win_tokens / total_traded_tokens if total_traded_tokens > 0 else 0
        
        metrics["all_time_profit"] = total_realized_profit_all
        metrics["all_time_profit_percentage"] = profit_percentage_all
        metrics["trade_win_count"] = win_tokens
        metrics["trade_lose_count"] = total_traded_tokens - win_tokens
        metrics["trade_win_rate"] = win_rate
        
        return metrics
    
    def calculate_average_prices(
        self, 
        activities: List[Dict[str, Any]], 
        time_range: int
    ) -> Dict[str, Any]:
        """
        計算平均買入/賣出價格
        
        Args:
            activities: 活動列表
            time_range: 時間範圍（天）
            
        Returns:
            平均價格指標
        """
        logger.info(f"計算 {len(activities)} 筆活動在 {time_range} 天內的平均價格")
        
        metrics = {}
        now = int(datetime.now().timestamp())
        start_time = now - (time_range * 86400)  # 轉換天數為秒數
        
        # 篩選時間範圍內的活動
        filtered_activities = [a for a in activities if a.get("block_time", 0) >= start_time]
        logger.info(f"篩選後有 {len(filtered_activities)} 筆活動")
        
        buy_prices = defaultdict(list)   # 買入價格列表
        sell_prices = defaultdict(list)  # 賣出價格列表
        buy_volumes = defaultdict(float) # 買入數量
        sell_volumes = defaultdict(float) # 賣出數量
        
        # 處理每個活動
        for activity in filtered_activities:
            activity_type = activity.get("activity_type", "")
            
            if activity_type == "token_swap":
                from_token = activity.get("from_token", {})
                to_token = activity.get("to_token", {})
                
                if not from_token or not to_token:
                    continue
                    
                from_token_address = from_token.get("address", "")
                to_token_address = to_token.get("address", "")
                from_amount = from_token.get("amount", 0)
                to_amount = to_token.get("amount", 0)
                
                # 避免除零錯誤
                if from_amount <= 0 or to_amount <= 0:
                    continue
                
                # 判斷買入還是賣出
                is_buy = self._is_token_buy(from_token_address, to_token_address)
                
                if is_buy:
                    # 買入 to_token
                    token_address = to_token_address
                    amount = to_amount
                    price = from_amount / to_amount  # 單位成本
                    
                    buy_prices[token_address].append(price)
                    buy_volumes[token_address] += amount
                else:
                    # 賣出 from_token
                    token_address = from_token_address
                    amount = from_amount
                    price = to_amount / from_amount  # 單位價值
                    
                    sell_prices[token_address].append(price)
                    sell_volumes[token_address] += amount
        
        # 計算加權平均價格
        avg_buy_prices = {}
        avg_sell_prices = {}
        
        for token, prices in buy_prices.items():
            if len(prices) > 0:
                avg_buy_prices[token] = sum(prices) / len(prices)
            else:
                avg_buy_prices[token] = 0
        
        for token, prices in sell_prices.items():
            if len(prices) > 0:
                avg_sell_prices[token] = sum(prices) / len(prices)
            else:
                avg_sell_prices[token] = 0
        
        # 保存結果
        metrics[f"{time_range}d_avg_buy_prices"] = avg_buy_prices
        metrics[f"{time_range}d_avg_sell_prices"] = avg_sell_prices
        metrics[f"{time_range}d_buy_volumes"] = dict(buy_volumes)
        metrics[f"{time_range}d_sell_volumes"] = dict(sell_volumes)
        
        return metrics
    
    def _is_token_buy(self, from_token: str, to_token: str) -> bool:
        """
        判斷交易是買入還是賣出
        
        Args:
            from_token: 源代幣地址
            to_token: 目標代幣地址
            
        Returns:
            如果是買入交易則返回True，否則返回False
        """
        stablecoins = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        ]
        sol_addresses = [
            "So11111111111111111111111111111111111111112",  # SOL
            "So11111111111111111111111111111111111111111"   # 另一種 SOL 地址表示
        ]
        
        # 檢查代幣地址是否為基礎代幣(SOL或穩定幣)
        from_is_base = from_token in stablecoins or from_token in sol_addresses
        to_is_base = to_token in stablecoins or to_token in sol_addresses
        
        # 邏輯1: 從SOL換成穩定幣 -> 賣出(SOL)
        if from_token in sol_addresses and to_token in stablecoins:
            return False
        
        # 邏輯2: 從穩定幣換成SOL -> 買入(SOL)
        if from_token in stablecoins and to_token in sol_addresses:
            return True
        
        # 邏輯3: 從基礎代幣換成其他代幣 -> 買入
        if from_is_base and not to_is_base:
            return True
        
        # 邏輯4: 從其他代幣換成基礎代幣 -> 賣出
        if not from_is_base and to_is_base:
            return False
        
        # 邏輯5: 都不是基礎代幣 -> 視為買入目標代幣(token2)
        return True
    
    def calculate_win_rate(self, token_stats: Dict[str, Dict]) -> Dict[str, Any]:
        """
        計算交易勝率
        
        Args:
            token_stats: 代幣統計數據
            
        Returns:
            勝率指標
        """
        total_tokens = len(token_stats)
        if total_tokens == 0:
            return {
                "total_tokens_traded": 0,
                "winning_tokens": 0,
                "win_rate": 0,
                "win_rate_percentage": 0
            }
        
        winning_tokens = 0
        for token_data in token_stats.values():
            # 計算該代幣的總成本和當前價值
            total_amount = token_data.get('balance', 0)
            total_cost = token_data.get('total_buy_cost', 0)
            avg_buy_price = token_data.get('avg_buy_price', 0)
            
            if total_amount > 0 and total_cost > 0:
                # 估算當前價值 (理想情況下應使用實時價格)
                current_value = total_amount * avg_buy_price
                
                # 計算盈虧
                if current_value > total_cost:
                    winning_tokens += 1
        
        win_rate = winning_tokens / total_tokens
        
        return {
            "total_tokens_traded": total_tokens,
            "winning_tokens": winning_tokens,
            "win_rate": win_rate,
            "win_rate_percentage": win_rate * 100
        }