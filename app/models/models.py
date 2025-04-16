from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, BIGINT, Index, Text, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime, timezone, timedelta

Base = declarative_base()

def get_utc8_time():
    """獲取UTC+8時間"""
    return datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)

class WalletSummary(Base):
    __tablename__ = 'wallet'
    __table_args__ = {'schema': 'solana'}

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    id = Column(Integer, primary_key=True, comment='ID')
    address = Column(String(512), nullable=False, unique=True, comment='錢包地址')
    balance = Column(Float, nullable=True, comment='錢包餘額')
    balance_USD = Column(Float, nullable=True, comment='錢包餘額 (USD)')
    chain = Column(String(50), nullable=False, comment='區塊鏈類型')
    tag = Column(String(50), nullable=True, comment='標籤')
    twitter_name = Column(String(50), nullable=True, comment='X名稱')
    twitter_username = Column(String(50), nullable=True, comment='X用戶名')
    is_smart_wallet = Column(Boolean, nullable=True, comment='是否為聰明錢包')
    wallet_type = Column(Integer, nullable=True, comment='0:一般聰明錢，1:pump聰明錢，2:moonshot聰明錢')
    asset_multiple = Column(Float, nullable=True, comment='資產翻倍數(到小數第1位)')
    token_list = Column(String(512), nullable=True, comment='用户最近交易的三种代币信息')

    # 交易數據
    avg_cost_30d = Column(Float, nullable=True, comment='30日平均成本')
    avg_cost_7d = Column(Float, nullable=True, comment='7日平均成本')
    avg_cost_1d = Column(Float, nullable=True, comment='1日平均成本')
    total_transaction_num_30d = Column(Integer, nullable=True, comment='30日總交易次數')
    total_transaction_num_7d = Column(Integer, nullable=True, comment='7日總交易次數')
    total_transaction_num_1d = Column(Integer, nullable=True, comment='1日總交易次數')
    buy_num_30d = Column(Integer, nullable=True, comment='30日買入次數')
    buy_num_7d = Column(Integer, nullable=True, comment='7日買入次數')
    buy_num_1d = Column(Integer, nullable=True, comment='1日買入次數')
    sell_num_30d = Column(Integer, nullable=True, comment='30日賣出次數')
    sell_num_7d = Column(Integer, nullable=True, comment='7日賣出次數')
    sell_num_1d = Column(Integer, nullable=True, comment='1日賣出次數')
    win_rate_30d = Column(Float, nullable=True, comment='30日勝率')
    win_rate_7d = Column(Float, nullable=True, comment='7日勝率')
    win_rate_1d = Column(Float, nullable=True, comment='1日勝率')

    # 盈虧數據
    pnl_30d = Column(Float, nullable=True, comment='30日盈虧')
    pnl_7d = Column(Float, nullable=True, comment='7日盈虧')
    pnl_1d = Column(Float, nullable=True, comment='1日盈虧')
    pnl_percentage_30d = Column(Float, nullable=True, comment='30日盈虧百分比')
    pnl_percentage_7d = Column(Float, nullable=True, comment='7日盈虧百分比')
    pnl_percentage_1d = Column(Float, nullable=True, comment='1日盈虧百分比')
    pnl_pic_30d = Column(String(1024), nullable=True, comment='30日每日盈虧圖')
    pnl_pic_7d = Column(String(1024), nullable=True, comment='7日每日盈虧圖')
    pnl_pic_1d = Column(String(1024), nullable=True, comment='1日每日盈虧圖')
    unrealized_profit_30d = Column(Float, nullable=True, comment='30日未實現利潤')
    unrealized_profit_7d = Column(Float, nullable=True, comment='7日未實現利潤')
    unrealized_profit_1d = Column(Float, nullable=True, comment='1日未實現利潤')
    total_cost_30d = Column(Float, nullable=True, comment='30日總成本')
    total_cost_7d = Column(Float, nullable=True, comment='7日總成本')
    total_cost_1d = Column(Float, nullable=True, comment='1日總成本')
    avg_realized_profit_30d = Column(Float, nullable=True, comment='30日平均已實現利潤')
    avg_realized_profit_7d = Column(Float, nullable=True, comment='7日平均已實現利潤')
    avg_realized_profit_1d = Column(Float, nullable=True, comment='1日平均已實現利潤')

    # 收益分布數據
    distribution_gt500_30d = Column(Integer, nullable=True, comment='30日收益分布 >500% 的次數')
    distribution_200to500_30d = Column(Integer, nullable=True, comment='30日收益分布 200%-500% 的次數')
    distribution_0to200_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-200% 的次數')
    distribution_0to50_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-50% 的次數')
    distribution_lt50_30d = Column(Integer, nullable=True, comment='30日收益分布 <50% 的次數')
    distribution_gt500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 >500% 的比例')
    distribution_200to500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 <50% 的比例')

    distribution_gt500_7d = Column(Integer, nullable=True, comment='7日收益分布 >500% 的次數')
    distribution_200to500_7d = Column(Integer, nullable=True, comment='7日收益分布 200%-500% 的次數')
    distribution_0to200_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-200% 的次數')
    distribution_0to50_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-50% 的次數')
    distribution_lt50_7d = Column(Integer, nullable=True, comment='7日收益分布 <50% 的次數')
    distribution_gt500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 >500% 的比例')
    distribution_200to500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 <50% 的比例')

    # 更新時間和最後交易時間
    update_time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    last_transaction_time = Column(BIGINT, nullable=True, comment='最後活躍時間')
    is_active = Column(Boolean, nullable=True, comment='是否還是聰明錢')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Holding(Base):
    __tablename__ = 'wallet_holding'
    __table_args__ = {'schema': 'solana'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(255), nullable=False)  # 添加长度限制
    token_address = Column(String(255), nullable=False)  # 添加长度限制
    token_icon = Column(String(255), nullable=True)  # 添加长度限制
    token_name = Column(String(255), nullable=True)  # 添加长度限制
    chain = Column(String(50), nullable=False, default='Unknown')  # 添加长度限制
    amount = Column(Float, nullable=False, default=0.0)
    value = Column(Float, nullable=False, default=0.0)
    value_USDT = Column(Float, nullable=False, default=0.0)
    unrealized_profits = Column(Float, nullable=False, default=0.0)
    pnl = Column(Float, nullable=False, default=0.0)
    pnl_percentage = Column(Float, nullable=False, default=0.0)
    avg_price = Column(Float, nullable=False, default=0.0)
    marketcap = Column(Float, nullable=False, default=0.0)
    is_cleared = Column(Boolean, nullable=False, default=False)
    cumulative_cost = Column(Float, nullable=False, default=0.0)
    cumulative_profit = Column(Float, nullable=False, default=0.0)
    last_transaction_time = Column(BIGINT, nullable=True, comment='最後活躍時間')
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Transaction(Base):
    """交易記錄表，用於存儲所有交易"""
    __tablename__ = 'wallet_transaction'
    __table_args__ = {'schema': 'solana'}
    
    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="聰明錢錢包地址")
    wallet_balance = Column(Float, nullable=False, comment="聰明錢錢包餘額(SOL+U)")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    token_icon = Column(Text, nullable=True, comment="代幣圖片網址")
    token_name = Column(String(100), nullable=True, comment="代幣名稱")
    price = Column(Float, nullable=True, comment="價格")
    amount = Column(Float, nullable=False, comment="數量")
    marketcap = Column(Float, nullable=True, comment="市值")
    value = Column(Float, nullable=True, comment="價值")
    holding_percentage = Column(Float, nullable=True, comment="倉位百分比")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    realized_profit = Column(Float, nullable=True, comment="已實現利潤")
    realized_profit_percentage = Column(Float, nullable=True, comment="已實現利潤百分比")
    transaction_type = Column(String(10), nullable=False, comment="事件 (buy, sell)")
    transaction_time = Column(BIGINT, nullable=False, comment="交易時間")
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    signature = Column(String(100), nullable=False, unique=True, comment="交易簽名")
    from_token_address = Column(String(100), nullable=False, comment="from token 地址")
    from_token_symbol = Column(String(100), nullable=True, comment="from token 名稱")
    from_token_amount = Column(Float, nullable=False, comment="from token 數量")
    dest_token_address = Column(String(100), nullable=False, comment="dest token 地址")
    dest_token_symbol = Column(String(100), nullable=True, comment="dest token 名稱")
    dest_token_amount = Column(Float, nullable=False, comment="dest token 數量")
    
    def __repr__(self):
        return f"<Transaction {self.signature[:10]}...>"

class TokenBuyData(Base):
    """代幣購買數據，用於追踪錢包持有的代幣購買成本"""
    __tablename__ = 'wallet_buy_data'
    __table_args__ = {'schema': 'solana'}
    
    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="錢包地址")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    total_amount = Column(Float, nullable=False, default=0.0, comment="當前持有代幣數量")
    total_cost = Column(Float, nullable=False, default=0.0, comment="當前持倉總成本")
    avg_buy_price = Column(Float, nullable=False, default=0.0, comment="當前持倉平均買入價格")
    position_opened_at = Column(DateTime, nullable=True, comment="當前倉位開始時間")
    historical_total_buy_amount = Column(Float, nullable=False, default=0.0, comment="歷史總買入數量")
    historical_total_buy_cost = Column(Float, nullable=False, default=0.0, comment="歷史總買入成本")
    historical_total_sell_amount = Column(Float, nullable=False, default=0.0, comment="歷史總賣出數量")
    historical_total_sell_value = Column(Float, nullable=False, default=0.0, comment="歷史總賣出價值")
    historical_avg_buy_price = Column(Float, nullable=False, default=0.0, comment="歷史平均買入價格")
    historical_avg_sell_price = Column(Float, nullable=False, default=0.0, comment="歷史平均賣出價格")
    last_active_position_closed_at = Column(DateTime, nullable=True, comment="上一個活躍倉位關閉時間")
    last_transaction_time = Column(Integer, nullable=False, default=0.0, comment="最後活躍時間")
    realized_profit = Column(Float, default=0.0, comment="已實現利潤")
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")
    
    __table_args__ = (
        UniqueConstraint('wallet_address', 'token_address', name='uix_wallet_token'),
        {'schema': 'solana'}
    )
    
    def __repr__(self):
        return f"<TokenBuyData {self.wallet_address[:6]}...:{self.token_address[:6]}...>"

class TransactionRecord(Base):
    """更細粒度的交易記錄表，用於存儲所有代幣交易"""
    __tablename__ = 'transaction_records'
    __table_args__ = {'schema': 'solana'}
    
    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, index=True)
    transaction_hash = Column(String(100), nullable=False, index=True)
    token_address = Column(String(100), nullable=False, index=True)
    activity_type = Column(String(20), nullable=False)
    amount = Column(Float, nullable=False)
    value = Column(Float)
    price = Column(Float)
    timestamp = Column(BIGINT, nullable=False)
    block_number = Column(BIGINT)
    from_address = Column(String(100))
    to_address = Column(String(100))
    created_at = Column(DateTime, server_default=func.now())
    
    def __repr__(self):
        return f"<TransactionRecord {self.transaction_hash[:10]}...>"