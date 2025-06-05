import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from sqlalchemy import Column, String, Integer, Float, BigInteger, DateTime, Text, Boolean, select, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from app.core.config import settings
import json
import os
import concurrent.futures
import requests

logger = logging.getLogger(__name__)
Base = declarative_base()

# 定義 UTC+8 時區
UTC_PLUS_8 = timezone(timedelta(hours=8))

class TokenInfo(Base):
    """代幣元數據表"""
    __tablename__ = 'token_metadata'
    __table_args__ = {'schema': 'solana'}
    
    address = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=True)
    symbol = Column(String(100), nullable=True)
    icon = Column(Text, nullable=True)
    decimals = Column(Integer, nullable=True)
    supply = Column(BigInteger, nullable=True)
    supply_float = Column(Float, nullable=True)  # 經過轉換後的供應量
    price = Column(Float, nullable=True)
    market_cap = Column(Float, nullable=True)
    holder_count = Column(Integer, nullable=True)
    created_time = Column(BigInteger, nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC_PLUS_8))
    
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class TokenRepository:
    """
    代幣信息存儲庫 - 提供代幣基本信息
    """
    
    def __init__(self):
        self.tokens = {}
        self.loaded = False
        self.base_url = settings.SOLSCAN_API_URL
        self.api_token = settings.SOLSCAN_API_TOKEN
        self.headers = {"token": self.api_token}
        self.session = None
        self.in_memory_cache = {}  # 內存緩存
        self.cache_expiry = {}  # 緩存過期時間
        self.cache_ttl = 7200  # 默認緩存有效期（秒）
        self.load_lock = asyncio.Lock()  # 用於管理同時的載入操作
        self.token_fetch_locks = {}  # 用於每個代幣的載入操作
        self.token_cache_updates = {}
        
        # 初始化數據庫引擎 - 改用同步 URL 和引擎
        try:
            # 檢查 DATABASE_URL_SYNC 環境變數，如果不存在則將 asyncpg 轉換為標準 postgresql
            db_url = os.getenv('DATABASE_URL_SYNC')
            if not db_url and settings.DATABASE_URL:
                db_url = settings.DATABASE_URL
                if db_url.startswith('postgresql+asyncpg://'):
                    db_url = db_url.replace('postgresql+asyncpg://', 'postgresql://')
            
            if db_url:
                self.engine = create_engine(
                    db_url,
                    echo=False,
                    future=True,
                    pool_size=20,  # 增加基本連接池大小
                    max_overflow=30,  # 增加最大溢出連接數
                    pool_timeout=60,  # 增加連接超時時間
                    pool_recycle=1800,  # 保持現有的連接回收時間
                    pool_pre_ping=True,
                    pool_use_lifo=True  # 使用LIFO策略，重用最近使用的連接
                )
                self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)  # 防止自動過期
                # 確保表存在
                Base.metadata.create_all(self.engine)
            else:
                self.engine = None
                self.Session = None
                logger.warning("數據庫 URL 未設定，代幣儲存庫將僅使用內存快取")
        except Exception as e:
            logger.error(f"初始化數據庫連接時發生錯誤: {e}")
            self.engine = None
            self.Session = None
    
    def load_tokens(self, force=False):
        """
        加載代幣數據
        """
        if self.loaded and not force:
            return
            
        try:
            # 嘗試從緩存文件加載
            cache_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'tokens.json')
            if os.path.exists(cache_path):
                with open(cache_path, 'r', encoding='utf-8') as f:
                    self.tokens = json.load(f)
                logger.info(f"Loaded {len(self.tokens)} tokens from cache")
                self.loaded = True
                return
                
        except Exception as e:
            logger.error(f"Failed to load tokens from cache: {str(e)}")
            
        # 如果沒有緩存或加載失敗，初始化常用代幣
        self.tokens = {
            # Solana主幣
            "So11111111111111111111111111111111111111112": {
                "symbol": "SOL",
                "name": "Solana",
                "decimals": 9,
                "address": "So11111111111111111111111111111111111111112",
                "is_stable": False,
                "is_native": True,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
            },
            # USDC
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
                "symbol": "USDC",
                "name": "USD Coin",
                "decimals": 6,
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "is_stable": True,
                "is_native": False,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
            },
            # USDT
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
                "symbol": "USDT",
                "name": "Tether USD",
                "decimals": 6,
                "address": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "is_stable": True,
                "is_native": False,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB/logo.png"
            }
        }
        logger.info(f"Initialized with {len(self.tokens)} common tokens")
        self.loaded = True
    
    def get_token(self, address: str) -> Optional[Dict[str, Any]]:
        """
        獲取代幣信息 (同步方法，優先使用內存緩存)
        
        Args:
            address: 代幣地址
            
        Returns:
            代幣信息，如果不存在則返回None
        """
        if not self.loaded:
            self.load_tokens()
            
        # 1. 先檢查預設的代幣列表
        if address in self.tokens:
            return self.tokens[address]
        
        # 2. 檢查內存緩存
        current_time = datetime.now(UTC_PLUS_8)
        if address in self.in_memory_cache:
            if self.cache_expiry.get(address, current_time) > current_time:
                return self.in_memory_cache[address]
        
        # 3. 從數據庫同步獲取
        if self.Session:
            try:
                session = self.Session()
                db_token = session.query(TokenInfo).filter(TokenInfo.address == address).first()
                
                if db_token:
                    token_data = db_token.as_dict()
                    
                    # 更新內存緩存
                    self.in_memory_cache[address] = token_data
                    self.cache_expiry[address] = current_time + timedelta(seconds=self.cache_ttl)
                    
                    return token_data
            except SQLAlchemyError as e:
                logger.error(f"從數據庫查詢代幣信息時錯誤: {e}")
            finally:
                session.close()
        
        # 4. 如果內存和數據庫都沒有，使用同步 HTTP 獲取
        try:
            headers = {"token": self.api_token}
            url = f"{self.base_url}/token/meta?address={address}"
            
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                
                if result.get("success"):
                    data = result.get("data", {})
                    
                    # 計算 supply_float
                    supply = data.get("supply", "0")
                    decimals = data.get("decimals", 0)
                    try:
                        supply_int = int(supply)
                        supply_float = supply_int / (10 ** decimals)
                    except:
                        supply_float = 0
                    
                    token_info = {
                        "address": address,
                        "name": data.get("name"),
                        "symbol": data.get("symbol"),
                        "icon": data.get("icon"),
                        "decimals": decimals,
                        "supply": supply,
                        "supply_float": supply_float,
                        "price": data.get("price", 0),
                        "market_cap": data.get("market_cap", 0),
                        "holder_count": data.get("holder", 0),
                        "created_time": data.get("created_time"),
                        "updated_at": datetime.now(UTC_PLUS_8)
                    }
                    
                    # 保存到內存緩存
                    self.in_memory_cache[address] = token_info
                    self.cache_expiry[address] = current_time + timedelta(seconds=self.cache_ttl)
                    
                    # 保存到數據庫
                    self._save_token_info_to_db_sync(token_info)
                    
                    return token_info
        except Exception as e:
            logger.error(f"同步獲取代幣 {address} 信息時出錯: {e}")
            
        # 5. 返回默認信息
        default_info = {
            "address": address,
            "symbol": address[:6] + "...",
            "name": f"Unknown Token ({address[:8]}...)",
            "decimals": 9,  # 默認 decimals
            "supply_float": 1000000000  # 預設供應量
        }
        
        # 保存默認信息到緩存
        self.in_memory_cache[address] = default_info
        self.cache_expiry[address] = current_time + timedelta(seconds=300)  # 缩短未知代幣的緩存時間
        
        return default_info
    
    def get_token_symbol(self, address: str) -> str:
        """
        獲取代幣符號
        
        Args:
            address: 代幣地址
            
        Returns:
            代幣符號，如果不存在則返回地址的前6位
        """
        token = self.get_token(address)
        if token and token.get("symbol"):
            return token["symbol"]
        
        # 如果不存在，返回地址的前6位
        return address[:6] + "..."
    
    def get_token_decimals(self, address: str) -> int:
        """
        獲取代幣小數位數
        
        Args:
            address: 代幣地址
            
        Returns:
            代幣小數位數，默認為9
        """
        token = self.get_token(address)
        if token and "decimals" in token:
            return token["decimals"]
        
        # Solana默認為9
        return 9
    
    def is_stable_token(self, address: str) -> bool:
        """
        檢查是否為穩定幣
        
        Args:
            address: 代幣地址
            
        Returns:
            是否為穩定幣
        """
        token = self.get_token(address)
        if token:
            return token.get("is_stable", False)
            
        # 常見穩定幣地址
        stable_tokens = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            "DUALa4FC2yREwZ59PHeu1un4wis36vHRv5hWVBmxWtY7",  # DAI
            "USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX"    # USDH
        ]
        
        return address in stable_tokens
    
    def save_token(self, token_data: Dict[str, Any]) -> bool:
        """
        保存新的代幣信息
        
        Args:
            token_data: 代幣數據
            
        Returns:
            保存是否成功
        """
        try:
            if "address" not in token_data:
                logger.error("Token data missing address")
                return False
                
            address = token_data["address"]
            self.tokens[address] = token_data
            
            # 更新內存緩存
            self.in_memory_cache[address] = token_data
            self.cache_expiry[address] = datetime.now(UTC_PLUS_8) + timedelta(seconds=self.cache_ttl)
            
            # 嘗試保存到緩存文件
            try:
                cache_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
                os.makedirs(cache_dir, exist_ok=True)
                
                cache_path = os.path.join(cache_dir, 'tokens.json')
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump(self.tokens, f, indent=2)
            except Exception as e:
                logger.warning(f"保存代幣到缓存文件失敗: {e}")
            
            # 嘗試保存到數據庫
            self._save_token_info_to_db_sync(token_data)
                
            logger.info(f"Saved token {address} to cache and database")
            return True
        except Exception as e:
            logger.exception(f"Failed to save token: {str(e)}")
            return False
    
    async def get_session(self):
        """獲取HTTP會話"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers)
        return self.session
    
    async def close(self):
        """關閉HTTP會話"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def get_token_info_async(self, token_address: str) -> Optional[Dict[str, Any]]:
        """
        以異步方式獲取代幣信息，優先從緩存獲取，如果緩存過期則從數據庫或API獲取
        
        Args:
            token_address: 代幣地址
        
        Returns:
            代幣信息或None（如果無法獲取）
        """
        # 檢查是否已在預設代幣中
        if not self.loaded:
            self.load_tokens()
            
        if token_address in self.tokens:
            return self.tokens[token_address]
        
        # 檢查內存緩存
        current_time = datetime.now(UTC_PLUS_8)
        if token_address in self.in_memory_cache:
            if self.cache_expiry.get(token_address, current_time) > current_time:
                return self.in_memory_cache[token_address]
        
        # 使用鎖確保每個代幣只有一個獲取操作在進行
        if token_address not in self.token_fetch_locks:
            self.token_fetch_locks[token_address] = asyncio.Lock()
        
        async with self.token_fetch_locks[token_address]:
            # 再次檢查緩存（可能在等待鎖的時間內已被其他協程更新）
            if token_address in self.in_memory_cache:
                if self.cache_expiry.get(token_address, current_time) > current_time:
                    return self.in_memory_cache[token_address]
            
            # 嘗試從數據庫獲取
            if self.Session:
                session = None
                try:
                    # 使用同步數據庫操作
                    session = self.Session()
                    db_token = session.query(TokenInfo).filter(TokenInfo.address == token_address).first()
                    
                    # 檢查是否過期（1天）
                    if db_token and (current_time - db_token.updated_at).total_seconds() < 86400:
                        token_data = db_token.as_dict()
                        
                        # 更新內存緩存
                        self.in_memory_cache[token_address] = token_data
                        self.cache_expiry[token_address] = current_time + timedelta(seconds=self.cache_ttl)
                        
                        return token_data
                except SQLAlchemyError as e:
                    logger.error(f"從數據庫查詢代幣信息時錯誤: {e}")
                finally:
                    if session:
                        session.close()  # 確保連接被釋放
            
            # 從 API 獲取
            try:
                token_data = await self._fetch_token_info_from_api(token_address)
                
                if token_data:
                    # 保存到數據庫
                    self._save_token_info_to_db_sync(token_data)
                    
                    # 更新內存緩存
                    self.in_memory_cache[token_address] = token_data
                    self.cache_expiry[token_address] = current_time + timedelta(seconds=self.cache_ttl)
                    
                    return token_data
            except Exception as e:
                logger.exception(f"獲取代幣 {token_address} 信息時出錯: {e}")
                
            # 如果所有嘗試都失敗，返回默認值
            default_info = {
                "address": token_address,
                "symbol": token_address[:6] + "...",
                "name": f"Unknown Token ({token_address[:8]}...)",
                "decimals": 9,  # 默認 decimals
                "supply_float": 1000000000  # 預設供應量
            }
            
            # 保存默認信息到緩存 (短期緩存)
            self.in_memory_cache[token_address] = default_info
            self.cache_expiry[token_address] = current_time + timedelta(seconds=300)  # 缩短未知代幣的緩存時間
            
            return default_info
    
    async def _fetch_token_info_from_api(self, token_address: str) -> Optional[Dict[str, Any]]:
        """從 Solscan API 獲取代幣信息"""
        for retry in range(3):  # 重試 3 次
            try:
                session = await self.get_session()
                url = f"{self.base_url}/token/meta?address={token_address}"
                
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        if result.get("success"):
                            data = result.get("data", {})
                            
                            # 計算 supply_float（考慮 decimals）
                            supply = data.get("supply", "0")
                            decimals = data.get("decimals", 0)
                            try:
                                supply_int = int(supply)
                                supply_float = supply_int / (10 ** decimals)
                            except:
                                supply_float = 0
                            
                            token_info = {
                                "address": token_address,
                                "name": data.get("name"),
                                "symbol": data.get("symbol"),
                                "icon": data.get("icon"),
                                "decimals": decimals,
                                "supply": supply,
                                "supply_float": supply_float,
                                "price": data.get("price", 0),
                                "market_cap": data.get("market_cap", 0),
                                "holder_count": data.get("holder", 0),
                                "created_time": data.get("created_time"),
                                "updated_at": datetime.now(UTC_PLUS_8)
                            }
                            
                            logger.info(f"從 API 獲取代幣 {token_address} 的信息")
                            return token_info
                        else:
                            logger.warning(f"獲取代幣信息失敗: {result.get('message', 'Unknown error')}")
                    else:
                        logger.error(f"API 請求失敗: {response.status}")
                        
                        # 如果是速率限制錯誤，等待後重試
                        if response.status == 429:
                            wait_time = 2 ** retry  # 指數退避
                            logger.info(f"API 請求受限，等待 {wait_time} 秒後重試")
                            await asyncio.sleep(wait_time)
                            continue
                        
            except asyncio.TimeoutError:
                logger.warning(f"獲取代幣 {token_address} 信息超時，重試 {retry+1}/3")
                await asyncio.sleep(1)
                continue
            except Exception as e:
                logger.exception(f"獲取代幣 {token_address} 信息時出錯: {str(e)}")
                break
        
        return None
    
    def _save_token_info_to_db_sync(self, token_data: Dict[str, Any]) -> bool:
        """同步方式保存代幣信息到數據庫"""
        if not self.Session:
            return False
        
        session = None
        try:
            session = self.Session()
            
            # 檢查是否已存在
            existing_token = session.query(TokenInfo).filter(TokenInfo.address == token_data["address"]).first()
            
            if existing_token:
                # 更新現有記錄
                for key, value in token_data.items():
                    if hasattr(existing_token, key):
                        setattr(existing_token, key, value)
                existing_token.updated_at = datetime.now(UTC_PLUS_8)
            else:
                # 創建新記錄
                new_token = TokenInfo(**token_data)
                session.add(new_token)
            
            session.commit()
            return True
        except SQLAlchemyError as e:
            logger.error(f"保存代幣信息到數據庫時錯誤: {e}")
            if session:
                session.rollback()
            return False
        finally:
            if session:
                session.close()  # 確保連接被釋放
    
    async def get_multiple_token_info(self, token_addresses: list) -> Dict[str, Dict[str, Any]]:
        """批量獲取多個代幣的信息"""
        results = {}
        
        # 先檢查內存緩存和預設代幣
        remaining_addresses = []
        for addr in token_addresses:
            # 檢查預設代幣
            if not self.loaded:
                self.load_tokens()
                
            if addr in self.tokens:
                results[addr] = self.tokens[addr]
                continue
            
            # 檢查內存緩存
            current_time = datetime.now(UTC_PLUS_8)
            if addr in self.in_memory_cache:
                if self.cache_expiry.get(addr, current_time) > current_time:
                    results[addr] = self.in_memory_cache[addr]
                    continue
            
            remaining_addresses.append(addr)
        
        # 為剩餘的代幣創建異步任務
        if remaining_addresses:
            # 使用 asyncio.gather 同時獲取多個代幣信息
            # 限制並發數量，避免過載 API
            semaphore = asyncio.Semaphore(10)  # 最多同時 10 個請求
            
            async def fetch_with_semaphore(addr):
                async with semaphore:
                    return addr, await self.get_token_info_async(addr)
            
            tasks = [fetch_with_semaphore(addr) for addr in remaining_addresses]
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in task_results:
                if isinstance(result, Exception):
                    logger.error(f"獲取代幣信息時發生錯誤: {result}")
                    continue
                
                addr, info = result
                if info:
                    results[addr] = info
        
        return results
    
    def run_async_safely(self, coro):
        """改進版：安全運行異步協程，避免事件循環嵌套問題"""
        import asyncio
        import concurrent.futures
        import threading
        
        # 確保不會有事件循環共享問題
        def run_in_new_thread():
            try:
                # 在新線程中創建並運行新的事件循環
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coro)
                finally:
                    new_loop.close()
            except Exception as e:
                logger.error(f"在新線程中執行異步協程時出錯: {str(e)}")
                return None
            
        # 檢查當前是否在事件循環中
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果當前在事件循環中，使用線程執行新的事件循環
                thread = threading.Thread(target=run_in_new_thread)
                thread.start()
                thread.join()  # 等待線程完成
                return None  # 注意這裡無法獲取返回值，這是一個限制
            else:
                # 如果當前不在事件循環中，直接使用當前循環
                return loop.run_until_complete(coro)
        except RuntimeError:
            # 如果當前線程沒有事件循環，創建一個
            return asyncio.run(coro)
        except Exception as e:
            logger.error(f"運行異步協程時出錯: {e}")
            return None
    
    # 批量預載入代幣信息的方法
    async def prefetch_token_info(self, token_addresses: list):
        """批量預載入代幣信息，但不等待結果"""
        if not token_addresses:
            return
            
        logger.info(f"開始預載入 {len(token_addresses)} 個代幣的信息")
        asyncio.create_task(self.get_multiple_token_info(token_addresses))

# 創建單例實例
token_repository = TokenRepository()
token_repository.load_tokens()  # 預加載常用代幣

# def get_token_info_safely(address):
#     """安全獲取代幣信息的統一方法"""
#     if not address:
#         return {}
    
#     # 硬編碼常見代幣
#     common_tokens = {
#         "So11111111111111111111111111111111111111112": {
#             "symbol": "SOL",
#             "name": "Solana",
#             "decimals": 9,
#             "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
#         },
#         "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
#             "symbol": "USDC",
#             "name": "USD Coin",
#             "decimals": 6
#         },
#         "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
#             "symbol": "USDT",
#             "name": "Tether USD",
#             "decimals": 6
#         }
#     }
    
#     # 檢查是否為常見代幣
#     if address in common_tokens:
#         return common_tokens[address]
    
#     # 檢查快取
#     if address in self.token_info_cache:
#         return self.token_info_cache[address]
    
#     # 嘗試從token_repository獲取
#     try:
#         info = token_repository.get_token(address)
#         if info:
#             self.token_info_cache[address] = info
#             return info
#     except Exception as e:
#         logger.error(f"獲取代幣 {address} 信息時出錯: {e}")
    
#     # 失敗時返回基本信息
#     return {"symbol": address[:5], "name": f"Unknown Token ({address[:8]}...)"} 