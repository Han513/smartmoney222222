import requests
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class TokenService:
    """
    代幣服務，用於獲取代幣信息
    """
    
    # 穩定幣和SOL地址列表，用於交易類型判斷
    STABLECOINS = {
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
            "symbol": "USDC",
            "url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png",
            "priceUsd": 1
        },
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
            "symbol": "USDT",
            "url": "https://dd.dexscreener.com/ds-data/tokens/solana/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB.png?key=413023",
            "priceUsd": 1
        }
    }
    
    SOL_ADDRESSES = [
        "So11111111111111111111111111111111111111111",
        "So11111111111111111111111111111111111111112"
    ]
    
    def __init__(self):
        self.token_cache = {}  # 緩存已查詢的代幣信息
    
    @staticmethod
    def get_token_info(token_mint_address: str) -> Dict[str, Any]:
        """
        獲取代幣的一般信息，優先使用Helius API，如果失敗則結合DexScreener和Jupiter API的數據。
        當DexScreener缺少關鍵數據時，使用Jupiter API補充。
        特定幣種（如USDT和USDC）直接返回靜態數據，避免查詢。
        
        Parameters:
        token_mint_address (str): 代幣的Mint地址
        
        Returns:
        dict: 包含代幣信息的字典
        """
        # 針對特定幣種直接返回靜態數據
        stablecoins = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
                "symbol": "USDC",
                "url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png",
                "priceUsd": 1
            },
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
                "symbol": "USDT",
                "url": "https://dd.dexscreener.com/ds-data/tokens/solana/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB.png?key=413023",
                "priceUsd": 1
            }
        }

        # 檢查是否為USDT或USDC
        if token_mint_address in stablecoins:
            return stablecoins[token_mint_address]
            
        # 檢查是否為SOL地址
        if token_mint_address in ["So11111111111111111111111111111111111111111", "So11111111111111111111111111111111111111112"]:
            return {
                "symbol": "SOL",
                "url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png",
                "priceUsd": 0  # 這裡需要實時查詢SOL價格，暫時設為0
            }
            
        dex_data = {}
        # 嘗試使用DexScreener API
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    first_pair = data['pairs'][0]  # 取第一個交易對
                    
                    dex_data = {
                        "symbol": first_pair.get('baseToken', {}).get('symbol', None),
                        "url": first_pair.get('info', {}).get('imageUrl', None),
                        "marketcap": first_pair.get('marketCap', 0),
                        "priceNative": float(first_pair.get('priceNative', 0)),
                        "priceUsd": float(first_pair.get('priceUsd', 0)),
                        "volume": first_pair.get('volume', 0),
                        "liquidity": first_pair.get('liquidity', 0),
                        "dex": first_pair.get('dexId', 0),
                        "social": first_pair.get('info', {}).get('socials', None),
                        "source": "dexscreener"
                    }
        except Exception as e:
            logger.error(f"DexScreener API error for {token_mint_address}: {str(e)}")
        
        # 檢查DexScreener是否返回了完整數據
        missing_key_data = not dex_data or dex_data.get("url") is None
        
        # 如果DexScreener API失敗或關鍵數據缺失，嘗試使用Jupiter API
        if missing_key_data:
            try:
                url = f"https://api.jup.ag/tokens/v1/token/{token_mint_address}"
                headers = {'Accept': 'application/json'}
                response = requests.get(url, headers=headers, timeout=5)
                
                if response.status_code == 200:
                    token_data = response.json()
                    # 如果DexScreener完全失敗，使用Jupiter的全部數據
                    if not dex_data:
                        return {
                            "symbol": token_data.get("symbol", None),
                            "url": token_data.get("icon", None),  # 使用logo作為URL
                            "marketcap": 0,   # Jupiter API沒有提供市值
                            "priceNative": 0, # Jupiter API沒有提供原生價格
                            "priceUsd": 0,    # Jupiter API沒有提供USD價格
                            "volume": 0,      # Jupiter API沒有提供交易量
                            "liquidity": 0,   # Jupiter API沒有提供流動性
                            "name": token_data.get("name", None),  # 額外信息
                            "decimals": token_data.get("decimals", 0),  # 額外信息
                            "source": "jupiter"  # 標記數據來源
                        }
                    # 如果DexScreener只缺少部分數據，補充這些數據
                    else:
                        # 檢查並補充缺失的數據
                        if dex_data.get("symbol") is None:
                            dex_data["symbol"] = token_data.get("symbol", None)
                        if dex_data.get("url") is None or dex_data.get("url") == "no url":
                            dex_data["url"] = token_data.get("logoURI", "no url")
                        # 添加Jupiter API獨有的有用數據
                        dex_data["name"] = token_data.get("name", None)
                        dex_data["icon"] = token_data.get("logoURI", None)
                        dex_data["decimals"] = token_data.get("decimals", 0)
                        dex_data["source"] = "dexscreener+jupiter"  # 標記為組合數據源
                        return dex_data
            except Exception as e:
                logger.error(f"Jupiter API error for {token_mint_address}: {str(e)}")
        
        # 如果有DexScreener數據，返回它
        if dex_data:
            return dex_data
            
        # 如果所有API都失敗，返回默認值
        return {
            "symbol": "UNKNOWN",
            "url": None,
            "priceUsd": 0,
            "source": "default"
        }
    
    def get_token_symbol(self, token_address: str) -> str:
        """
        獲取代幣符號，帶緩存
        
        Args:
            token_address: 代幣地址
            
        Returns:
            代幣符號
        """
        if token_address not in self.token_cache:
            try:
                token_info = self.get_token_info(token_address)
                self.token_cache[token_address] = token_info
            except Exception as e:
                logger.error(f"Error getting token info for {token_address}: {str(e)}")
                return "UNKNOWN"
                
        return self.token_cache[token_address].get("symbol", "UNKNOWN")
    
    def get_token_icon(self, token_address: str) -> str:
        """
        獲取代幣圖標URL，帶緩存
        
        Args:
            token_address: 代幣地址
            
        Returns:
            代幣圖標URL
        """
        if token_address not in self.token_cache:
            try:
                token_info = self.get_token_info(token_address)
                self.token_cache[token_address] = token_info
            except Exception as e:
                logger.error(f"Error getting token info for {token_address}: {str(e)}")
                return None
                
        return self.token_cache[token_address].get("url")
    
    def determine_transaction_type(self, token1_address: str, token2_address: str) -> str:
        """
        根據交易涉及的代幣確定交易類型
        
        Args:
            token1_address: 從代幣地址
            token2_address: 目標代幣地址
            
        Returns:
            交易類型："buy" 或 "sell"
        """
        is_token1_stable = token1_address in self.STABLECOINS or token1_address in self.SOL_ADDRESSES
        is_token2_stable = token2_address in self.STABLECOINS or token2_address in self.SOL_ADDRESSES
        
        # 判斷邏輯：
        # 1. token1是穩定幣/SOL，token2不是 => 買入(使用穩定幣/SOL購買token2)
        # 2. token1不是穩定幣/SOL，token2是 => 賣出(賣出token1換取穩定幣/SOL)
        # 3. token1是SOL，token2是穩定幣 => 賣出(賣出SOL換取穩定幣)
        # 4. token1是穩定幣，token2是SOL => 買入(買入SOL)
        # 5. token1和token2都不是穩定幣/SOL => 以token2作為交易幣種，視為買入
        
        if token1_address in self.SOL_ADDRESSES and token2_address in self.STABLECOINS:
            return "sell"  # 賣出SOL換取穩定幣
        elif token1_address in self.STABLECOINS and token2_address in self.SOL_ADDRESSES:
            return "buy"   # 買入SOL
        elif is_token1_stable and not is_token2_stable:
            return "buy"   # 使用穩定幣/SOL購買token2
        elif not is_token1_stable and is_token2_stable:
            return "sell"  # 賣出token1換取穩定幣/SOL
        else:
            return "buy"   # 默認為買入(token2)

# 單例實例
token_service = TokenService() 