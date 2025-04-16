from fastapi import Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from typing import Optional
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

# 定義 API 密鑰頭部
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: Optional[str] = Security(API_KEY_HEADER)) -> str:
    """
    驗證 API 密鑰
    
    Args:
        api_key: API 密鑰
        
    Returns:
        有效的 API 密鑰
        
    Raises:
        HTTPException: 如果 API 密鑰無效
    """
    if not api_key:
        logger.warning("API key is missing")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is missing",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    if api_key not in settings.API_KEYS:
        logger.warning(f"Invalid API key: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    return api_key


async def optional_api_key(api_key: Optional[str] = Security(API_KEY_HEADER)) -> Optional[str]:
    """
    可選的 API 密鑰驗證，用於某些可以公開訪問的端點
    
    Args:
        api_key: API 密鑰
        
    Returns:
        有效的 API 密鑰，如果未提供則為 None
    """
    if not api_key:
        return None
    
    if api_key not in settings.API_KEYS:
        return None
    
    return api_key


async def verify_admin_api_key(api_key: str = Security(API_KEY_HEADER)) -> str:
    """
    驗證管理員 API 密鑰，用於管理操作
    
    Args:
        api_key: API 密鑰
        
    Returns:
        有效的管理員 API 密鑰
        
    Raises:
        HTTPException: 如果 API 密鑰不是管理員密鑰
    """
    # 先驗證基本 API 密鑰
    await verify_api_key(api_key)
    
    # 檢查是否是管理員密鑰（這裡簡化處理，使用第一個密鑰作為管理員密鑰）
    if settings.API_KEYS and api_key != settings.API_KEYS[0]:
        logger.warning(f"Not an admin API key: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This operation requires admin privileges",
        )
    
    return api_key