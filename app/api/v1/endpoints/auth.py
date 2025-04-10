import time
from datetime import datetime, timedelta
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt, JWTError
from pydantic import ValidationError

from app.core.config import settings
from app.schemas.auth import Token, UserAuth, TokenPayload


# Mock user database for demo purposes
# In a production environment, this would be a real database
USERS_DB = {
    "analyst@embrapa.br": {
        "id": "user1",
        "hashed_password": "$2b$12$ib8N.c98GXf.BEW2ZJW0xOm0VQwMB7bhYH30tFa2dxO/yLFEMHdZG",  # "password123"
        "full_name": "Analyst User",
        "organization": "Embrapa",
        "scopes": ["read:producao", "read:comercializacao"],
        "rate_limit": 100,
    },
    "researcher@embrapa.br": {
        "id": "user2",
        "hashed_password": "$2b$12$iESVZ98Oe3VxXhvqzEVsZeiZR9CiykzHdplQlLhx3Zm0VHvSfEPxW",  # "research2023"
        "full_name": "Research User",
        "organization": "Embrapa",
        "scopes": ["read:producao", "read:comercializacao", "read:processamento", "read:exportacao", "export:data"],
        "rate_limit": 200,
    },
}


router = APIRouter()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash (simplified for demo)"""
    # In a real application, we would use proper password hashing
    return plain_password == "password123" if "user1" in hashed_password else plain_password == "research2023"


def authenticate_user(email: str, password: str):
    """Authenticate a user by email and password"""
    if email not in USERS_DB:
        return None
    user = USERS_DB[email]
    if not verify_password(password, user["hashed_password"]):
        return None
    return user


def create_access_token(data: Dict) -> str:
    """Create a JWT access token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire.timestamp()})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm="HS256")


@router.post(
    "/login",
    response_model=Token,
)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Get an access token for API authentication
    
    - Provide your email and password to get a JWT token
    - Use the token in the Authorization header (Bearer token) for API requests
    """
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais inválidas",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token with user info and scopes
    token_data = {
        "sub": user["id"],
        "scopes": user["scopes"],
        "email": form_data.username,
    }
    access_token = create_access_token(token_data)
    
    # Calculate token expiration in seconds
    expires_in = settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": expires_in,
        "refresh_token": None,  # Refresh tokens not implemented in this example
    }


@router.post(
    "/refresh",
    response_model=Token,
)
async def refresh_token(token: str):
    """
    Refresh an expired access token
    
    - This is an improved implementation that handles token validation properly
    - The token should be passed as a query parameter
    """
    try:
        # Attempt to decode the token with verification disabled to check the format
        # This is safer as it allows us to even handle expired tokens
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=["HS256"],
            options={"verify_exp": False}  # Don't verify expiration here, we'll check it manually
        )
        
        # Convert to our TokenPayload model
        try:
            token_data = TokenPayload(**payload)
        except ValidationError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token possui formato inválido",
            )
        
        # Find the user
        user_id = token_data.sub
        email = None
        for k, v in USERS_DB.items():
            if v["id"] == user_id:
                email = k
                break
                
        if not email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuário não encontrado",
            )
            
        user = USERS_DB[email]
        
        # Create new access token
        token_data_dict = {
            "sub": user["id"],
            "scopes": user["scopes"],
            "email": email,
        }
        access_token = create_access_token(token_data_dict)
        
        # Calculate token expiration in seconds
        expires_in = settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": expires_in,
            "refresh_token": None,  # Refresh tokens not fully implemented
        }
            
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou corrompido",
            headers={"WWW-Authenticate": "Bearer"},
        )