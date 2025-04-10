from typing import List, Optional
from pydantic import BaseModel, Field, EmailStr


class Token(BaseModel):
    """Token response schema"""
    
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field("bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiration time in seconds")
    refresh_token: Optional[str] = Field(None, description="Refresh token")


class TokenPayload(BaseModel):
    """JWT token payload schema"""
    
    sub: str = Field(..., description="Subject (user ID)")
    scopes: List[str] = Field(default_factory=list, description="Token scopes")
    exp: int = Field(..., description="Expiration timestamp")


class UserAuth(BaseModel):
    """User authentication schema"""
    
    email: EmailStr = Field(..., description="User email")
    password: str = Field(..., description="User password")


class UserBase(BaseModel):
    """Base user information schema"""
    
    email: EmailStr = Field(..., description="User email")
    full_name: Optional[str] = Field(None, description="User full name")
    organization: Optional[str] = Field(None, description="User organization")


class User(UserBase):
    """Complete user schema (internal)"""
    
    id: str = Field(..., description="User ID")
    is_active: bool = Field(True, description="Whether the user is active")
    scopes: List[str] = Field(default_factory=list, description="User permission scopes")
    rate_limit: int = Field(100, description="Rate limit per minute")


class UserCreate(UserBase):
    """User creation schema"""
    
    password: str = Field(..., description="User password")
    scopes: Optional[List[str]] = Field(None, description="User permission scopes")