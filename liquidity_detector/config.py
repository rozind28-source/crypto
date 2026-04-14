"""
Configuration module using Pydantic Settings.
Reads environment variables from .env file.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from typing import Literal


class Config(BaseSettings):
    """Application configuration loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Bybit WebSocket v5 settings
    BYBIT_WS_URL: str = Field(
        default="wss://stream.bybit.com/v5/public/linear",
        description="Bybit WebSocket v5 endpoint"
    )
    SYMBOL: str = Field(default="BTCUSDT", description="Trading pair symbol")
    CATEGORY: Literal["linear", "inverse", "spot"] = Field(
        default="linear",
        description="Product category"
    )
    DEPTH_LEVEL: int = Field(default=50, ge=1, le=200, description="Order book depth")
    
    # Detector settings
    DELTA_WINDOW_SEC: float = Field(
        default=60.0,
        gt=0,
        description="Time window in seconds for volume delta calculation"
    )
    DELTA_THRESHOLD_USDT: float = Field(
        default=50000.0,
        gt=0,
        description="Threshold for delta divergence alert in USDT"
    )
    PRICE_CHANGE_THRESHOLD_PCT: float = Field(
        default=0.5,
        gt=0,
        le=100,
        description="Max price change percentage to consider for divergence"
    )
    SPOOF_SIZE_THRESHOLD_USDT: float = Field(
        default=100000.0,
        gt=0,
        description="Minimum order size in USDT to consider for spoofing detection"
    )
    SPOOF_LIFETIME_SEC: float = Field(
        default=0.5,
        gt=0,
        description="Maximum lifetime in seconds for a spoofed order"
    )
    
    # Telegram settings
    TELEGRAM_BOT_TOKEN: str = Field(
        ...,
        description="Telegram bot token for sending alerts"
    )
    TELEGRAM_CHAT_ID: str = Field(
        ...,
        description="Telegram chat ID to receive alerts"
    )
    
    @field_validator("BYBIT_WS_URL")
    @classmethod
    def validate_ws_url(cls, v: str) -> str:
        if not v.startswith("wss://"):
            raise ValueError("WebSocket URL must start with wss://")
        return v
    
    @property
    def orderbook_channel(self) -> str:
        """Returns the orderbook subscription channel name."""
        return f"orderbook.{self.DEPTH_LEVEL}.{self.SYMBOL}"
    
    @property
    def trades_channel(self) -> str:
        """Returns the trades subscription channel name."""
        return f"publicTrade.{self.SYMBOL}"


config = Config()
