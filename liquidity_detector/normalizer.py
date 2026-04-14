"""
Normalizer module for parsing and validating Bybit WebSocket messages.
Converts raw data into structured Pydantic models.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
from loguru import logger


class OrderBookLevel(BaseModel):
    """Single order book level."""
    price: float = Field(..., gt=0, description="Price level")
    size: float = Field(..., ge=0, description="Order size in base currency")
    side: str = Field(..., description="Bid or Ask")
    
    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        if v not in ("Buy", "Sell"):
            raise ValueError(f"Invalid side: {v}")
        return v


class Trade(BaseModel):
    """Single trade record."""
    price: float = Field(..., gt=0, description="Trade price")
    size: float = Field(..., gt=0, description="Trade size in base currency")
    side: str = Field(..., description="Buy or Sell (derived from isBuyerMaker)")
    timestamp: datetime = Field(..., description="Trade timestamp")
    trade_id: str = Field(..., description="Unique trade ID")
    
    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        if v not in ("Buy", "Sell"):
            raise ValueError(f"Invalid side: {v}")
        return v


class NormalizedOrderBook(BaseModel):
    """Normalized order book snapshot or delta."""
    bids: List[OrderBookLevel] = Field(default_factory=list, description="Bid levels")
    asks: List[OrderBookLevel] = Field(default_factory=list, description="Ask levels")
    timestamp: datetime = Field(..., description="Order book timestamp")
    sequence: int = Field(..., description="Sequence number for delta updates")
    is_snapshot: bool = Field(default=False, description="True if this is a full snapshot")


class Normalizer:
    """Parses and validates raw Bybit WebSocket messages."""
    
    @staticmethod
    def parse_orderbook(raw_data: Dict[str, Any], received_at: datetime) -> Optional[NormalizedOrderBook]:
        """
        Parse orderbook snapshot or delta message.
        
        Bybit v5 orderbook format:
        - Snapshot: contains full book with 'bids' and 'asks' arrays
        - Delta: contains incremental changes
        
        Each level: [price, size]
        """
        try:
            # Extract sequence number
            sequence = raw_data.get("seq", 0)
            
            # Determine if snapshot
            update_type = raw_data.get("type", "snapshot")
            is_snapshot = update_type == "snapshot"
            
            # Parse bids
            bids = []
            for level in raw_data.get("bids", []):
                if len(level) >= 2:
                    price_str, size_str = level[0], level[1]
                    try:
                        price = float(price_str)
                        size = float(size_str)
                        if price > 0 and size >= 0:
                            bids.append(OrderBookLevel(
                                price=price,
                                size=size,
                                side="Buy"
                            ))
                    except (ValueError, TypeError):
                        logger.debug(f"Skipping invalid bid level: {level}")
            
            # Parse asks
            asks = []
            for level in raw_data.get("asks", []):
                if len(level) >= 2:
                    price_str, size_str = level[0], level[1]
                    try:
                        price = float(price_str)
                        size = float(size_str)
                        if price > 0 and size >= 0:
                            asks.append(OrderBookLevel(
                                price=price,
                                size=size,
                                side="Sell"
                            ))
                    except (ValueError, TypeError):
                        logger.debug(f"Skipping invalid ask level: {level}")
            
            # Use exchange timestamp if available, otherwise received time
            ts_ms = raw_data.get("ts")
            if ts_ms:
                try:
                    timestamp = datetime.utcfromtimestamp(int(ts_ms) / 1000.0)
                except (ValueError, TypeError, OSError):
                    timestamp = received_at
            else:
                timestamp = received_at
            
            return NormalizedOrderBook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=sequence,
                is_snapshot=is_snapshot
            )
            
        except Exception as e:
            logger.error(f"Failed to parse orderbook: {e}")
            return None
    
    @staticmethod
    def parse_trade(raw_data: Dict[str, Any], received_at: datetime) -> Optional[Trade]:
        """
        Parse single trade message.
        
        Bybit v5 trade format:
        - T: timestamp (ms)
        - p: price
        - v: volume/size
        - S: side (Buy/Sell)
        - i: trade ID
        
        Note: Bybit v5 already provides the side directly.
        For older APIs, isBuyerMaker=True means the buyer was maker (sell).
        """
        try:
            # Extract fields from Bybit v5 format
            price_str = raw_data.get("p", "")
            size_str = raw_data.get("v", "")
            side = raw_data.get("S", "")
            trade_id = raw_data.get("i", "")
            ts_ms = raw_data.get("T", 0)
            
            # Convert types
            price = float(price_str)
            size = float(size_str)
            
            # Validate side
            if side not in ("Buy", "Sell"):
                logger.warning(f"Invalid trade side: {side}")
                return None
            
            # Convert timestamp
            try:
                timestamp = datetime.utcfromtimestamp(int(ts_ms) / 1000.0)
            except (ValueError, TypeError, OSError):
                timestamp = received_at
            
            return Trade(
                price=price,
                size=size,
                side=side,
                timestamp=timestamp,
                trade_id=str(trade_id)
            )
            
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse trade data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing trade: {e}")
            return None
