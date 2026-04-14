"""
Anomaly detection core module.
Implements Volume Delta Divergence and Spoofing/Layering detection algorithms.
"""
import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Deque
from loguru import logger

from config import Config
from normalizer import Trade, NormalizedOrderBook


@dataclass
class PriceVolumePoint:
    """Single point in the delta calculation window."""
    timestamp: datetime
    price: float
    buy_volume: float  # in base currency
    sell_volume: float  # in base currency
    buy_value: float = field(init=False)  # in quote currency (USDT)
    sell_value: float = field(init=False)  # in quote currency (USDT)
    
    def __post_init__(self):
        self.buy_value = self.buy_volume * self.price
        self.sell_value = self.sell_volume * self.price


@dataclass
class OrderBookLevelState:
    """Tracks state of a single order book level for spoofing detection."""
    price: float
    side: str  # "Buy" or "Sell"
    size: float
    first_seen: datetime
    last_update: datetime
    alerted: bool = False


@dataclass
class AnomalyAlert:
    """Structured alert for detected anomalies."""
    anomaly_type: str  # "DIVERGENCE" or "SPOOFING"
    symbol: str
    timestamp: datetime
    details: Dict[str, float | str]
    message: str


class AnomalyDetector:
    """
    Core anomaly detection engine.
    
    Detects two types of anomalies:
    1. Volume Delta Divergence - when large delta doesn't move price
    2. Spoofing/Layering - large orders that appear and disappear quickly
    """
    
    def __init__(self, config: Config):
        self.config = config
        
        # Sliding window for delta calculation using deque
        self._trade_window: Deque[PriceVolumePoint] = deque()
        
        # Order book state for spoofing detection
        # Key: (price, side), Value: OrderBookLevelState
        self._orderbook_state: Dict[Tuple[float, str], OrderBookLevelState] = {}
        
        # Track mid-price for divergence calculation
        self._prev_mid_price: Optional[float] = None
        self._window_start_price: Optional[float] = None
        
        # Alert queue
        self._alert_queue: asyncio.Queue[AnomalyAlert] = asyncio.Queue()
        
        # Last processed timestamp for cleanup
        self._last_timestamp: Optional[datetime] = None
    
    @property
    def alert_queue(self) -> asyncio.Queue[AnomalyAlert]:
        """Returns the alert queue for consumers."""
        return self._alert_queue
    
    def _calculate_delta(self) -> float:
        """
        Calculate volume delta over the current window.
        
        Delta = sum(buy_volume * price) - sum(sell_volume * price)
        Positive delta: more buying pressure
        Negative delta: more selling pressure
        """
        if not self._trade_window:
            return 0.0
        
        total_buy_value = sum(p.buy_value for p in self._trade_window)
        total_sell_value = sum(p.sell_value for p in self._trade_window)
        
        return total_buy_value - total_sell_value
    
    def _get_price_change_pct(self, current_mid: float) -> float:
        """Calculate price change percentage from window start."""
        if self._window_start_price is None or self._window_start_price == 0:
            return 0.0
        
        return ((current_mid - self._window_start_price) / self._window_start_price) * 100
    
    def _cleanup_window(self, current_time: datetime) -> None:
        """Remove trades outside the delta window."""
        cutoff = current_time - timedelta(seconds=self.config.DELTA_WINDOW_SEC)
        
        while self._trade_window and self._trade_window[0].timestamp < cutoff:
            self._trade_window.popleft()
        
        # Update window start price to earliest trade in window
        if self._trade_window:
            self._window_start_price = self._trade_window[0].price
    
    def _check_divergence(self, current_mid: float, current_time: datetime) -> Optional[AnomalyAlert]:
        """
        Check for Volume Delta Divergence.
        
        Divergence occurs when:
        1. Price change is minimal (< PRICE_CHANGE_THRESHOLD_PCT)
        2. But delta is significant (> DELTA_THRESHOLD_USDT)
        
        This suggests hidden absorption or manipulation.
        """
        if len(self._trade_window) < 5:  # Need minimum trades for meaningful delta
            return None
        
        delta = self._calculate_delta()
        price_change_pct = self._get_price_change_pct(current_mid)
        
        # Check divergence conditions
        if (abs(price_change_pct) < self.config.PRICE_CHANGE_THRESHOLD_PCT and
            abs(delta) > self.config.DELTA_THRESHOLD_USDT):
            
            direction = "BULLISH" if delta > 0 else "BEARISH"
            
            return AnomalyAlert(
                anomaly_type="DIVERGENCE",
                symbol=self.config.SYMBOL,
                timestamp=current_time,
                details={
                    "delta_usdt": round(abs(delta), 2),
                    "price_change_pct": round(price_change_pct, 4),
                    "current_price": round(current_mid, 2),
                    "direction": direction,
                    "window_sec": self.config.DELTA_WINDOW_SEC
                },
                message=f"Volume Delta {direction}: ${abs(delta):,.2f} USDT delta with only {price_change_pct:.3f}% price move"
            )
        
        return None
    
    def _check_spoofing(self, current_time: datetime) -> List[AnomalyAlert]:
        """
        Detect spoofing/layering patterns.
        
        Spoofing occurs when:
        1. Large order appears (> SPOOF_SIZE_THRESHOLD_USDT)
        2. Order disappears or shrinks significantly
        3. Lifetime is very short (< SPOOF_LIFETIME_SEC)
        
        This suggests fake liquidity intended to manipulate price.
        """
        alerts = []
        
        # Clean up old entries and check for disappeared orders
        keys_to_remove = []
        
        for (price, side), state in self._orderbook_state.items():
            lifetime = (current_time - state.first_seen).total_seconds()
            
            # Check if order qualifies as potential spoof
            order_value_usdt = state.size * price
            
            if order_value_usdt >= self.config.SPOOF_SIZE_THRESHOLD_USDT:
                # Order was large enough to monitor
                if lifetime < self.config.SPOOF_LIFETIME_SEC and not state.alerted:
                    # Order existed briefly and hasn't been alerted yet
                    # If it's still here, mark as alerted to avoid repeat
                    state.alerted = True
                    
                    alerts.append(AnomalyAlert(
                        anomaly_type="SPOOFING",
                        symbol=self.config.SYMBOL,
                        timestamp=current_time,
                        details={
                            "price": round(price, 2),
                            "side": side,
                            "size_usdt": round(order_value_usdt, 2),
                            "lifetime_sec": round(lifetime, 3),
                            "threshold_sec": self.config.SPOOF_LIFETIME_SEC
                        },
                        message=f"Spoof detected: ${order_value_usdt:,.2f} {side} order at ${price} lasted only {lifetime:.3f}s"
                    ))
            
            # Remove stale entries (either alerted or too old)
            if state.alerted or lifetime > self.config.SPOOF_LIFETIME_SEC * 10:
                keys_to_remove.append((price, side))
        
        # Remove processed entries
        for key in keys_to_remove:
            del self._orderbook_state[key]
        
        return alerts
    
    def _update_orderbook_state(self, orderbook: NormalizedOrderBook) -> None:
        """
        Update order book state tracking for spoofing detection.
        
        Compares new order book levels with existing state to detect:
        - New large orders (potential spoofs)
        - Removed/shrunk orders (spoof execution)
        """
        current_time = orderbook.timestamp
        seen_levels: set[Tuple[float, str]] = set()
        
        # Process bids
        for level in orderbook.bids:
            key = (level.price, "Buy")
            seen_levels.add(key)
            
            order_value = level.size * level.price
            
            if key in self._orderbook_state:
                # Existing level - update
                state = self._orderbook_state[key]
                
                # Check if order significantly decreased (potential spoof execution)
                if level.size < state.size * 0.5 and state.size * state.price >= self.config.SPOOF_SIZE_THRESHOLD_USDT:
                    lifetime = (current_time - state.first_seen).total_seconds()
                    
                    if lifetime < self.config.SPOOF_LIFETIME_SEC and not state.alerted:
                        # Order was removed quickly - spoof detected
                        state.alerted = True
                        
                        alert = AnomalyAlert(
                            anomaly_type="SPOOFING",
                            symbol=self.config.SYMBOL,
                            timestamp=current_time,
                            details={
                                "price": round(level.price, 2),
                                "side": "Buy",
                                "original_size_usdt": round(state.size * state.price, 2),
                                "remaining_size_usdt": round(order_value, 2),
                                "lifetime_sec": round(lifetime, 3)
                            },
                            message=f"Spoof removed: Buy wall ${state.size * state.price:,.2f} -> ${order_value:,.2f} in {lifetime:.3f}s"
                        )
                        asyncio.create_task(self._alert_queue.put(alert))
                
                # Update state
                state.last_update = current_time
                state.size = level.size
                
            elif order_value >= self.config.SPOOF_SIZE_THRESHOLD_USDT:
                # New large order - start tracking
                self._orderbook_state[key] = OrderBookLevelState(
                    price=level.price,
                    side="Buy",
                    size=level.size,
                    first_seen=current_time,
                    last_update=current_time,
                    alerted=False
                )
        
        # Process asks
        for level in orderbook.asks:
            key = (level.price, "Sell")
            seen_levels.add(key)
            
            order_value = level.size * level.price
            
            if key in self._orderbook_state:
                # Existing level - update
                state = self._orderbook_state[key]
                
                # Check if order significantly decreased
                if level.size < state.size * 0.5 and state.size * state.price >= self.config.SPOOF_SIZE_THRESHOLD_USDT:
                    lifetime = (current_time - state.first_seen).total_seconds()
                    
                    if lifetime < self.config.SPOOF_LIFETIME_SEC and not state.alerted:
                        state.alerted = True
                        
                        alert = AnomalyAlert(
                            anomaly_type="SPOOFING",
                            symbol=self.config.SYMBOL,
                            timestamp=current_time,
                            details={
                                "price": round(level.price, 2),
                                "side": "Sell",
                                "original_size_usdt": round(state.size * state.price, 2),
                                "remaining_size_usdt": round(order_value, 2),
                                "lifetime_sec": round(lifetime, 3)
                            },
                            message=f"Spoof removed: Sell wall ${state.size * state.price:,.2f} -> ${order_value:,.2f} in {lifetime:.3f}s"
                        )
                        asyncio.create_task(self._alert_queue.put(alert))
                
                state.last_update = current_time
                state.size = level.size
                
            elif order_value >= self.config.SPOOF_SIZE_THRESHOLD_USDT:
                # New large order - start tracking
                self._orderbook_state[key] = OrderBookLevelState(
                    price=level.price,
                    side="Sell",
                    size=level.size,
                    first_seen=current_time,
                    last_update=current_time,
                    alerted=False
                )
        
        # Remove levels no longer in order book (could indicate spoof removal)
        removed_keys = []
        for key, state in list(self._orderbook_state.items()):
            if key not in seen_levels:
                # Level disappeared from order book
                order_value = state.size * state.price
                lifetime = (current_time - state.first_seen).total_seconds()
                
                if (order_value >= self.config.SPOOF_SIZE_THRESHOLD_USDT and 
                    lifetime < self.config.SPOOF_LIFETIME_SEC and 
                    not state.alerted):
                    
                    state.alerted = True
                    
                    alert = AnomalyAlert(
                        anomaly_type="SPOOFING",
                        symbol=self.config.SYMBOL,
                        timestamp=current_time,
                        details={
                            "price": round(state.price, 2),
                            "side": state.side,
                            "size_usdt": round(order_value, 2),
                            "lifetime_sec": round(lifetime, 3),
                            "status": "removed"
                        },
                        message=f"Spoof removed: {state.side} wall ${order_value:,.2f} at ${state.price} disappeared in {lifetime:.3f}s"
                    )
                    asyncio.create_task(self._alert_queue.put(alert))
                
                removed_keys.append(key)
        
        for key in removed_keys:
            del self._orderbook_state[key]
    
    async def process_trade(self, trade: Trade) -> None:
        """Process incoming trade and check for divergence."""
        try:
            current_time = trade.timestamp
            self._last_timestamp = current_time
            
            # Add to sliding window
            if trade.side == "Buy":
                point = PriceVolumePoint(
                    timestamp=current_time,
                    price=trade.price,
                    buy_volume=trade.size,
                    sell_volume=0.0
                )
            else:
                point = PriceVolumePoint(
                    timestamp=current_time,
                    price=trade.price,
                    buy_volume=0.0,
                    sell_volume=trade.size
                )
            
            self._trade_window.append(point)
            
            # Cleanup old trades
            self._cleanup_window(current_time)
            
            # Set window start price if needed
            if self._window_start_price is None and self._trade_window:
                self._window_start_price = self._trade_window[0].price
            
            # Store previous mid-price before checking
            if self._prev_mid_price is not None:
                # Check for divergence
                alert = self._check_divergence(trade.price, current_time)
                if alert:
                    await self._alert_queue.put(alert)
                    logger.info(f"Divergence alert: {alert.message}")
            
            self._prev_mid_price = trade.price
            
        except Exception as e:
            logger.error(f"Error processing trade: {e}")
    
    async def process_orderbook(self, orderbook: NormalizedOrderBook) -> None:
        """Process incoming order book and check for spoofing."""
        try:
            if not orderbook.bids or not orderbook.asks:
                return
            
            current_time = orderbook.timestamp
            self._last_timestamp = current_time
            
            # Calculate mid-price for divergence reference
            best_bid = orderbook.bids[0].price if orderbook.bids else None
            best_ask = orderbook.asks[0].price if orderbook.asks else None
            
            if best_bid and best_ask:
                mid_price = (best_bid + best_ask) / 2
                
                # Update window start price if needed
                if self._window_start_price is None:
                    self._window_start_price = mid_price
                
                # Update order book state and check for spoofing
                self._update_orderbook_state(orderbook)
                
                # Also check divergence based on order book movement
                if self._prev_mid_price is not None:
                    alert = self._check_divergence(mid_price, current_time)
                    if alert:
                        await self._alert_queue.put(alert)
                        logger.info(f"Divergence alert from orderbook: {alert.message}")
                
                self._prev_mid_price = mid_price
            
        except Exception as e:
            logger.error(f"Error processing orderbook: {e}")
    
    async def get_alert(self) -> AnomalyAlert:
        """Get next alert from queue (blocking)."""
        return await self._alert_queue.get()
