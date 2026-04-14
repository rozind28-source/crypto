"""
Async WebSocket client for Bybit v5 API.
Handles connection, reconnection with exponential backoff, and message queuing.
"""
import asyncio
import json
from datetime import datetime
from typing import AsyncGenerator, Dict, Any

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from loguru import logger

from config import Config


class BybitWSClient:
    """Asynchronous WebSocket client for Bybit v5 public streams."""
    
    def __init__(self, config: Config):
        self.config = config
        self._queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 30.0
        self._ping_interval = 20.0
        self._pong_timeout = 10.0
    
    @property
    def queue(self) -> asyncio.Queue[Dict[str, Any]]:
        """Returns the message queue for consumers."""
        return self._queue
    
    async def _send_subscribe(self, websocket) -> None:
        """Send subscription message for orderbook and trades channels."""
        subscribe_msg = {
            "op": "subscribe",
            "args": [
                self.config.orderbook_channel,
                self.config.trades_channel
            ]
        }
        await websocket.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to channels: {self.config.orderbook_channel}, {self.config.trades_channel}")
    
    async def _handle_message(self, raw_message: str) -> None:
        """Parse and validate incoming WebSocket message."""
        try:
            data = json.loads(raw_message)
            
            # Handle pong response
            if data.get("op") == "pong":
                logger.debug("Received pong from Bybit")
                return
            
            # Handle subscription confirmation
            if data.get("op") == "subscribe":
                if data.get("success"):
                    logger.info("Subscription confirmed")
                else:
                    logger.warning(f"Subscription failed: {data}")
                return
            
            # Handle snapshot or delta updates
            if "topic" in data and "data" in data:
                topic = data["topic"]
                if topic.startswith("orderbook"):
                    await self._queue.put({
                        "type": "orderbook",
                        "data": data["data"],
                        "timestamp": datetime.utcnow()
                    })
                elif topic.startswith("publicTrade"):
                    await self._queue.put({
                        "type": "trade",
                        "data": data["data"],
                        "timestamp": datetime.utcnow()
                    })
            else:
                logger.debug(f"Unhandled message type: {data}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _run_connection(self) -> None:
        """Establish and maintain WebSocket connection."""
        while self._running:
            try:
                uri = f"{self.config.BYBIT_WS_URL}"
                logger.info(f"Connecting to {uri}...")
                
                async with websockets.connect(
                    uri,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._pong_timeout,
                    close_timeout=10,
                ) as websocket:
                    logger.info("WebSocket connected")
                    self._reconnect_delay = 1.0  # Reset on successful connection
                    
                    await self._send_subscribe(websocket)
                    
                    async for message in websocket:
                        if not self._running:
                            break
                        await self._handle_message(message)
                        
            except ConnectionClosed as e:
                logger.warning(f"Connection closed: code={e.code}, reason={e.reason}")
            except WebSocketException as e:
                logger.error(f"WebSocket error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error in WebSocket connection: {e}")
            
            if self._running:
                logger.info(f"Reconnecting in {self._reconnect_delay:.1f}s...")
                await asyncio.sleep(self._reconnect_delay)
                # Exponential backoff with max delay
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._max_reconnect_delay
                )
    
    async def start(self) -> None:
        """Start the WebSocket client."""
        self._running = True
        logger.info("Starting WebSocket client...")
        await self._run_connection()
    
    def stop(self) -> None:
        """Stop the WebSocket client."""
        logger.info("Stopping WebSocket client...")
        self._running = False
    
    async def get_message(self) -> Dict[str, Any]:
        """Get next message from queue (blocking)."""
        return await self._queue.get()
