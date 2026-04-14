"""
Main entry point for the Liquidity Detector application.
Orchestrates all components and handles graceful shutdown.
Now includes a web interface instead of Telegram alerts.
Fixed for Windows compatibility (no signal handlers on Win32).
"""
import asyncio
import signal
import sys
from datetime import datetime
from typing import List, Optional
from urllib.parse import parse_qs

from loguru import logger

from config import config, Config
from ws_client import BybitWSClient
from normalizer import Normalizer, Trade, NormalizedOrderBook
from detector import AnomalyDetector
from web_api import create_api, run_server, WebAPI


class LiquidityDetector:
    """
    Main orchestrator for the liquidity anomaly detection system.
    
    Coordinates:
    - WebSocket client for market data
    - Normalizer for data parsing
    - Detector for anomaly detection
    - Web API for real-time interface
    """
    
    def __init__(self, symbol: str = "BTCUSDT"):
        # Create config with specified symbol
        self.config = Config(SYMBOL=symbol)
        self.ws_client = BybitWSClient(self.config)
        self.normalizer = Normalizer()
        self.detector = AnomalyDetector(self.config)
        self.web_api: Optional[WebAPI] = None
        
        # Shutdown coordination
        self._shutdown_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
    
    def _setup_logging(self) -> None:
        """Configure loguru logging."""
        logger.remove()  # Remove default handler
        
        # Console handler with nice formatting
        logger.add(
            sink=lambda msg: print(msg, end=""),
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                   "<level>{message}</level>",
            level="INFO",
            colorize=True
        )
        
        # File handler for debugging
        logger.add(
            "logs/liquidity_detector_{time:YYYY-MM-DD}.log",
            rotation="00:00",
            retention="7 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            backtrace=True,
            diagnose=True
        )
    
    async def _ws_consumer(self) -> None:
        """
        Consume messages from WebSocket queue and route to processors.
        
        This runs as a separate task alongside the WebSocket connection.
        """
        logger.info("WebSocket consumer started")
        
        while not self._shutdown_event.is_set():
            try:
                # Get message with timeout to check shutdown
                try:
                    message = await asyncio.wait_for(
                        self.ws_client.queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                msg_type = message.get("type")
                raw_data = message.get("data", [])
                received_at = message.get("timestamp", datetime.utcnow())
                
                if msg_type == "orderbook":
                    # Parse orderbook
                    orderbook = self.normalizer.parse_orderbook(raw_data, received_at)
                    if orderbook:
                        await self.detector.process_orderbook(orderbook)
                        
                        # Update web API with latest orderbook data
                        if self.web_api and orderbook.bids and orderbook.asks:
                            await self.web_api.update_market_data(
                                best_bid=orderbook.bids[0].price,
                                best_ask=orderbook.asks[0].price,
                                mid_price=(orderbook.bids[0].price + orderbook.asks[0].price) / 2
                            )
                
                elif msg_type == "trade":
                    # Parse each trade in the batch
                    for trade_data in raw_data:
                        trade = self.normalizer.parse_trade(trade_data, received_at)
                        if trade:
                            await self.detector.process_trade(trade)
                            
                            # Update web API with latest trade data
                            if self.web_api:
                                await self.web_api.update_market_data(
                                    last_trade_price=trade.price,
                                    last_trade_size=trade.size
                                )
                
                else:
                    logger.debug(f"Unknown message type: {msg_type}")
                
            except asyncio.CancelledError:
                logger.info("WebSocket consumer cancelled")
                break
            except Exception as e:
                logger.error(f"Error in WebSocket consumer: {e}")
                await asyncio.sleep(0.1)  # Prevent tight loop on errors
        
        logger.info("WebSocket consumer stopped")
    
    async def _alerter_consumer(self) -> None:
        """Run the alerter worker to send alerts to web interface."""
        logger.info("Alert consumer started")
        
        while not self._shutdown_event.is_set():
            try:
                # Get alert with timeout to check shutdown
                try:
                    alert = await asyncio.wait_for(
                        self.detector.alert_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Publish to web API
                if self.web_api:
                    await self.web_api.publish_alert(alert)
                    logger.info(f"🚨 Alert published: {alert.anomaly_type} - {alert.message}")
                
            except asyncio.CancelledError:
                logger.info("Alert consumer cancelled")
                break
            except Exception as e:
                logger.error(f"Error in alert consumer: {e}")
                await asyncio.sleep(0.1)
        
        logger.info("Alert consumer stopped")
    
    async def _run(self) -> None:
        """Main run loop - starts all components."""
        logger.info("=" * 60)
        logger.info("LIQUIDITY DETECTOR - Starting")
        logger.info("=" * 60)
        logger.info(f"Symbol: {self.config.SYMBOL}")
        logger.info(f"Category: {self.config.CATEGORY}")
        logger.info(f"Depth: {self.config.DEPTH_LEVEL}")
        logger.info(f"Delta Window: {self.config.DELTA_WINDOW_SEC}s")
        logger.info(f"Delta Threshold: ${self.config.DELTA_THRESHOLD_USDT:,.2f}")
        logger.info(f"Price Change Threshold: {self.config.PRICE_CHANGE_THRESHOLD_PCT}%")
        logger.info(f"Spoof Size Threshold: ${self.config.SPOOF_SIZE_THRESHOLD_USDT:,.2f}")
        logger.info(f"Spoof Lifetime: {self.config.SPOOF_LIFETIME_SEC}s")
        logger.info("=" * 60)
        
        # Create and configure web API
        self.web_api = create_api(self.config)
        self.web_api.set_detector(self.detector)
        
        # Start tasks
        logger.info("Starting components...")
        
        # Task 1: WebSocket connection (runs indefinitely)
        ws_task = asyncio.create_task(
            self.ws_client.start(),
            name="websocket"
        )
        self._tasks.append(ws_task)
        
        # Small delay to allow connection establishment
        await asyncio.sleep(2.0)
        
        # Task 2: WebSocket message consumer
        consumer_task = asyncio.create_task(
            self._ws_consumer(),
            name="ws_consumer"
        )
        self._tasks.append(consumer_task)
        
        # Task 3: Alert sender to web
        alerter_task = asyncio.create_task(
            self._alerter_consumer(),
            name="alerter"
        )
        self._tasks.append(alerter_task)
        
        # Task 4: Web server
        web_task = asyncio.create_task(
            run_server(self.config, self.web_api),
            name="web_server"
        )
        self._tasks.append(web_task)
        
        logger.info("All components started")
        logger.info(f"🌐 Web interface available at http://{self.config.WEB_HOST}:{self.config.WEB_PORT}")
        logger.info("Monitoring for anomalies...")
        
        # Wait for shutdown signal
        await self._shutdown_event.wait()
        
        logger.info("Shutdown signal received")
    
    async def shutdown(self) -> None:
        """Graceful shutdown of all components."""
        logger.info("Initiating graceful shutdown...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Stop WebSocket client
        self.ws_client.stop()
        
        # Cancel all tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete
        if self._tasks:
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                task_name = self._tasks[i].get_name()
                if isinstance(result, asyncio.CancelledError):
                    logger.info(f"Task '{task_name}' cancelled successfully")
                elif isinstance(result, Exception):
                    logger.error(f"Task '{task_name}' raised exception: {result}")
                else:
                    logger.info(f"Task '{task_name}' completed")
        
        logger.info("Exited cleanly")
        logger.info("=" * 60)
    
    def run(self) -> None:
        """Start the application and handle signals (Windows compatible)."""
        self._setup_logging()
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Setup signal handlers only on Unix-like systems
        # Windows does not support add_signal_handler for SIGINT/SIGTERM in asyncio
        if sys.platform != "win32":
            def signal_handler(sig):
                logger.info(f"Received signal {sig.name}")
                loop.create_task(self.shutdown())
            
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
                except NotImplementedError:
                    logger.warning(f"Signal handler for {sig.name} not available on this platform")
        else:
            logger.info("Running on Windows - using KeyboardInterrupt for shutdown")
        
        try:
            # Run main application
            loop.run_until_complete(self._run())
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
            loop.run_until_complete(self.shutdown())
        finally:
            # Cleanup
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            
            logger.info("Event loop closed")


def main():
    """Entry point - parses URL query params for symbol selection."""
    # Default symbol
    symbol = "BTCUSDT"
    
    # Check command line args for symbol
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg.startswith("--symbol="):
                symbol = arg.split("=")[1].upper()
            elif arg.startswith("?symbol="):
                symbol = arg.split("=")[1].upper()
    
    logger.info(f"Starting Liquidity Detector for {symbol}")
    detector = LiquidityDetector(symbol=symbol)
    detector.run()


if __name__ == "__main__":
    main()
