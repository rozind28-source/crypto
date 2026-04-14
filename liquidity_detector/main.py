"""
Main entry point for the Liquidity Detector application.
Orchestrates all components and handles graceful shutdown.
"""
import asyncio
import signal
from datetime import datetime
from typing import List, Optional

from loguru import logger

from config import config
from ws_client import BybitWSClient
from normalizer import Normalizer, Trade, NormalizedOrderBook
from detector import AnomalyDetector
from alerter import TelegramAlerter, alerter_worker


class LiquidityDetector:
    """
    Main orchestrator for the liquidity anomaly detection system.
    
    Coordinates:
    - WebSocket client for market data
    - Normalizer for data parsing
    - Detector for anomaly detection
    - Alerter for Telegram notifications
    """
    
    def __init__(self):
        self.config = config
        self.ws_client = BybitWSClient(self.config)
        self.normalizer = Normalizer()
        self.detector = AnomalyDetector(self.config)
        self.alerter = TelegramAlerter(self.config)
        
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
                
                elif msg_type == "trade":
                    # Parse each trade in the batch
                    for trade_data in raw_data:
                        trade = self.normalizer.parse_trade(trade_data, received_at)
                        if trade:
                            await self.detector.process_trade(trade)
                
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
        """Run the alerter worker to send alerts to Telegram."""
        await alerter_worker(
            self.alerter,
            self.detector.alert_queue,
            self._shutdown_event
        )
    
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
        
        # Send test message to verify Telegram configuration
        try:
            logger.info("Sending test message to Telegram...")
            test_sent = await self.alerter.send_test_message()
            if test_sent:
                logger.info("✅ Telegram configured successfully")
            else:
                logger.warning("⚠️ Failed to send test message - check Telegram credentials")
        except Exception as e:
            logger.warning(f"⚠️ Telegram test failed: {e}")
        
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
        
        # Task 3: Alert sender
        alerter_task = asyncio.create_task(
            self._alerter_consumer(),
            name="alerter"
        )
        self._tasks.append(alerter_task)
        
        logger.info("All components started")
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
        
        # Close alerter HTTP client
        await self.alerter.close()
        
        logger.info("Exited cleanly")
        logger.info("=" * 60)
    
    def run(self) -> None:
        """Start the application and handle signals."""
        self._setup_logging()
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Setup signal handlers
        def signal_handler(sig):
            logger.info(f"Received signal {sig.name}")
            loop.create_task(self.shutdown())
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
        
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
    """Entry point."""
    detector = LiquidityDetector()
    detector.run()


if __name__ == "__main__":
    main()
