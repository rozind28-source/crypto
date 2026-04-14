"""
Telegram alerter module.
Sends structured alerts to Telegram with rate limiting and retry logic.
"""
import asyncio
from datetime import datetime
from typing import Optional

import httpx
from loguru import logger

from config import Config
from detector import AnomalyAlert


class TelegramAlerter:
    """
    Asynchronous Telegram bot client for sending alerts.
    
    Features:
    - Rate limiting to avoid Telegram API limits
    - Exponential backoff retry on failures
    - Non-blocking operation
    """
    
    def __init__(self, config: Config):
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limit_delay = 0.5  # Minimum delay between messages
        self._max_retries = 3
        self._base_retry_delay = 1.0
        self._last_send_time: Optional[datetime] = None
    
    async def _init_client(self) -> None:
        """Initialize HTTP client if not already done."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=10.0,
                follow_redirects=True,
                headers={"Content-Type": "application/json"}
            )
    
    async def _send_request(self, message: str) -> bool:
        """
        Send message to Telegram API with retry logic.
        
        Returns True if successful, False otherwise.
        """
        await self._init_client()
        
        url = f"https://api.telegram.org/bot{self.config.TELEGRAM_BOT_TOKEN}/sendMessage"
        
        payload = {
            "chat_id": self.config.TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        for attempt in range(self._max_retries):
            try:
                response = await self._client.post(url, json=payload)
                
                if response.status_code == 200:
                    logger.debug("Telegram message sent successfully")
                    return True
                
                elif response.status_code == 429:
                    # Rate limited - extract retry-after from response
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        delay = float(retry_after)
                    else:
                        delay = self._base_retry_delay * (2 ** attempt)
                    
                    logger.warning(f"Telegram rate limited, waiting {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                
                elif 500 <= response.status_code < 600:
                    # Server error - retry with backoff
                    delay = self._base_retry_delay * (2 ** attempt)
                    logger.warning(f"Telegram server error {response.status_code}, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                
                else:
                    # Client error - don't retry
                    logger.error(f"Telegram API error {response.status_code}: {response.text}")
                    return False
                    
            except httpx.TimeoutException:
                delay = self._base_retry_delay * (2 ** attempt)
                logger.warning(f"HTTP timeout, retrying in {delay}s...")
                await asyncio.sleep(delay)
                
            except httpx.RequestError as e:
                logger.error(f"HTTP request error: {e}")
                return False
            
            except Exception as e:
                logger.error(f"Unexpected error sending Telegram message: {e}")
                return False
        
        logger.error(f"Failed to send Telegram message after {self._max_retries} attempts")
        return False
    
    def _format_alert(self, alert: AnomalyAlert) -> str:
        """Format anomaly alert as Telegram message."""
        # Build emoji based on anomaly type
        if alert.anomaly_type == "DIVERGENCE":
            emoji = "📊"
            type_emoji = "🔍"
        elif alert.anomaly_type == "SPOOFING":
            emoji = "🎭"
            type_emoji = "🔍"
        else:
            emoji = "⚠️"
            type_emoji = "🔍"
        
        # Format details
        details_lines = []
        for key, value in alert.details.items():
            if isinstance(value, float):
                if "usdt" in key.lower() or "size" in key.lower():
                    details_lines.append(f"• {key.replace('_', ' ').title()}: ${value:,.2f}")
                elif "pct" in key.lower() or "sec" in key.lower():
                    details_lines.append(f"• {key.replace('_', ' ').title()}: {value:.3f}")
                elif "price" in key.lower():
                    details_lines.append(f"• {key.replace('_', ' ').title()}: ${value:,.2f}")
                else:
                    details_lines.append(f"• {key.replace('_', ' ').title()}: {value:.4f}")
            else:
                details_lines.append(f"• {key.replace('_', ' ').title()}: {value}")
        
        details_text = "\n".join(details_lines)
        
        # TradingView link
        chart_link = f"https://tradingview.com/chart/?symbol=BYBIT:{alert.symbol}"
        
        # Build message
        message = (
            f"{emoji} <b>ЛИКВИДНОСТЬ: {alert.symbol}</b>\n\n"
            f"{type_emoji} <b>Аномалия:</b> {alert.anomaly_type}\n\n"
            f"{details_text}\n\n"
            f"⏱ <b>Время:</b> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            f"🔗 <a href='{chart_link}'>Открыть график</a>"
        )
        
        return message
    
    async def send_alert(self, alert: AnomalyAlert) -> bool:
        """
        Send alert to Telegram with rate limiting.
        
        Returns True if successful, False otherwise.
        """
        try:
            # Rate limiting
            if self._last_send_time:
                elapsed = (datetime.utcnow() - self._last_send_time).total_seconds()
                if elapsed < self._rate_limit_delay:
                    await asyncio.sleep(self._rate_limit_delay - elapsed)
            
            message = self._format_alert(alert)
            success = await self._send_request(message)
            
            if success:
                self._last_send_time = datetime.utcnow()
                logger.info(f"Alert sent: {alert.anomaly_type} for {alert.symbol}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending alert: {e}")
            return False
    
    async def send_test_message(self) -> bool:
        """Send a test message to verify Telegram configuration."""
        test_alert = AnomalyAlert(
            anomaly_type="TEST",
            symbol=self.config.SYMBOL,
            timestamp=datetime.utcnow(),
            details={"status": "configured"},
            message="✅ Liquidity Detector initialized successfully"
        )
        return await self.send_alert(test_alert)
    
    async def close(self) -> None:
        """Close HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            logger.info("Telegram client closed")


async def alerter_worker(
    alerter: TelegramAlerter,
    alert_queue: asyncio.Queue[AnomalyAlert],
    shutdown_event: asyncio.Event
) -> None:
    """
    Worker coroutine that processes alerts from queue and sends to Telegram.
    
    Runs until shutdown_event is set.
    """
    logger.info("Alerter worker started")
    
    while not shutdown_event.is_set():
        try:
            # Wait for alert with timeout to check shutdown
            try:
                alert = await asyncio.wait_for(alert_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            
            # Send alert
            success = await alerter.send_alert(alert)
            
            if not success:
                logger.warning(f"Failed to send alert: {alert.anomaly_type}")
            
            # Mark task as done
            alert_queue.task_done()
            
        except asyncio.CancelledError:
            logger.info("Alerter worker cancelled")
            break
        except Exception as e:
            logger.error(f"Alerter worker error: {e}")
            await asyncio.sleep(1.0)
    
    logger.info("Alerter worker stopped")
