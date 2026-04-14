"""
Web API module providing REST and WebSocket endpoints for the liquidity detector.
Serves real-time anomaly alerts and market data to the web interface.
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from loguru import logger

from config import Config
from detector import AnomalyAlert


class AlertResponse(BaseModel):
    """API response model for anomaly alerts."""
    anomaly_type: str
    symbol: str
    timestamp: str
    details: Dict[str, Any]
    message: str


class MarketDataResponse(BaseModel):
    """API response model for current market data."""
    symbol: str
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid_price: Optional[float] = None
    last_trade_price: Optional[float] = None
    last_trade_size: Optional[float] = None
    delta_value: Optional[float] = None
    window_trades_count: int = 0
    tracked_orders_count: int = 0
    updated_at: str


class WebAPI:
    """
    FastAPI-based web server for the liquidity detector.
    Provides REST endpoints and WebSocket for real-time updates.
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.app = FastAPI(title="Liquidity Detector API", version="1.0.0")
        
        # Shared state for market data
        self._market_data: Dict[str, Any] = {
            "symbol": config.SYMBOL,
            "best_bid": None,
            "best_ask": None,
            "mid_price": None,
            "last_trade_price": None,
            "last_trade_size": None,
            "delta_value": None,
            "window_trades_count": 0,
            "tracked_orders_count": 0,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Connected WebSocket clients
        self._websocket_clients: Set[WebSocket] = set()
        
        # Alert history (last 100 alerts)
        self._alert_history: List[AlertResponse] = []
        self._max_history = 100
        
        # Reference to detector for accessing internal state
        self._detector = None
        
        # Callback for symbol change
        self._on_symbol_change = None
        
        self._setup_routes()
    
    def set_detector(self, detector) -> None:
        """Set reference to detector for accessing internal state."""
        self._detector = detector
    
    def set_symbol_change_callback(self, callback) -> None:
        """Set callback function for handling symbol changes."""
        self._on_symbol_change = callback
    
    def _setup_routes(self) -> None:
        """Configure FastAPI routes."""
        
        @self.app.get("/", response_class=HTMLResponse)
        async def get_index():
            """Serve the main HTML interface."""
            try:
                return FileResponse("static/index.html")
            except FileNotFoundError:
                # Return inline HTML if file not found
                return HTMLResponse(content=self._get_inline_html(), status_code=200)
        
        @self.app.get("/api/alerts", response_model=List[AlertResponse])
        async def get_alerts(limit: int = Query(default=50, ge=1, le=100)):
            """Get recent anomaly alerts."""
            return self._alert_history[-limit:]
        
        @self.app.get("/api/market", response_model=MarketDataResponse)
        async def get_market_data():
            """Get current market data."""
            # Update from detector if available
            if self._detector:
                self._update_market_data_from_detector()
            return MarketDataResponse(**self._market_data)
        
        @self.app.get("/api/symbols")
        async def get_available_symbols():
            """Get list of popular trading symbols."""
            return {
                "symbols": [
                    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
                    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "LINKUSDT",
                    "MATICUSDT", "DOTUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT"
                ]
            }
        
        @self.app.post("/api/symbol")
        async def set_symbol(symbol_data: Dict[str, str]):
            """Change the current trading symbol."""
            new_symbol = symbol_data.get("symbol", "").upper()
            if not new_symbol:
                raise HTTPException(status_code=400, detail="Symbol is required")
            
            # Validate symbol format
            if not new_symbol.endswith("USDT"):
                raise HTTPException(status_code=400, detail="Symbol must end with USDT")
            
            # Update config and restart detector via callback
            if self._on_symbol_change:
                await self._on_symbol_change(new_symbol)
            
            self.config.SYMBOL = new_symbol
            self._market_data["symbol"] = new_symbol
            
            logger.info(f"Symbol changed to {new_symbol}")
            return {"status": "success", "symbol": new_symbol}
        
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            """WebSocket endpoint for real-time alert streaming."""
            await websocket.accept()
            self._websocket_clients.add(websocket)
            logger.info(f"WebSocket client connected. Total clients: {len(self._websocket_clients)}")
            
            try:
                # Send initial market data
                await websocket.send_json({
                    "type": "market_data",
                    "data": self._market_data
                })
                
                # Send recent alerts
                for alert in self._alert_history[-10:]:
                    await websocket.send_json({
                        "type": "alert",
                        "data": alert.model_dump()
                    })
                
                # Keep connection alive
                while True:
                    try:
                        # Wait for any message from client (heartbeat)
                        await asyncio.wait_for(
                            websocket.receive_text(),
                            timeout=30.0
                        )
                    except asyncio.TimeoutError:
                        # Send heartbeat
                        await websocket.send_json({"type": "ping"})
                        
            except WebSocketDisconnect:
                logger.info("WebSocket client disconnected")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            finally:
                self._websocket_clients.discard(websocket)
                logger.info(f"WebSocket client removed. Total clients: {len(self._websocket_clients)}")
        
        # Mount static files
        try:
            self.app.mount("/static", StaticFiles(directory="static"), name="static")
        except RuntimeError:
            # Directory doesn't exist yet - will use inline HTML
            pass
    
    def _update_market_data_from_detector(self) -> None:
        """Update market data from detector's internal state."""
        if not self._detector:
            return
        
        self._market_data["symbol"] = self.config.SYMBOL
        self._market_data["window_trades_count"] = len(self._detector._trade_window)
        self._market_data["tracked_orders_count"] = len(self._detector._orderbook_state)
        
        if self._detector._trade_window:
            # Calculate current delta
            total_buy = sum(p.buy_value for p in self._detector._trade_window)
            total_sell = sum(p.sell_value for p in self._detector._trade_window)
            self._market_data["delta_value"] = round(total_buy - total_sell, 2)
        
        self._market_data["updated_at"] = datetime.utcnow().isoformat()
    
    async def publish_alert(self, alert: AnomalyAlert) -> None:
        """Publish alert to all connected WebSocket clients and store in history."""
        alert_response = AlertResponse(
            anomaly_type=alert.anomaly_type,
            symbol=alert.symbol,
            timestamp=alert.timestamp.isoformat(),
            details=alert.details,
            message=alert.message
        )
        
        # Add to history
        self._alert_history.append(alert_response)
        if len(self._alert_history) > self._max_history:
            self._alert_history.pop(0)
        
        # Broadcast to all WebSocket clients
        if self._websocket_clients:
            message = {
                "type": "alert",
                "data": alert_response.model_dump()
            }
            disconnected = set()
            
            for client in self._websocket_clients:
                try:
                    await client.send_json(message)
                except Exception:
                    disconnected.add(client)
            
            # Remove disconnected clients
            for client in disconnected:
                self._websocket_clients.discard(client)
    
    async def update_market_data(self, **kwargs) -> None:
        """Update market data and broadcast to WebSocket clients."""
        for key, value in kwargs.items():
            if key in self._market_data:
                self._market_data[key] = value
        
        self._market_data["updated_at"] = datetime.utcnow().isoformat()
        
        # Broadcast to WebSocket clients periodically (not on every update to avoid spam)
        if self._websocket_clients:
            message = {
                "type": "market_data",
                "data": self._market_data.copy()
            }
            # Only send to a subset or throttle - sending on every update is too much
            # We'll let clients poll via REST for frequent updates
    
    def _get_inline_html(self) -> str:
        """Return inline HTML interface (fallback if static file not found)."""
        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Liquidity Anomaly Detector</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #eee;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding: 20px;
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            backdrop-filter: blur(10px);
        }
        h1 { font-size: 1.8em; background: linear-gradient(90deg, #00d9ff, #00ff88); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .controls { display: flex; gap: 15px; align-items: center; }
        select, button {
            padding: 10px 20px;
            border: none;
            border-radius: 8px;
            font-size: 14px;
            cursor: pointer;
            transition: all 0.3s;
        }
        select {
            background: rgba(255,255,255,0.1);
            color: #fff;
            border: 1px solid rgba(255,255,255,0.2);
        }
        select option { background: #1a1a2e; color: #fff; }
        button {
            background: linear-gradient(90deg, #00d9ff, #00ff88);
            color: #000;
            font-weight: bold;
        }
        button:hover { transform: translateY(-2px); box-shadow: 0 5px 20px rgba(0,217,255,0.4); }
        .dashboard { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .card {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255,255,255,0.1);
        }
        .card h2 { font-size: 1.2em; margin-bottom: 15px; color: #00d9ff; }
        .metric { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.1); }
        .metric:last-child { border-bottom: none; }
        .metric-label { color: #aaa; }
        .metric-value { font-weight: bold; font-size: 1.1em; }
        .metric-value.positive { color: #00ff88; }
        .metric-value.negative { color: #ff4757; }
        .alerts-section { margin-top: 20px; }
        .alert {
            background: rgba(255,255,255,0.05);
            border-left: 4px solid;
            padding: 15px;
            margin-bottom: 10px;
            border-radius: 0 8px 8px 0;
            animation: slideIn 0.3s ease;
        }
        @keyframes slideIn { from { transform: translateX(-20px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        .alert.DIVERGENCE { border-color: #ffa502; }
        .alert.SPOOFING { border-color: #ff4757; }
        .alert-header { display: flex; justify-content: space-between; margin-bottom: 10px; }
        .alert-type { font-weight: bold; font-size: 0.9em; }
        .alert-time { color: #666; font-size: 0.85em; }
        .alert-message { color: #ddd; margin-bottom: 8px; }
        .alert-details { display: flex; gap: 15px; flex-wrap: wrap; font-size: 0.85em; color: #aaa; }
        .badge { padding: 3px 8px; border-radius: 4px; font-size: 0.8em; }
        .badge.DIVERGENCE { background: rgba(255,165,2,0.2); color: #ffa502; }
        .badge.SPOOFING { background: rgba(255,71,87,0.2); color: #ff4757; }
        .no-alerts { text-align: center; color: #666; padding: 40px; }
        .status-indicator { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }
        .status-connected { background: #00ff88; box-shadow: 0 0 10px #00ff88; }
        .status-disconnected { background: #ff4757; }
        .full-width { grid-column: 1 / -1; }
        @media (max-width: 768px) { .dashboard { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 Liquidity Anomaly Detector</h1>
            <div class="controls">
                <span><span class="status-indicator" id="statusIndicator"></span><span id="statusText">Connecting...</span></span>
                <select id="symbolSelect"></select>
                <button onclick="changeSymbol()">Change Symbol</button>
            </div>
        </header>
        
        <div class="dashboard">
            <div class="card">
                <h2>📈 Market Data - <span id="marketSymbol">-</span></h2>
                <div class="metric"><span class="metric-label">Best Bid</span><span class="metric-value" id="bestBid">-</span></div>
                <div class="metric"><span class="metric-label">Best Ask</span><span class="metric-value" id="bestAsk">-</span></div>
                <div class="metric"><span class="metric-label">Mid Price</span><span class="metric-value" id="midPrice">-</span></div>
                <div class="metric"><span class="metric-label">Last Trade</span><span class="metric-value" id="lastTrade">-</span></div>
                <div class="metric"><span class="metric-label">Volume Delta</span><span class="metric-value" id="deltaValue">-</span></div>
                <div class="metric"><span class="metric-label">Trades in Window</span><span class="metric-value" id="tradesCount">0</span></div>
                <div class="metric"><span class="metric-label">Tracked Orders</span><span class="metric-value" id="trackedOrders">0</span></div>
                <div class="metric"><span class="metric-label">Last Update</span><span class="metric-value" id="lastUpdate">-</span></div>
            </div>
            
            <div class="card">
                <h2>⚙️ Detection Settings</h2>
                <div class="metric"><span class="metric-label">Delta Window</span><span class="metric-value" id="deltaWindow">60s</span></div>
                <div class="metric"><span class="metric-label">Delta Threshold</span><span class="metric-value">$50,000</span></div>
                <div class="metric"><span class="metric-label">Price Change Max</span><span class="metric-value">0.5%</span></div>
                <div class="metric"><span class="metric-label">Spoof Size Min</span><span class="metric-value">$100,000</span></div>
                <div class="metric"><span class="metric-label">Spoof Lifetime Max</span><span class="metric-value">0.5s</span></div>
            </div>
            
            <div class="card full-width alerts-section">
                <h2>🔔 Real-time Alerts</h2>
                <div id="alertsContainer">
                    <div class="no-alerts">Waiting for anomalies...</div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let ws = null;
        let currentSymbol = 'BTCUSDT';
        
        // Load available symbols
        async function loadSymbols() {
            try {
                const resp = await fetch('/api/symbols');
                const data = await resp.json();
                const select = document.getElementById('symbolSelect');
                select.innerHTML = '';
                data.symbols.forEach(sym => {
                    const opt = document.createElement('option');
                    opt.value = sym;
                    opt.textContent = sym;
                    if (sym === currentSymbol) opt.selected = true;
                    select.appendChild(opt);
                });
            } catch (e) { console.error('Failed to load symbols:', e); }
        }
        
        function changeSymbol() {
            const select = document.getElementById('symbolSelect');
            const newSymbol = select.value;
            if (newSymbol && newSymbol !== currentSymbol) {
                // Call API to change symbol without page reload
                fetch('/api/symbol', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ symbol: newSymbol })
                })
                .then(async resp => {
                    if (!resp.ok) {
                        const err = await resp.json();
                        throw new Error(err.detail || 'Failed to change symbol');
                    }
                    return resp.json();
                })
                .then(data => {
                    console.log('Symbol changed:', data);
                    currentSymbol = data.symbol;
                    // Clear alerts and reset UI for new symbol
                    document.getElementById('alertsContainer').innerHTML = '<div class="no-alerts">Waiting for anomalies...</div>';
                    updateMarketData({
                        best_bid: null, best_ask: null, mid_price: null,
                        last_trade_price: null, delta_value: null,
                        window_trades_count: 0, tracked_orders_count: 0
                    });
                })
                .catch(err => {
                    console.error('Error changing symbol:', err);
                    alert('Error: ' + err.message);
                });
            }
        }
        
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onopen = () => {
                document.getElementById('statusIndicator').className = 'status-indicator status-connected';
                document.getElementById('statusText').textContent = 'Connected';
                console.log('WebSocket connected');
            };
            
            ws.onclose = () => {
                document.getElementById('statusIndicator').className = 'status-indicator status-disconnected';
                document.getElementById('statusText').textContent = 'Disconnected';
                console.log('WebSocket disconnected, reconnecting...');
                setTimeout(connectWebSocket, 3000);
            };
            
            ws.onerror = (e) => { console.error('WebSocket error:', e); };
            
            ws.onmessage = (event) => {
                const msg = JSON.parse(event.data);
                if (msg.type === 'market_data') {
                    updateMarketData(msg.data);
                } else if (msg.type === 'alert') {
                    addAlert(msg.data);
                } else if (msg.type === 'ping') {
                    ws.send('pong');
                }
            };
        }
        
        function updateMarketData(data) {
            document.getElementById('marketSymbol').textContent = data.symbol || currentSymbol;
            document.getElementById('bestBid').textContent = data.best_bid ? `$${data.best_bid.toFixed(2)}` : '-';
            document.getElementById('bestAsk').textContent = data.best_ask ? `$${data.best_ask.toFixed(2)}` : '-';
            document.getElementById('midPrice').textContent = data.mid_price ? `$${data.mid_price.toFixed(2)}` : '-';
            document.getElementById('lastTrade').textContent = data.last_trade_price ? `$${data.last_trade_price.toFixed(2)} (${data.last_trade_size})` : '-';
            
            const deltaEl = document.getElementById('deltaValue');
            if (data.delta_value !== null && data.delta_value !== undefined) {
                deltaEl.textContent = `${data.delta_value >= 0 ? '+' : ''}$${Math.abs(data.delta_value).toLocaleString()}`;
                deltaEl.className = 'metric-value ' + (data.delta_value >= 0 ? 'positive' : 'negative');
            } else {
                deltaEl.textContent = '-';
                deltaEl.className = 'metric-value';
            }
            
            document.getElementById('tradesCount').textContent = data.window_trades_count || 0;
            document.getElementById('trackedOrders').textContent = data.tracked_orders_count || 0;
            document.getElementById('lastUpdate').textContent = data.updated_at ? new Date(data.updated_at).toLocaleTimeString() : '-';
        }
        
        function addAlert(alertData) {
            const container = document.getElementById('alertsContainer');
            const noAlerts = container.querySelector('.no-alerts');
            if (noAlerts) noAlerts.remove();
            
            const alertEl = document.createElement('div');
            alertEl.className = `alert ${alertData.anomaly_type}`;
            
            const time = new Date(alertData.timestamp).toLocaleString();
            alertEl.innerHTML = `
                <div class="alert-header">
                    <span class="alert-type"><span class="badge ${alertData.anomaly_type}">${alertData.anomaly_type}</span></span>
                    <span class="alert-time">${time}</span>
                </div>
                <div class="alert-message">${alertData.message}</div>
                <div class="alert-details">
                    ${Object.entries(alertData.details).map(([k,v]) => `<span>${k}: ${typeof v === 'number' ? v.toLocaleString() : v}</span>`).join('')}
                </div>
            `;
            
            container.insertBefore(alertEl, container.firstChild);
            
            // Keep only last 20 alerts in DOM
            while (container.children.length > 20) {
                container.removeChild(container.lastChild);
            }
        }
        
        // Load market data on page load
        async function loadMarketData() {
            try {
                const resp = await fetch('/api/market');
                const data = await resp.json();
                updateMarketData(data);
            } catch (e) { console.error('Failed to load market data:', e); }
        }
        
        // Initialize
        document.addEventListener('DOMContentLoaded', () => {
            // Get symbol from URL params
            const urlParams = new URLSearchParams(window.location.search);
            const symbolParam = urlParams.get('symbol');
            if (symbolParam) {
                currentSymbol = symbolParam;
                document.getElementById('symbolSelect').value = symbolParam;
            }
            
            loadSymbols();
            loadMarketData();
            connectWebSocket();
            
            // Refresh market data every 5 seconds
            setInterval(loadMarketData, 5000);
        });
    </script>
</body>
</html>"""


# Global API instance
api: Optional[WebAPI] = None


def create_api(config: Config) -> WebAPI:
    """Create and configure the WebAPI instance."""
    global api
    api = WebAPI(config)
    return api


async def run_server(config: Config, api_instance: WebAPI) -> None:
    """Run the FastAPI server using uvicorn."""
    import uvicorn
    
    # Configure uvicorn
    uv_config = uvicorn.Config(
        app=api_instance.app,
        host=config.WEB_HOST,
        port=config.WEB_PORT,
        log_level="info",
        access_log=False
    )
    server = uvicorn.Server(uv_config)
    
    logger.info(f"Starting web server on http://{config.WEB_HOST}:{config.WEB_PORT}")
    await server.serve()
