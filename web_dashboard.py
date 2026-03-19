import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

active_connections = set()

async def consume_tcp_stream():
    """Background task: Reads from Rust IPC and broadcasts to all WebSocket clients."""
    while True:
        try:
            reader, _ = await asyncio.open_connection('127.0.0.1', 9000)
            print("FastAPI connected to Rust Engine IPC.")
            while True:
                line = await reader.readline()
                if not line:
                    break
                
                msg = line.decode('utf-8').strip()
                
                # Fan-out message to active WS clients
                disconnected_clients = set()
                for connection in active_connections:
                    try:
                        await connection.send_text(msg)
                    except Exception:
                        disconnected_clients.add(connection)
                
                active_connections.difference_update(disconnected_clients)
                
        except Exception as e:
            print(f"FastAPI IPC connection error: {e}")
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_tcp_stream())
    yield
    task.cancel()

app = FastAPI(title="Bongus Web Dashboard", lifespan=lifespan)

HTML_CONTENT = """
<!DOCTYPE html>
<html>
<head>
    <title>Bongus Multi-Asset Arbitrage Orchestrator</title>
    <style>
        body { background: #111827; color: #fff; font-family: sans-serif; padding: 20px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .panel { background: #1f2937; padding: 20px; border-radius: 8px; }
        table { border-collapse: collapse; width: 100%; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #374151; font-variant-numeric: tabular-nums; }
        th { color: #9ca3af; font-weight: normal; }
        .event-log { font-family: monospace; font-size: 0.85em; background: #111827; padding: 10px; border-radius: 4px; overflow-y: auto; height: 180px; }
        .event-log div { margin-bottom: 4px; padding-bottom: 2px; border-bottom: 1px solid #1f2937; }
        .status-pill { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 0.85em; background: #374151; }
    </style>
</head>
<body>
    <div style="background: #1f2937; padding: 20px; border-radius: 8px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center;">
        <div>
            <h1 style="margin: 0; color: #e0e0e0;">Total PnL: <span id="total-pnl" style="color: #4ade80;">$0.00</span></h1>
            <div id="ws-status" style="color: #60a5fa; margin-top: 10px;">Connecting to engine...</div>
        </div>
        <div style="text-align: right;">
            <div style="color: #9ca3af; font-size: 0.9em;">BTCUSDT Ann. Funding</div>
            <div id="funding-rate" style="font-size: 1.5em; color: #fbbf24;">Loading...</div>
        </div>
    </div>

    <div class="grid">
        <div class="panel" style="overflow: hidden; display: flex; flex-direction: column;">
            <h3 style="margin-top: 0; display: flex; justify-content: space-between;">
                Market Prices <span class="status-pill">Updates: <span id="count-bookticker">0</span></span>
            </h3>
            <div style="overflow-y: auto; flex-grow: 1; max-height: 200px;">
                <table>
                    <thead><tr><th>Symbol</th><th>Bid</th><th>Ask</th></tr></thead>
                    <tbody id="price-details"><tr><td colspan="3" style="text-align:center; color:#9ca3af;">Awaiting BookTicker data...</td></tr></tbody>
                </table>
            </div>
        </div>

        <div class="panel">
            <h3 style="margin-top: 0; display: flex; justify-content: space-between;">
                Event Log & Orders <span class="status-pill">Events: <span id="count-events">0</span></span>
            </h3>
            <div class="event-log" id="order-log">
                <div style="color:#9ca3af;" id="empty-log-msg">No recent order or engine events...</div>
            </div>
        </div>
    </div>

    <details class="panel" style="display: block; cursor: pointer;" open>
        <summary style="font-size: 1.2em; color: #60a5fa; outline: none; margin-bottom: 10px;">
            Detailed Asset PnL <span class="status-pill" style="float:right;">Updates: <span id="count-account">0</span></span>
        </summary>
        <table>
            <thead><tr><th>Asset</th><th>Current Balance</th><th>Calculated PnL</th></tr></thead>
            <tbody id="pnl-details">
                <tr><td colspan="3" style="text-align:center; color:#facc15; padding: 20px;">No AccountUpdate received yet. Balances will populate upon exchange update.</td></tr>
            </tbody>
        </table>
    </details>

    <script>
        let initialBalances = null;
        let currentBalances = {};
        let prices = {};
        let counters = { bookTicker: 0, account: 0, events: 0 };
        
        const protocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        const ws = new WebSocket(protocol + window.location.host + "/ws");

        ws.onopen = () => { document.getElementById("ws-status").innerText = "● Live execution stream active"; };
        ws.onclose = () => { document.getElementById("ws-status").innerText = "○ Disconnected from stream"; };

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            if (data.event === "BookTicker") {
                counters.bookTicker++;
                document.getElementById("count-bookticker").innerText = counters.bookTicker;
                prices[data.symbol] = { bid: data.bid_price, ask: data.ask_price };
                updatePrices();
            }
            else if (data.event === "AccountUpdate") {
                counters.account++;
                document.getElementById("count-account").innerText = counters.account;
                
                currentBalances = data.balances;
                if (!initialBalances) {
                    initialBalances = JSON.parse(JSON.stringify(data.balances));
                }

                let totalPnl = 0.0;
                let pnlHtml = "";

                for (const [asset, bal] of Object.entries(currentBalances)) {
                    let startBal = initialBalances[asset] || 0.0;
                    let diff = bal - startBal;
                    
                    if (asset === "USDT") { totalPnl += diff; } 

                    let color = diff >= 0 ? "#4ade80" : "#f87171";
                    let sign = diff > 0 ? '+' : '';
                    pnlHtml += `<tr>
                        <td>${asset}</td>
                        <td>${bal.toFixed(4)}</td>
                        <td style="color: ${color}">${sign}${diff.toFixed(4)}</td>
                    </tr>`;
                }

                let totalColor = totalPnl >= 0 ? "#4ade80" : "#f87171";
                let totalSign = totalPnl > 0 ? '+' : '';
                document.getElementById("total-pnl").innerText = `${totalSign}$${totalPnl.toFixed(2)}`;
                document.getElementById("total-pnl").style.color = totalColor;
                document.getElementById("pnl-details").innerHTML = pnlHtml;
            }
            else if (
                data.event === "OrderUpdate" ||
                data.event === "Connected" ||
                data.event === "Disconnected" ||
                data.event === "OrderPlaced" ||
                data.event === "OrderPlacementFailed" ||
                data.event === "ReconciliationStarted" ||
                data.event === "ReconciliationCompleted" ||
                data.event === "ReconciliationFallback" ||
                data.event === "AlphaIgnored" ||
                data.event === "ChaseReset"
            ) {
                counters.events++;
                document.getElementById("count-events").innerText = counters.events;
                
                const logEl = document.getElementById("order-log");
                const emptyMsg = document.getElementById("empty-log-msg");
                if (emptyMsg) emptyMsg.remove();
                
                const timeStr = new Date().toLocaleTimeString();
                const div = document.createElement("div");
                
                if (data.event === "OrderUpdate") {
                    div.innerText = `[${timeStr}] ${data.symbol}: ${data.status} (fill: ${data.filled_qty}) - ${data.client_order_id}`;
                    if (data.status === "FILLED") div.style.color = "#4ade80";
                    else if (data.status === "CANCELED" || data.status === "EXPIRED") div.style.color = "#f87171";
                    else div.style.color = "#fbbf24";
                } else if (data.event === "OrderPlaced") {
                    div.innerText = `[${timeStr}] ORDER PLACED: ${data.symbol} ${data.side} qty=${data.quantity} (${data.leg})`;
                    div.style.color = "#60a5fa";
                } else if (data.event === "OrderPlacementFailed") {
                    div.innerText = `[${timeStr}] ORDER FAILED: ${data.symbol || "?"} (${data.leg}) cid=${data.client_order_id || "n/a"}`;
                    div.style.color = "#f87171";
                } else if (data.event === "ReconciliationStarted") {
                    div.innerText = `[${timeStr}] ENGINE: Reconciliation started`;
                    div.style.color = "#fbbf24";
                } else if (data.event === "ReconciliationCompleted") {
                    div.innerText = `[${timeStr}] ENGINE: Reconciliation completed (${data.mode})`;
                    div.style.color = "#4ade80";
                } else if (data.event === "ReconciliationFallback") {
                    div.innerText = `[${timeStr}] ENGINE: Reconciliation fallback (${data.reason})`;
                    div.style.color = "#f87171";
                } else if (data.event === "AlphaIgnored") {
                    div.innerText = `[${timeStr}] ALPHA IGNORED: ${data.reason} (${data.state}) ${data.symbol}`;
                    div.style.color = "#fbbf24";
                } else if (data.event === "ChaseReset") {
                    div.innerText = `[${timeStr}] ENGINE: Chase reset (${data.reason})`;
                    div.style.color = "#f87171";
                } else {
                    div.innerText = `[${timeStr}] ENGINE: ${data.event} - ${data.symbol}`;
                    div.style.color = data.event === "Connected" ? "#60a5fa" : "#f87171";
                }
                
                logEl.prepend(div);
                if (logEl.children.length > 100) logEl.lastChild.remove();
            }
        };

        function updatePrices() {
            let html = "";
            const sortedSymbols = Object.keys(prices).sort();
            for (const sym of sortedSymbols) {
                html += `<tr>
                    <td>${sym}</td>
                    <td style="color:#4ade80">${prices[sym].bid.toFixed(2)}</td>
                    <td style="color:#f87171">${prices[sym].ask.toFixed(2)}</td>
                </tr>`;
            }
            document.getElementById("price-details").innerHTML = html;
        }

        async function fetchFunding() {
            try {
                const res = await fetch("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT");
                const data = await res.json();
                const fundRate = parseFloat(data.lastFundingRate);
                const annFunding = fundRate * 3 * 365 * 100;
                document.getElementById("funding-rate").innerText = annFunding.toFixed(2) + "%";
            } catch (e) {
                console.error("Funding fetch error", e);
            }
        }
        setInterval(fetchFunding, 10000);
        fetchFunding();
    </script>
</body>
</html>
"""

@app.get("/")
async def get_dashboard():
    return HTMLResponse(HTML_CONTENT)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)

if __name__ == "__main__":
    import uvicorn
    # Make sure we don't block the ports
    uvicorn.run("web_dashboard:app", host="127.0.0.1", port=8000, reload=False)