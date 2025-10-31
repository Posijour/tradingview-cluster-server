# app.py — исправленный и готовый к бою

import os, time, json, threading, csv, hmac, hashlib, html as _html, re
from datetime import datetime, timedelta
from collections import deque, defaultdict
from time import monotonic
from flask import Flask, request, jsonify
import requests

# =============== 🔧 НАСТРОЙКИ ===============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID", "766363011")

CLUSTER_WINDOW_MIN     = int(os.getenv("CLUSTER_WINDOW_MIN", "60"))
CLUSTER_THRESHOLD      = int(os.getenv("CLUSTER_THRESHOLD", "6"))
CHECK_INTERVAL_SEC     = int(os.getenv("CHECK_INTERVAL_SEC", "60"))
VALID_TF               = os.getenv("VALID_TF", "15m")
WEBHOOK_SECRET         = os.getenv("WEBHOOK_SECRET", "")
CLUSTER_COOLDOWN_SEC   = int(os.getenv("CLUSTER_COOLDOWN_SEC", "300"))

BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL   = os.getenv("BYBIT_BASE_URL", "https://api-testnet.bybit.com")
TRADE_ENABLED    = os.getenv("TRADE_ENABLED", "false").lower() == "true"
MAX_RISK_USDT    = float(os.getenv("MAX_RISK_USDT", "50"))
LEVERAGE         = float(os.getenv("LEVERAGE", "5"))
SYMBOL_WHITELIST = set(s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST","").split(",") if s.strip())

LOG_FILE = "signals_log.csv"

# =============== 🧠 ГЛОБАЛЬНЫЕ СТРУКТУРЫ ===============
signals = deque(maxlen=5000)
lock = threading.Lock()
state_lock = threading.Lock()
log_lock = threading.Lock()
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}
app = Flask(__name__)

tg_times = deque(maxlen=20)
MD_ESCAPE = re.compile(r'([_*\[\]()~`>#+\-=|{}.!])')
def md_escape(text: str) -> str:
    return MD_ESCAPE.sub(r'\\\1', text)
def esc(x): return _html.escape(str(x), quote=True)

# =============== 🔐 HELPERS ===============
def verify_signature(secret, body, signature):
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)

def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    try:
        safe = md_escape(text)
        now = monotonic()
        tg_times.append(now)
        if len(tg_times) >= 2 and now - tg_times[-2] < 1.0:
            time.sleep(1.0 - (now - tg_times[-2]))
        if len(tg_times) == tg_times.maxlen and now - tg_times[0] < 60:
            time.sleep(60 - (now - tg_times[0]))
        threading.Thread(
            target=requests.get,
            kwargs={
                "url": f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                "params": {"chat_id": CHAT_ID, "text": safe, "parse_mode": "MarkdownV2"},
                "timeout": 8,
            },
            daemon=True
        ).start()
    except Exception as e:
        print("❌ Telegram error:", e)

# =============== 📝 ЛОГИРОВАНИЕ ===============
def log_signal(ticker, direction, tf, sig_type, entry=None, stop=None, target=None):
    row = [
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ticker, direction, tf, sig_type,
        entry or "", stop or "", target or ""
    ]
    try:
        with log_lock:
            create_header = not os.path.exists(LOG_FILE)
            with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if create_header:
                    w.writerow(["time_utc","ticker","direction","tf","type","entry","stop","target"])
                w.writerow(row)
        print(f"📝 Logged {sig_type} {ticker} {direction} {tf}")
    except Exception as e:
        print("❌ Log error:", e)

# =============== 🔐 BYBIT HELPERS ===============
def _bybit_sign(payload: dict):
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    body = json.dumps(payload) if payload else ""
    pre_sign = ts + BYBIT_API_KEY + recv_window + body
    sign = hmac.new(BYBIT_API_SECRET.encode(), pre_sign.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
    }
    return headers, body

def bybit_post(path: str, payload: dict) -> dict:
    url = BYBIT_BASE_URL.rstrip("/") + path
    headers, body = _bybit_sign(payload)
    r = requests.post(url, headers=headers, data=body, timeout=10)
    try:
        j = r.json()
    except Exception:
        return {"http": r.status_code, "text": r.text}
    if j.get("retCode", 0) != 0:
        print("❌ Bybit error:", j)
    return j

def normalize_qty(symbol: str, qty: float) -> float:
    try:
        r = requests.get(
            f"{BYBIT_BASE_URL}/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol}, timeout=5
        ).json()
        info = (((r or {}).get("result") or {}).get("list") or [])[0]
        step = float(info.get("lotSizeFilter", {}).get("qtyStep", "0.001"))
        precision = max(0, str(step)[::-1].find('.'))
        normalized = max(step, (qty // step) * step)
        return float(f"{normalized:.{precision}f}")
    except Exception:
        return float(f"{qty:.6f}")

def calc_qty_from_risk(entry: float, stop: float, risk_usdt: float, symbol: str) -> float:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0
    raw_qty = risk_usdt / risk_per_unit
    qty = normalize_qty(symbol, raw_qty)
    return float(f"{qty:.6f}")

def set_leverage(symbol: str, leverage: float):
    payload = {"category":"linear", "symbol":symbol, "buyLeverage":str(leverage), "sellLeverage":str(leverage)}
    return bybit_post("/v5/position/set-leverage", payload)

def place_order_market_with_tp_sl(symbol: str, side: str, qty: float, tp: float, sl: float):
    payload = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "GoodTillCancel",
        "tpSlMode": "Full",
        "takeProfit": str(tp),
        "stopLoss": str(sl),
        "reduceOnly": False
    }
    return bybit_post("/v5/order/create", payload)

def get_atr(symbol: str, period: int = 14, interval: str = "15", limit: int = 100) -> float:
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        r = requests.get(url, params=params, timeout=5).json()
        candles = (((r or {}).get("result") or {}).get("list") or [])
        if not candles:
            return 0.0
        candles.sort(key=lambda c: int(c[0]))
        highs  = [float(c[2]) for c in candles]
        lows   = [float(c[3]) for c in candles]
        closes = [float(c[4]) for c in candles]
        trs = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(highs))]
        if not trs: return 0.0
        lookback = min(period, len(trs))
        return sum(trs[-lookback:]) / lookback
    except Exception as e:
        print("ATR fetch error:", e)
        return 0.0

# =============== 🔍 ПАРСЕР PAYLOAD ===============
def parse_payload(req) -> dict:
    """Поддерживает твой текущий формат JSON из Pine"""
    data = request.get_json(silent=True) or {}
    if not data:
        try:
            data = json.loads(req.data)
        except Exception:
            data = {}
    # стандартные поля твоего Pine
    return {
        "type": data.get("type", "").upper(),
        "ticker": data.get("ticker", "").replace("BYBIT:","").replace(".P","").upper(),
        "direction": data.get("direction", "").upper(),
        "tf": data.get("tf", "15m").lower(),  # если нет — подставляем 15m
        "message": data.get("message", ""),
        "entry": data.get("entry"),
        "stop": data.get("stop"),
        "target": data.get("target"),
    }

# =============== 🔔 ВЕБХУК ===============
@app.route("/webhook", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET:
        key = request.args.get("key", "")
        if key != WEBHOOK_SECRET:
            return "forbidden", 403

    payload = parse_payload(request)
    typ = payload.get("type", "")
    tf  = payload.get("tf", "")
    msg = payload.get("message", "")
    ticker = payload.get("ticker", "")
    direction = payload.get("direction", "")
    entry = payload.get("entry")
    stop = payload.get("stop")
    target = payload.get("target")

    if msg:
        send_telegram(msg)
        print(f"📨 Forwarded MTF alert: {ticker} {direction}")

        # логируем и в очередь
        if ticker and direction in ("UP","DOWN") and tf == VALID_TF:
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))
            log_signal(ticker, direction, tf, "MTF", entry, stop, target)

        # автоторговля
        if TRADE_ENABLED and typ == "MTF":
            try:
                if not (ticker and direction in ("UP","DOWN")):
                    print("⛔ Нет symbol/direction — пропуск торговли")
                elif SYMBOL_WHITELIST and ticker not in SYMBOL_WHITELIST:
                    print(f"⛔ {ticker} не в белом списке — пропуск")
                elif entry and stop and target:
                    side = "Sell" if direction == "UP" else "Buy"
                    set_leverage(ticker, LEVERAGE)
                    qty = calc_qty_from_risk(float(entry), float(stop), MAX_RISK_USDT, ticker)
                    if qty > 0:
                        resp = place_order_market_with_tp_sl(ticker, side, qty, float(target), float(stop))
                        print("Bybit order resp:", resp)
                        send_telegram(f"🚀 *AUTO-TRADE*\n{ticker} {side}\nQty: {qty}\nEntry~{entry}\nTP: {target}\nSL: {stop}")
                else:
                    print("ℹ️ Нет entry/stop/target — пропуск")
            except Exception as e:
                print("Trade error:", e)
        return jsonify({"status": "forwarded"}), 200

    # fallback если нет message
    if typ == "MTF" and tf == VALID_TF:
        if ticker and direction in ("UP", "DOWN"):
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))
            log_signal(ticker, direction, tf, "MTF", entry, stop, target)
            print(f"✅ {ticker} {direction} ({tf}) added for cluster window")
            return jsonify({"status": "ok"}), 200

    return jsonify({"status": "ignored"}), 200

# =============== 🧠 CLUSTER WORKER ===============
def cluster_worker():
    while True:
        try:
            now = time.time()
            cutoff = now - CLUSTER_WINDOW_MIN * 60
            with lock:
                while signals and signals[0][0] < cutoff:
                    signals.popleft()
                ups, downs, tickers_seen = set(), set(), set()
                for (_, t, d, _) in signals:
                    tickers_seen.add(t)
                    if d == "UP": ups.add(t)
                    elif d == "DOWN": downs.add(t)

            with state_lock:
                if len(ups) >= CLUSTER_THRESHOLD and now - last_cluster_sent["UP"] >= CLUSTER_COOLDOWN_SEC:
                    msg = f"🟢 *CLUSTER UP* — {len(ups)} из {len(tickers_seen)} монет (TF {VALID_TF})\n📈 {', '.join(sorted(list(ups)))}"
                    send_telegram(msg)
                    log_signal(",".join(sorted(list(ups))), "UP", VALID_TF, "CLUSTER")
                    last_cluster_sent["UP"] = now
                if len(downs) >= CLUSTER_THRESHOLD and now - last_cluster_sent["DOWN"] >= CLUSTER_COOLDOWN_SEC:
                    msg = f"🔴 *CLUSTER DOWN* — {len(downs)} из {len(tickers_seen)} монет (TF {VALID_TF})\n📉 {', '.join(sorted(list(downs)))}"
                    send_telegram(msg)
                    log_signal(",".join(sorted(list(downs))), "DOWN", VALID_TF, "CLUSTER")
                    last_cluster_sent["DOWN"] = now

            # кластерная автоторговля
            if TRADE_ENABLED:
                try:
                    direction, ticker = None, None
                    if len(ups) >= CLUSTER_THRESHOLD and ups:
                        direction, ticker = "UP", list(ups)[0]
                    elif len(downs) >= CLUSTER_THRESHOLD and downs:
                        direction, ticker = "DOWN", list(downs)[0]
                    if ticker and direction:
                        if SYMBOL_WHITELIST and ticker not in SYMBOL_WHITELIST:
                            print(f"⛔ {ticker} не в белом списке — пропуск кластерной торговли")
                        else:
                            resp = requests.get(
                                f"{BYBIT_BASE_URL}/v5/market/tickers",
                                params={"category": "linear", "symbol": ticker},
                                timeout=5
                            ).json()
                            entry_price = float(resp["result"]["list"][0]["lastPrice"])
                            atr_val = get_atr(ticker, period=14, interval="15")
                            atr_base = get_atr(ticker, period=100, interval="15")
                            vol_scale = max(0.7, min(atr_val / max(atr_base, 0.0001), 1.3))
                            rr_stop = atr_val * 0.8 * vol_scale
                            rr_target = atr_val * 2.4 * vol_scale
                            stop_price = entry_price + rr_stop if direction=="UP" else entry_price - rr_stop
                            target_price = entry_price - rr_target if direction=="UP" else entry_price + rr_target
                            side = "Sell" if direction=="UP" else "Buy"
                            qty = calc_qty_from_risk(entry_price, stop_price, MAX_RISK_USDT, ticker)
                            if qty <= 0:
                                raise ValueError("Qty <= 0 after normalization")
                            resp = place_order_market_with_tp_sl(ticker, side, qty, target_price, stop_price)
                            print(f"💥 Cluster auto-trade {ticker} {side} -> TP:{target_price}, SL:{stop_price}")
                            send_telegram(
                                f"⚡ *CLUSTER AUTO-TRADE*\n{ticker} {side}\nQty: {qty}\n"
                                f"Entry~{entry_price}\nTP: {target_price}\nSL: {stop_price}"
                            )
                except Exception as e:
                    print("❌ Cluster auto-trade error:", e)

            time.sleep(CHECK_INTERVAL_SEC)
        except Exception as e:
            print("💀 cluster_worker crashed:", e)
            time.sleep(10)

# =============== 💙 HEARTBEAT ===============
def heartbeat_loop():
    sent_today = None
    while True:
        try:
            now_utc = datetime.utcnow()
            local_time = now_utc + timedelta(hours=2)
            if local_time.hour == 3 and sent_today != local_time.date():
                msg = f"🩵 *HEARTBEAT*\nServer alive ✅\n⏰ {local_time.strftime('%H:%M %d-%m-%Y')} UTC+2"
                send_telegram(msg)
                sent_today = local_time.date()
        except Exception as e:
            print("❌ Heartbeat error:", e)
        time.sleep(60)

# =============== HEALTH ===============
@app.route("/")
def root(): return "OK", 200
@app.route("/health")
def health(): return "OK", 200

# =============== MAIN ===============
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    threading.Thread(target=cluster_worker, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
