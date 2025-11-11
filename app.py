# app.py — минимизированный сервер автотрейда (только SCALP)

import os, time, json, threading, csv, hmac, hashlib, html as _html, re, math, requests
from datetime import datetime, timedelta, timezone
from collections import deque
from flask import Flask, request, jsonify

# =============== 🔧 НАСТРОЙКИ ===============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "766363011")

BACKUP_ENABLED       = os.getenv("BACKUP_ENABLED", "true").lower() == "true"
BACKUP_INTERVAL_MIN  = int(os.getenv("BACKUP_INTERVAL_MIN", "360"))
BACKUP_ONLY_IF_GROWS = os.getenv("BACKUP_ONLY_IF_GROWS", "true").lower() == "true"

BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL   = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")

TRADE_ENABLED  = os.getenv("TRADE_ENABLED", "false").lower() == "true"
SCALP_ENABLED  = os.getenv("SCALP_ENABLED", "true").lower() == "true"
MAX_RISK_USDT  = float(os.getenv("MAX_RISK_USDT", "1"))
LEVERAGE       = float(os.getenv("LEVERAGE", "20"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

MAX_SL_STREAK = 3
PAUSE_MINUTES = 30

loss_streak = {}
loss_streak_reset_time = {}

LOG_FILE = "/tmp/signals_log.csv"

app = Flask(__name__)

# =============== 🔐 BYBIT SIGN ===============
def _bybit_sign(payload: dict, method: str = "POST", query_string: str = ""):
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    if method.upper() == "POST":
        body = json.dumps(payload or {}, separators=(",", ":"))
        pre_sign = ts + BYBIT_API_KEY + recv_window + body
    else:
        body = ""
        pre_sign = ts + BYBIT_API_KEY + recv_window + (query_string or "")
    sign = hmac.new(BYBIT_API_SECRET.encode(), pre_sign.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
    }
    if method.upper() == "POST":
        headers["Content-Type"] = "application/json"
    return headers, body

# =============== 📨 Telegram ===============
MD_ESCAPE = re.compile(r'([_*\[\]()~`>#+\-=|{}.!])')
def md_escape(text: str) -> str:
    return MD_ESCAPE.sub(r'\\\1', text)

def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    safe_text = md_escape(text)
    try:
        requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": safe_text, "parse_mode": "MarkdownV2"},
            timeout=8,
        )
    except Exception as e:
        print("❌ Telegram error:", e)

def send_telegram_document(filepath: str, caption: str = ""):
    if not os.path.exists(filepath): return False
    try:
        with open(filepath, "rb") as f:
            files = {"document": (os.path.basename(filepath), f)}
            data = {"chat_id": CHAT_ID, "caption": caption[:1024]}
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                data=data, files=files, timeout=20)
        print("✅ Sent CSV to Telegram" if r.status_code == 200 else f"❌ {r.text}")
        return True
    except Exception as e:
        print("❌ Telegram sendDocument exception:", e)
        return False

# =============== 📜 ЛОГИРОВАНИЕ ===============
log_lock = threading.Lock()
def log_signal(ticker, direction, tf, sig_type, entry=None, stop=None, target=None):
    row = [datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), ticker, direction, tf, sig_type, entry or "", stop or "", target or ""]
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

# =============== 💰 BYBIT ORDER HELPERS ===============
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

def _decimals_from_step(step_str: str) -> int:
    s = str(step_str)
    if "e" in s: return max(0, -int(s.split("e")[-1]))
    if "." in s: return len(s.split(".")[1].rstrip("0"))
    return 0

def normalize_qty(symbol: str, qty: float) -> float:
    try:
        r = requests.get(f"{BYBIT_BASE_URL}/v5/market/instruments-info", params={"category": "linear", "symbol": symbol}, timeout=5).json()
        info = (((r or {}).get("result") or {}).get("list") or [])[0]
        lot_info = info.get("lotSizeFilter", {}) or {}
        step_str = lot_info.get("qtyStep", "0.001")
        min_qty_str = lot_info.get("minOrderQty", step_str)
        step = float(step_str); min_qty = float(min_qty_str)
        decimals = _decimals_from_step(step_str)
        stepped = math.floor(qty / step) * step
        normalized = max(min_qty, stepped)
        return float(f"{normalized:.{decimals}f}")
    except Exception:
        return float(f"{qty:.6f}")

def calc_qty_from_risk(entry, stop, risk_usdt, symbol):
    try:
        entry, stop, risk_usdt = float(entry), float(stop), float(risk_usdt)
    except Exception: return 0.0
    if entry <= 0 or stop <= 0 or risk_usdt <= 0: return 0.0
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 1e-12: return 0.0
    raw_qty = risk_usdt / risk_per_unit
    return normalize_qty(symbol, raw_qty)

def set_leverage(symbol, leverage):
    try:
        payload = {"category":"linear","symbol":symbol,"buyLeverage":str(leverage),"sellLeverage":str(leverage)}
        headers, body = _bybit_sign(payload)
        url = BYBIT_BASE_URL.rstrip("/") + "/v5/position/set-leverage"
        r = requests.post(url, headers=headers, data=body, timeout=5)
        print("✅ Leverage set", r.json())
    except Exception as e:
        print("❌ Leverage set exception:", e)

def get_atr(symbol, period=14, interval="5", limit=100):
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        r = requests.get(url, params=params, timeout=5).json()
        candles = (((r or {}).get("result") or {}).get("list") or [])
        if not candles: return 0.0
        candles.sort(key=lambda c: int(c[0]))
        highs = [float(c[2]) for c in candles]
        lows  = [float(c[3]) for c in candles]
        closes= [float(c[4]) for c in candles]
        trs=[]
        for i in range(1,len(highs)):
            tr=max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])); trs.append(tr)
        if not trs: return 0.0
        lookback=min(period,len(trs))
        return sum(trs[-lookback:])/lookback
    except Exception:
        return 0.0

# =============== 🧠 PARSE PAYLOAD ===============
def parse_payload(req):
    data = request.get_json(silent=True) or {}
    ticker_clean = (data.get("ticker","").replace("BYBIT:","").replace(".P","").upper())
    return {
        "type":str(data.get("type","")).upper(),
        "ticker":ticker_clean,
        "direction":str(data.get("direction","")).upper(),
        "tf":str(data.get("tf","1m")).lower(),
        "entry":data.get("entry"),
    }

# =============== 🔔 ВЕБХУК: ТОЛЬКО SCALP ===============
@app.route("/webhook", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET and request.args.get("key","") != WEBHOOK_SECRET:
        return "forbidden",403

    payload = parse_payload(request)
    typ, ticker, direction, entry = payload["type"], payload["ticker"], payload["direction"], payload["entry"]

    if typ != "SCALP" or not SCALP_ENABLED:
        return jsonify({"status":"ignored"}),200

    # === Блокировка при открытой позиции ===
    try:
        resp = requests.get(f"{BYBIT_BASE_URL}/v5/position/list",params={"category":"linear","symbol":ticker},timeout=5)
        j=resp.json(); pos_list=((j.get("result")or{}).get("list")or[])
        open_size=sum(abs(float(p.get("size",0))) for p in pos_list if p.get("symbol")==ticker)
        if open_size>0:
            print(f"⏸ {ticker}: позиция уже открыта, сигнал пропущен.")
            return jsonify({"status":"skipped_open_position"}),200
    except Exception as e:
        print(f"⚠️ Ошибка проверки позиции {ticker}: {e}")

    if not TRADE_ENABLED:
        print(f"🚫 TRADE_DISABLED: {ticker}")
        return jsonify({"status":"trade_disabled"}),200

    try:
        now=time.time()
        streak=loss_streak.get(ticker,0); last_reset=loss_streak_reset_time.get(ticker,0)
        if streak>=MAX_SL_STREAK and (now-last_reset<PAUSE_MINUTES*60):
            print(f"⏸ {ticker} пауза из-за серии стопов.")
            return jsonify({"status":"paused_due_to_sl_streak"}),200

        entry_f=float(entry)
        atr=get_atr(ticker,period=14,interval="5")
        atr_base=get_atr(ticker,period=100,interval="5")
        atr_rel=atr/entry_f if entry_f else 0
        stop_size=max(entry_f*0.002,atr*(0.006/max(atr_rel,1e-6)))
        ratio_rel=atr/max(atr_base,1e-8)
        rr_ratio=1.5
        if ratio_rel>1.8: rr_ratio*=1.5
        elif ratio_rel<0.7: rr_ratio*=0.8
        take_size=stop_size*rr_ratio

        if direction=="UP":
            stop_f=round(entry_f-stop_size,6)
            target_f=round(entry_f+take_size,6)
            side="Buy"
        else:
            stop_f=round(entry_f+stop_size,6)
            target_f=round(entry_f-take_size,6)
            side="Sell"

        sl_pct=round(abs((entry_f-stop_f)/entry_f)*100,3)
        tp_pct=round(abs((target_f-entry_f)/entry_f)*100,3)
        msg=f"⚡ SCALP {ticker} {side} | Entry={entry_f:.6f} Stop={stop_f:.6f} Target={target_f:.6f} (SL={sl_pct}%, TP={tp_pct}%)"
        print(msg)

        set_leverage(ticker,LEVERAGE)
        qty=calc_qty_from_risk(entry_f,stop_f,MAX_RISK_USDT*0.5,ticker)
        if qty<=0:
            print("⚠️ Qty <= 0 — торговля пропущена")
            return jsonify({"status":"skipped"}),200

        # === Маркет ордер + SL/TP ===
        place_order_market_with_limit_tp_sl(ticker,side,qty,target_f,stop_f)
        send_telegram(f"⚡ *AUTO-TRADE (SCALP)*\n{ticker} {side}\nEntry~{entry_f}\nTP:{target_f}\nSL:{stop_f}")
        log_signal(ticker,direction,"1m","SCALP",entry_f,stop_f,target_f)

    except Exception as e:
        print("❌ Trade error (SCALP):", e)

    return jsonify({"status":"ok"}),200

# =============== 💀 PLACE ORDER (сокращённая версия) ===============
def place_order_market_with_limit_tp_sl(symbol,side,qty,tp_price,sl_price):
    try:
        print(f"🚀 NEW TRADE {symbol} {side} qty={qty}")
        bybit_post("/v5/order/create",{
            "category":"linear","symbol":symbol,"side":side,"orderType":"Market","qty":str(qty)
        })
        exit_side="Sell" if side=="Buy" else "Buy"
        bybit_post("/v5/order/create",{
            "category":"linear","symbol":symbol,"side":exit_side,"orderType":"Limit","qty":str(qty),
            "price":str(tp_price),"reduceOnly":True,"timeInForce":"PostOnly"
        })
        bybit_post("/v5/order/create",{
            "category":"linear","symbol":symbol,"side":exit_side,"orderType":"Market","qty":str(qty),
            "triggerPrice":str(sl_price),"triggerBy":"LastPrice","reduceOnly":True,"closeOnTrigger":True
        })
        print("✅ TP/SL placed.")
    except Exception as e:
        print("💀 place_order_market_with_limit_tp_sl:", e)

# =============== 🧩 СЕРВИСНЫЕ ВОРКЕРЫ ===============
def heartbeat_loop():
    sent_today=None
    while True:
        try:
            now=datetime.utcnow()+timedelta(hours=2)
            if now.hour==3 and sent_today!=now.date():
                send_telegram(f"💙 *HEARTBEAT*\nServer alive {now.strftime('%H:%M')}")
                sent_today=now.date()
        except Exception as e:
            print("❌ Heartbeat:", e)
        time.sleep(60)

def backup_log_worker():
    if not BACKUP_ENABLED: return
    last_size=-1
    while True:
        try:
            if os.path.exists(LOG_FILE):
                size=os.path.getsize(LOG_FILE)
                if (not BACKUP_ONLY_IF_GROWS) or (size>last_size):
                    ts=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    send_telegram_document(LOG_FILE,caption=f"📦 Backup {ts}")
                    last_size=size
        except Exception as e:
            print("❌ Backup error:", e)
        time.sleep(BACKUP_INTERVAL_MIN*60)

# =============== MAIN ===============
@app.route("/")
def root(): return "OK",200

if __name__=="__main__":
    print("🚀 Starting SCALP-only server")
    threading.Thread(target=heartbeat_loop,daemon=True).start()
    threading.Thread(target=backup_log_worker,daemon=True).start()
    port=int(os.getenv("PORT","8080"))
    app.run(host="0.0.0.0",port=port,use_reloader=False)
