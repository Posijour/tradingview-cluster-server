# app.py — минимизированный сервер автотрейда (только SCALP)

import os, time, json, threading, csv, hmac, hashlib, html as _html, re, math, requests, base64
from datetime import datetime, timedelta, timezone
from collections import deque
from flask import Flask, request, jsonify

# =============== 🔧 НАСТРОЙКИ ===============
DEBUG = False

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "766363011")

BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "true").lower() == "true"
BACKUP_INTERVAL_MIN = int(os.getenv("BACKUP_INTERVAL_MIN", "360"))
BACKUP_ONLY_IF_GROWS = os.getenv("BACKUP_ONLY_IF_GROWS", "true").lower() == "true"

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
BYBIT_LONG_DAYS_ENV  = os.getenv("BYBIT_LONG_DAYS",  "0,1,2,3,4,5,6")
BYBIT_SHORT_DAYS_ENV = os.getenv("BYBIT_SHORT_DAYS", "0,1,2,3,4,5,6")

BYBIT_LONG_HOURS_ENV  = os.getenv(
    "BYBIT_LONG_HOURS",
    "0-3,3-6,6-9,9-12,12-15,15-18,18-21,21-24"
)
BYBIT_SHORT_HOURS_ENV = os.getenv(
    "BYBIT_SHORT_HOURS",
    "0-3,3-6,6-9,9-12,12-15,15-18,18-21,21-24"
)

TRADE_ENABLED = os.getenv("TRADE_ENABLED", "false").lower() == "true"
SCALP_ENABLED = os.getenv("SCALP_ENABLED", "true").lower() == "true"
MAX_RISK_USDT = float(os.getenv("MAX_RISK_USDT", "1"))
LEVERAGE = float(os.getenv("LEVERAGE", "20"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# OKX (для объединённого single-service деплоя)
OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")
OKX_BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com")
WEBHOOK_SECRET_OKX = os.getenv("WEBHOOK_SECRET_OKX", "")
TRADE_ENABLED_OKX = os.getenv("TRADE_ENABLED_OKX", "false").lower() == "true"
MAX_RISK_USDT_OKX = float(os.getenv("MAX_RISK_USDT_OKX", "1"))
OKX_LEVERAGE = float(os.getenv("OKX_LEVERAGE", "20"))
OKX_BASE_SL_PCT = float(os.getenv("OKX_BASE_SL_PCT", "0.003"))
OKX_RR_RATIO = float(os.getenv("OKX_RR_RATIO", "2.4"))
OKX_LONG_DAYS_ENV = os.getenv("OKX_LONG_DAYS", "0,1,2,3,4,5,6")
OKX_SHORT_DAYS_ENV = os.getenv("OKX_SHORT_DAYS", "0,1,2,3,4,5,6")
OKX_LONG_HOURS_ENV = os.getenv("OKX_LONG_HOURS", "0-3,3-6,6-9,9-12,12-15,15-18,18-21,21-24")
OKX_SHORT_HOURS_ENV = os.getenv("OKX_SHORT_HOURS", "0-3,3-6,6-9,9-12,12-15,15-18,18-21,21-24")
# БАЗОВЫЕ НАСТРОЙКИ SL/TP ДЛЯ SCALP
BASE_SL_PCT = 0.003   # 0.3% от цены входа
RR_RATIO    = 2.4     # TP = SL * 2.4

MAX_SL_STREAK = 3
PAUSE_MINUTES = 30

loss_streak = {}
loss_streak_reset_time = {}
trade_global_cooldown_until = 0
okx_trade_global_cooldown_until = 0

LOG_FILE = "/tmp/signals_log.csv"

app = Flask(__name__)

def parse_days(env_value: str) -> set:
    try:
        return {int(x) for x in env_value.split(",") if x.strip().isdigit()}
    except Exception:
        return set()
BYBIT_LONG_DAYS  = parse_days(BYBIT_LONG_DAYS_ENV)
BYBIT_SHORT_DAYS = parse_days(BYBIT_SHORT_DAYS_ENV)

def parse_symbols(env_value: str) -> set:
    return {s.strip().upper() for s in env_value.split(",") if s.strip()}
BYBIT_LONG_SYMBOLS  = parse_symbols(os.getenv("BYBIT_LONG_SYMBOLS", ""))
BYBIT_SHORT_SYMBOLS = parse_symbols(os.getenv("BYBIT_SHORT_SYMBOLS", ""))

def parse_hours(env_value: str) -> list[tuple[int, int]]:
    ranges = []
    for part in env_value.split(","):
        part = part.strip()
        if "-" not in part:
            continue
        try:
            start, end = part.split("-", 1)
            start_h = int(start)
            end_h = int(end)
            if 0 <= start_h <= 24 and 0 <= end_h <= 24 and start_h != end_h:
                ranges.append((start_h, end_h))
        except Exception:
            continue
    return ranges

BYBIT_LONG_HOURS  = parse_hours(BYBIT_LONG_HOURS_ENV)
BYBIT_SHORT_HOURS = parse_hours(BYBIT_SHORT_HOURS_ENV)

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
MD_ESCAPE = re.compile(r'([_*\[\]()~>#+\-=|{}.!])')
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
    row = [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), ticker, direction, tf, sig_type, entry or "", stop or "", target or ""]
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
        if DEBUG:
            print(f"\n📡 Bybit POST {path}\nPayload: {payload}\nResponse: {r.status_code} {r.text[:500]}\n", flush=True)
        j = r.json()
    except Exception:
        return {"http": r.status_code, "text": r.text}
    if j.get("retCode", 0) != 0:
        print("❌ Bybit error:", j)
    elif DEBUG:
        print(f"✅ Bybit OK: {path}")       
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

def hour_allowed(hour: int, ranges: list[tuple[int,int]]) -> bool:
    for start, end in ranges:
        if start < end:
            if start <= hour < end:
                return True
        else:
            # на случай диапазона через полночь, типа 22-2
            if hour >= start or hour < end:
                return True
    return False

def log_block(reason: str, ticker: str, direction: str, payload: dict):
    entry = payload.get("entry")

    print(
        f"🚫 BYBIT BLOCKED | {reason} | {ticker} {direction} | entry={entry}",
        flush=True
    )

    log_signal(ticker, direction, payload.get("tf"), reason)


# =============== 🔔 ВЕБХУК: ТОЛЬКО SCALP ===============
@app.route("/webhook", methods=["POST"])
def webhook():
    global trade_global_cooldown_until   # ← ЭТОТ ПАРЕНЬ ДОЛЖЕН БЫТЬ ВОТ ТУТ

    if WEBHOOK_SECRET and request.args.get("key", "") != WEBHOOK_SECRET:
        return "forbidden", 403

    # === Расширенное логирование ===
    try:
        raw_body = request.get_data(as_text=True)
        raw_json = request.get_json(silent=True)
        print("\n================= WEBHOOK RECEIVED =================", flush=True)
        print("RAW BODY:", raw_body, flush=True)
        print("PARSED JSON:", raw_json, flush=True)
        print("ARGS:", dict(request.args), flush=True)
        print("HEADERS:", {k:v for k,v in request.headers.items()}, flush=True)
    except Exception as e:
        print("❌ Error reading request:", e, flush=True)

    payload = parse_payload(request)
    typ, ticker, direction, entry = payload["type"], payload["ticker"], payload["direction"], payload["entry"]
    print("PARSED PAYLOAD:", payload, flush=True)

    if typ != "SCALP" or not SCALP_ENABLED:
        log_block("NOT_SCALP", ticker, direction, payload)
        return jsonify({"status": "ignored"}), 200

    # === FILTER: DAY OF WEEK + DIRECTION ===
    now_local = datetime.now(timezone.utc) + timedelta(hours=2)
    weekday = now_local.weekday()  # 0=Mon ... 6=Sun
    hour = now_local.hour          # 0..23

    if direction == "UP":
        if weekday not in BYBIT_LONG_DAYS:
            log_block("BLOCKED_DAY", ticker, direction, payload)
            return jsonify({"status": "blocked_day"}), 200
    
    elif direction == "DOWN":
        if weekday not in BYBIT_SHORT_DAYS:
            log_block("BLOCKED_DAY", ticker, direction, payload)
            return jsonify({"status": "blocked_day"}), 200

    # === FILTER: HOURS (UTC+2) ===

    if direction == "UP":
        if not hour_allowed(hour, BYBIT_LONG_HOURS):
            log_block("BLOCKED_HOUR", ticker, direction, payload)
            return jsonify({"status": "blocked_hour"}), 200
    
    elif direction == "DOWN":
        if not hour_allowed(hour, BYBIT_SHORT_HOURS):
            log_block("BLOCKED_HOUR", ticker, direction, payload)
            return jsonify({"status": "blocked_hour"}), 200

    # === FILTER: SYMBOL + DIRECTION ===
    if direction == "UP":
        if ticker not in BYBIT_LONG_SYMBOLS:
            log_block("BLOCKED_SYMBOL", ticker, direction, payload)
            return jsonify({"status": "blocked_symbol"}), 200

    elif direction == "DOWN":
        if ticker not in BYBIT_SHORT_SYMBOLS:
            log_block("BLOCKED_SYMBOL", ticker, direction, payload)
            return jsonify({"status": "blocked_symbol"}), 200

    # === CHECK GLOBAL 3-MIN COOLDOWN ===
    now = time.time()

    if now < trade_global_cooldown_until:
        remaining = int(trade_global_cooldown_until - now)
        log_block(f"GLOBAL_COOLDOWN_{remaining}s", ticker, direction, payload)
        return jsonify({"status": "blocked"}), 200

    # === Мгновенная защита от дублей (5 секунд) ===
    global last_signal_lock
    if 'last_signal_lock' not in globals():
        last_signal_lock = {}

    key = f"{ticker}_{direction}"
    now = time.time()
    cooldown = 5  # сек

    if key in last_signal_lock and (now - last_signal_lock[key]) < cooldown:
        print(f"🚫 {ticker} {direction}: дубликат в пределах {cooldown}с, пропускаю")
        return jsonify({"status": "duplicate_ignored"}), 200

    last_signal_lock[key] = now  # регистрируем сигнал мгновенно

    # === Проверка открытой позиции ===
    try:
        resp = requests.get(f"{BYBIT_BASE_URL}/v5/position/list", params={"category": "linear", "symbol": ticker}, timeout=5)
        j = resp.json()
        pos_list = ((j.get("result") or {}).get("list") or [])
        open_size = sum(abs(float(p.get("size", 0))) for p in pos_list if p.get("symbol") == ticker)
        if open_size > 0:
            print(f"⏸ {ticker}: позиция уже открыта, сигнал пропущен.")
            return jsonify({"status": "skipped_open_position"}), 200
    except Exception as e:
        print(f"⚠️ Ошибка проверки позиции {ticker}: {e}")

    if not TRADE_ENABLED:
        print(f"🚫 TRADE_DISABLED: {ticker}")
        return jsonify({"status": "trade_disabled"}), 200

    # === Обычная логика входа (только базовые SL/TP) ===
    try:
        entry_f = float(entry)

        # 0.3% стоп от цены входа
        stop_size = entry_f * BASE_SL_PCT

        # TP = SL * 2.4
        take_size = stop_size * RR_RATIO

        if direction == "UP":
            stop_f   = round(entry_f - stop_size, 6)
            target_f = round(entry_f + take_size, 6)
            side = "Buy"
        else:
            stop_f   = round(entry_f + stop_size, 6)
            target_f = round(entry_f - take_size, 6)
            side = "Sell"

        # Просто для инфы в лог/телегу
        sl_pct = round(abs((entry_f - stop_f) / entry_f) * 100, 3)
        tp_pct = round(abs((target_f - entry_f) / entry_f) * 100, 3)
        msg = (
            f"⚡ SCALP {ticker} {side} | "
            f"Entry={entry_f:.6f} Stop={stop_f:.6f} Target={target_f:.6f} "
            f"(SL={sl_pct}%, TP={tp_pct}%)"
        )
        print(msg)

        set_leverage(ticker, LEVERAGE)

        # Риск всё ещё считается как раньше, только по нашему фиксированному стопу
        qty = calc_qty_from_risk(entry_f, stop_f, MAX_RISK_USDT * 0.5, ticker)
        if qty <= 0:
            print("⚠️ Qty <= 0 — торговля пропущена")
            return jsonify({"status": "skipped"}), 200

        ok = place_order_market_with_limit_tp_sl(ticker, side, qty, target_f, stop_f)
        if not ok:
            print("🚫 Trade failed at MARKET stage — no Telegram")
            return jsonify({"status": "order_failed"}), 200
        
        send_telegram(
            f"⚡ *BYBIT TRADE*\n"
            f"{ticker} {side}\n"
            f"Entry~{entry_f}\n"
            f"TP:{target_f}\n"
            f"SL:{stop_f}"
        )
        log_signal(ticker, direction, "1m", "SCALP", entry_f, stop_f, target_f)


        # === ACTIVATE GLOBAL COOLDOWN ===
        trade_global_cooldown_until = time.time() + 180  # 3 minutes
        print(f"🕒 GLOBAL COOLDOWN ACTIVATED for 180s due to {ticker} {direction}")

        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        print("❌ Trade error (SCALP):", e)

    # === FALLBACK (ОБЯЗАТЕЛЬНО) ===
    log_block("NO_ACTION", ticker, direction, payload)
    return jsonify({"status": "no_action"}), 200

# (остальная часть твоего кода — place_order_market_with_limit_tp_sl, monitor_and_cleanup, monitor_closed_trades, heartbeat_loop, backup_log_worker, main, health — остаётся без изменений)

def place_order_market_with_limit_tp_sl(symbol, side, qty, tp_price, sl_price):
    try:
        print(f"\n🚀 NEW TRADE {symbol} {side} qty={qty}")

        # === 1. MARKET ENTRY ===
        entry_payload = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(qty),
            "timeInForce": "IOC"
        }
        entry_resp = bybit_post("/v5/order/create", entry_payload)
        if entry_resp.get("retCode") != 0:
            print("❌ MARKET ENTRY FAILED:", entry_resp)
            return False
        
        time.sleep(1.2)


        # Определяем сторону выхода
        exit_side = "Sell" if side == "Buy" else "Buy"

        # === 2. LIMIT TAKE-PROFIT ===
        tp_payload = {
            "category": "linear",
            "symbol": symbol,
            "side": exit_side,
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(tp_price),
            "timeInForce": "PostOnly",
            "reduceOnly": True
        }
        tp_resp = bybit_post("/v5/order/create", tp_payload)

        # === 3. Актуальная рыночная цена для проверки SL ===
        ticker_info = requests.get(
            f"{BYBIT_BASE_URL}/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
            timeout=5
        ).json()

        last_price = float(ticker_info["result"]["list"][0]["lastPrice"])

        # === 4. Коррекция SL, если он на неправильной стороне ===
        # (та самая магия, которая спасает от всех ошибок)
        if exit_side == "Sell":  
            # мы закрываем LONG → SL должен быть НИЖЕ цены
            if sl_price >= last_price:
                sl_price = last_price * 0.999  # чуть ниже рынка
        else:
            # мы закрываем SHORT → SL должен быть ВЫШЕ цены
            if sl_price <= last_price:
                sl_price = last_price * 1.001  # чуть выше рынка

        sl_price = round(sl_price, 6)

        # === 5. STOP-MARKET SL (Bybit-совместимый) ===
        sl_payload = {
            "category": "linear",
            "symbol": symbol,
            "side": exit_side,
            "orderType": "Market",
            "qty": str(qty),
            "triggerPrice": str(sl_price),
            "triggerBy": "LastPrice",
            "triggerDirection": 1 if exit_side == "Buy" else 2,
            "reduceOnly": True,
            "closeOnTrigger": True
        }
        sl_resp = bybit_post("/v5/order/create", sl_payload)

        threading.Thread(target=monitor_and_cleanup, args=(symbol,), daemon=True).start()
        return True
        
    except Exception as e:
        print("💀 place_order_market_with_limit_tp_sl error:", e)
        return False

# =============== 🧹 ЧИСТКА СТОПОВ ПОСЛЕ ЗАКРЫТИЯ ===============
def _min_qty(symbol: str) -> float:
    try:
        r = requests.get(
            f"{BYBIT_BASE_URL}/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol},
            timeout=5
        ).json()
        info = (((r.get("result") or {}).get("list") or []))[0]
        min_qty = float((info.get("lotSizeFilter") or {}).get("minOrderQty", "0.001"))
        return min_qty
    except Exception:
        return 0.001

def cancel_all_orders(symbol: str, retries: int = 3):
    """Чистит и активные, и условные ордера. Делает несколько попыток."""
    for attempt in range(retries):
        try:
            # активные (Limit/Market)
            bybit_post("/v5/order/cancel-all", {"category": "linear", "symbol": symbol, "orderFilter": "Order"})
            # условные (триггерные SL/TP)
            bybit_post("/v5/order/cancel-all", {"category": "linear", "symbol": symbol, "orderFilter": "StopOrder"})
            print(f"🧹 {symbol}: cancel-all done (try {attempt+1}/{retries})")
            return
        except Exception as e:
            print(f"⚠️ {symbol}: cancel-all failed on try {attempt+1}: {e}")
            time.sleep(1.2)

def monitor_and_cleanup(symbol: str, check_every: float = 3.0, max_checks: int = 5000):
    """Проверяет размер позиции; как только он ~0 — удаляет все ордера окончательно."""
    time.sleep(8)  # ждём перед первой проверкой, чтобы Bybit обновил позицию
    tiny = _min_qty(symbol) * 0.6
    no_position_count = 0

    for i in range(max_checks):
        try:
            time.sleep(check_every)

            # Авторизованный запрос позиций
            path = "/v5/position/list"
            query = f"category=linear&symbol={symbol}"
            headers, _ = _bybit_sign({}, method="GET", query_string=query)
            resp = requests.get(f"{BYBIT_BASE_URL}{path}?{query}", headers=headers, timeout=5)
            if not resp.text:
                print(f"⚠️ monitor_and_cleanup {symbol}: пустой ответ от API")
                continue
            r = resp.json()

            pos_list = ((r.get("result") or {}).get("list") or [])
            size = sum(abs(float(p.get("size", 0))) for p in pos_list if p.get("symbol") == symbol)

            # если позиции нет, увеличиваем счётчик
            if size <= tiny:
                no_position_count += 1
                print(f"🔍 {symbol}: позиция нулевая ({size}), попытка чистки {no_position_count}/3")
                cancel_all_orders(symbol)
                if no_position_count >= 3:
                    print(f"✅ {symbol}: все ордера гарантированно очищены")
                    cancel_all_orders(symbol)  # финальная зачистка после выхода
                    return

                time.sleep(1.5)
            else:
                no_position_count = 0  # сброс если снова есть объём

        except Exception as e:
            print(f"⚠️ monitor_and_cleanup {symbol}: {e}")

    print(f"⏳ {symbol}: cleanup timed out (возможно, позиция не закрыта)")

# =============== 🔍 MONITOR CLOSED TRADES (тихий, без Telegram) ===============
def monitor_closed_trades():
    print("⚙️ Silent trade monitor started")
    checked = set()
    while True:
        try:
            time.sleep(60)
            if not os.path.exists(LOG_FILE): continue
            with log_lock:
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    rows = list(csv.reader(f))
            if not rows or len(rows) < 2: continue
            if "time_utc" in rows[0][0].lower(): rows = rows[1:]
            open_trades = []
            for r in rows:
                if len(r) < 8: continue
                if len(r) >= 9 and r[8] in ("TP","SL"): continue
                try:
                    open_trades.append((r[1], r[2], float(r[5]), float(r[6]), float(r[7])))
                except: continue
            for ticker, direction, entry, stop, target in open_trades:
                key=f"{ticker}_{direction}_{entry}"
                if key in checked: continue
                checked.add(key)
                resp = requests.get(f"{BYBIT_BASE_URL}/v5/position/list", params={"category":"linear","symbol":ticker}, timeout=5)
                if not resp.text:
                    print(f"⚠️ monitor_closed_trades: пустой ответ по {ticker}, пропускаю итерацию")
                    continue
                pos = resp.json()
                pos_list = ((pos.get("result") or {}).get("list") or [])
                size = sum(abs(float(p.get("size",0))) for p in pos_list if p.get("symbol")==ticker)
                if size>0: continue
                hist=requests.get(f"{BYBIT_BASE_URL}/v5/order/history",params={"category":"linear","symbol":ticker,"limit":10},timeout=5).json()
                orders=((hist.get("result")or{}).get("list")or[])
                result=None
                for o in orders:
                    if o.get("orderStatus")!="Filled": continue
                    if o.get("reduceOnly") and o.get("orderType")=="Limit": result="TP"; break
                    if o.get("closeOnTrigger") and o.get("orderType")=="Market": result="SL"; break
                    if direction=="UP" and o.get("side")=="Sell": result="TP" if "Limit" in o.get("orderType","") else "SL"; break
                    if direction=="DOWN" and o.get("side")=="Buy": result="TP" if "Limit" in o.get("orderType","") else "SL"; break
                if not result: continue
                with log_lock:
                    updated=[]
                    with open(LOG_FILE,"r",encoding="utf-8") as f: updated=list(csv.reader(f))
                    for row in updated:
                        if len(row)<8: continue
                        if row[1]==ticker and row[2]==direction and row[5]==str(entry):
                            if len(row)<9: row.append(result)
                            else: row[8]=result
                            break
                    with open(LOG_FILE,"w",newline="",encoding="utf-8") as f:
                        w=csv.writer(f); [w.writerow(r) for r in updated]
                now=time.time()
                if result=="SL":
                    loss_streak[ticker]=loss_streak.get(ticker,0)+1
                    loss_streak_reset_time[ticker]=now
                elif result=="TP":
                    loss_streak[ticker]=0
                    loss_streak_reset_time[ticker]=now
                print(f"📊 {ticker}: closed as {result}, SL streak={loss_streak.get(ticker,0)}")
                cancel_all_orders(ticker)
        except Exception as e:
            print("💀 monitor_closed_trades crashed:", e)
            time.sleep(15)


# =============== OKX HELPERS ===============
instrument_locks = {}
instrument_locks_lock = threading.Lock()
_okx_inst_cache = {}
_okx_pos_mode = None

OKX_LONG_DAYS_SET = parse_days(OKX_LONG_DAYS_ENV)
OKX_SHORT_DAYS_SET = parse_days(OKX_SHORT_DAYS_ENV)
OKX_LONG_HOUR_RANGES = parse_hours(OKX_LONG_HOURS_ENV)
OKX_SHORT_HOUR_RANGES = parse_hours(OKX_SHORT_HOURS_ENV)

def acquire_instrument_lock(inst_id: str, ttl: int = 30) -> bool:
    now = time.time()
    with instrument_locks_lock:
        ts = instrument_locks.get(inst_id, 0)
        if now < ts:
            return False
        instrument_locks[inst_id] = now + ttl
        return True

def release_instrument_lock(inst_id: str):
    with instrument_locks_lock:
        instrument_locks.pop(inst_id, None)

def _okx_timestamp() -> str:
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _okx_sign(method: str, path: str, body: str = ""):
    ts = _okx_timestamp()
    prehash = f"{ts}{method.upper()}{path}{body}"
    sign = base64.b64encode(
        hmac.new(OKX_API_SECRET.encode(), prehash.encode(), digestmod="sha256").digest()
    ).decode()
    return {
        "OK-ACCESS-KEY": OKX_API_KEY,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
        "Content-Type": "application/json",
    }

def okx_private_get(path: str, params: dict = None, timeout: int = 10):
    qs = ""
    if params:
        parts = [f"{k}={v}" for k, v in params.items()]
        qs = "?" + "&".join(parts)
    headers = _okx_sign("GET", path, "")
    url = OKX_BASE_URL.rstrip("/") + path + qs
    r = requests.get(url, headers=headers, timeout=timeout)
    if DEBUG:
        print("GET", url, r.status_code, r.text[:400])
    return r.json()

def okx_private_post(path: str, payload: dict, timeout: int = 10):
    body = json.dumps(payload, separators=(",", ":"))
    headers = _okx_sign("POST", path, body)
    url = OKX_BASE_URL.rstrip("/") + path
    r = requests.post(url, headers=headers, data=body, timeout=timeout)
    text_preview = r.text[:400]
    if DEBUG:
        print("POST", url, "payload:", payload, "resp:", r.status_code, text_preview)
    try:
        j = r.json()
    except Exception:
        print("❌ OKX raw response (not JSON):", text_preview)
        return {"http": r.status_code, "text": r.text}
    if j.get("code") not in ("0", 0):
        print("❌ OKX error:", j)
    else:
        print("✅ OKX OK:", j)
    return j

def tv_ticker_to_okx_inst_id(tv_ticker: str) -> str:
    s = tv_ticker.upper().replace("OKX:", "").replace(".P", "")
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}-USDT-SWAP"
    return s

def get_okx_inst_info(inst_id: str):
    if inst_id in _okx_inst_cache:
        return _okx_inst_cache[inst_id]
    resp = requests.get(
        OKX_BASE_URL.rstrip("/") + "/api/v5/public/instruments",
        params={"instType": "SWAP", "instId": inst_id},
        timeout=10,
    ).json()
    data = (resp.get("data") or resp.get("result") or [])
    if not data:
        raise RuntimeError(f"Нет данных по инструменту {inst_id}: {resp}")
    info = data[0]
    _okx_inst_cache[inst_id] = info
    return info

def get_okx_pos_mode() -> str:
    global _okx_pos_mode
    if _okx_pos_mode:
        return _okx_pos_mode
    try:
        cfg = okx_private_get("/api/v5/account/config", timeout=10)
        data = cfg.get("data") or []
        if data:
            raw = (data[0].get("posMode") or "net").lower()
            if "long" in raw and "short" in raw:
                _okx_pos_mode = "long_short"
            elif "long_short" in raw:
                _okx_pos_mode = "long_short"
            else:
                _okx_pos_mode = "net"
        else:
            _okx_pos_mode = "net"
        print("🔧 OKX posMode detected:", _okx_pos_mode)
    except Exception as e:
        print("⚠️ Cannot detect posMode, fallback to 'net':", e)
        _okx_pos_mode = "net"
    return _okx_pos_mode

def set_okx_leverage(inst_id: str, leverage: float):
    try:
        payload = {"instId": inst_id, "lever": str(leverage), "mgnMode": "cross"}
        resp = okx_private_post("/api/v5/account/set-leverage", payload)
        print("✅ OKX leverage response:", resp)
    except Exception as e:
        print("❌ set_okx_leverage exception:", e)

def calc_sz_from_risk_okx(entry, stop, risk_usdt, inst_id: str) -> float:
    try:
        entry, stop, risk_usdt = float(entry), float(stop), float(risk_usdt)
    except Exception:
        return 0.0
    if entry <= 0 or stop <= 0 or risk_usdt <= 0:
        return 0.0
    info = get_okx_inst_info(inst_id)
    ct_val = float(info.get("ctVal", "0.001"))
    min_sz = float(info.get("minSz", "1"))
    lot_sz = float(info.get("lotSz", min_sz))
    price_risk = abs(entry - stop)
    if price_risk <= 1e-12:
        return 0.0
    risk_per_contract = price_risk * ct_val
    raw_sz = risk_usdt / risk_per_contract
    stepped = math.floor(raw_sz / lot_sz) * lot_sz
    sz = max(min_sz, stepped)
    return float(f"{sz:.4f}")

def okx_has_position(inst_id: str) -> bool:
    j = okx_private_get("/api/v5/account/positions", {"instType": "SWAP", "instId": inst_id})
    for p in j.get("data", []):
        pos = float(p.get("pos", "0"))
        avail = float(p.get("availPos", "0"))
        if abs(pos) > 0 or abs(avail) > 0:
            return True
    return False

def okx_has_open_orders(inst_id: str) -> bool:
    j = okx_private_get("/api/v5/trade/orders-pending", {"instType": "SWAP", "instId": inst_id})
    return bool(j.get("data"))

def okx_has_algo_orders(inst_id: str) -> bool:
    j = okx_private_get("/api/v5/trade/orders-algo-pending", {"instType": "SWAP", "instId": inst_id})
    return bool(j.get("data"))

def okx_place_order_with_tp_sl(inst_id: str, side: str, entry: float, tp: float, sl: float, risk_usdt: float):
    sz = calc_sz_from_risk_okx(entry, sl, risk_usdt, inst_id)
    if sz <= 0:
        msg = f"{inst_id}: sz <= 0, сделка пропущена (risk={risk_usdt}, entry={entry}, sl={sl})"
        print("⚠️", msg)
        send_telegram("⚠️ *OKX SIZE ERROR*\n" + msg)
        return {"error": "bad_size"}
    payload = {
        "instId": inst_id,
        "tdMode": "cross",
        "side": side,
        "ordType": "market",
        "sz": str(sz),
        "attachAlgoOrds": [{
            "tpTriggerPx": str(tp),
            "tpOrdPx": str(tp),
            "tpTriggerPxType": "last",
            "slTriggerPx": str(sl),
            "slOrdPx": str(sl),
            "slTriggerPxType": "last",
        }],
    }
    if get_okx_pos_mode() == "long_short":
        payload["posSide"] = "long" if side == "buy" else "short"
    resp = okx_private_post("/api/v5/trade/order", payload)
    return resp

def parse_payload_okx(req):
    data = request.get_json(silent=True) or {}
    raw_ticker = str(data.get("ticker", "")).upper()
    return {
        "type": str(data.get("type", "")).upper(),
        "tv_ticker": raw_ticker,
        "instId": tv_ticker_to_okx_inst_id(raw_ticker),
        "direction": str(data.get("direction", "")).upper(),
        "entry": data.get("entry"),
        "tf": str(data.get("tf", "1m")).lower(),
    }

@app.route("/webhook_okx", methods=["POST"])
def webhook_okx():
    global okx_trade_global_cooldown_until
    if WEBHOOK_SECRET_OKX and request.args.get("key", "") != WEBHOOK_SECRET_OKX:
        return "forbidden", 403
    payload = parse_payload_okx(request)
    typ = payload["type"]
    inst_id = payload["instId"]
    direction = payload["direction"]
    entry = payload["entry"]
    if not acquire_instrument_lock(inst_id, ttl=60):
        return jsonify({"status": "blocked_local_lock"}), 200
    try:
        now_dt = datetime.now(timezone.utc) + timedelta(hours=2)
        wd = now_dt.weekday()
        hour = now_dt.hour
        if direction == "UP":
            days_set = OKX_LONG_DAYS_SET
            hour_ranges = OKX_LONG_HOUR_RANGES
        else:
            days_set = OKX_SHORT_DAYS_SET
            hour_ranges = OKX_SHORT_HOUR_RANGES
        if days_set and wd not in days_set:
            return jsonify({"status": "blocked_day"}), 200
        if not hour_allowed(hour, hour_ranges):
            return jsonify({"status": "blocked_hour"}), 200
        if typ != "SCALP":
            return jsonify({"status": "ignored"}), 200
        now = time.time()
        if now < okx_trade_global_cooldown_until:
            return jsonify({"status": "cooldown"}), 200
        if okx_has_position(inst_id) or okx_has_open_orders(inst_id) or okx_has_algo_orders(inst_id):
            return jsonify({"status": "blocked_existing_state"}), 200
        if not TRADE_ENABLED_OKX:
            return jsonify({"status": "trade_disabled"}), 200
        try:
            entry_f = float(entry)
        except Exception:
            return jsonify({"status": "bad_entry"}), 200
        stop_size = entry_f * OKX_BASE_SL_PCT
        take_size = stop_size * OKX_RR_RATIO
        if direction == "UP":
            side = "buy"
            sl = round(entry_f - stop_size, 6)
            tp = round(entry_f + take_size, 6)
        else:
            side = "sell"
            sl = round(entry_f + stop_size, 6)
            tp = round(entry_f - take_size, 6)
        set_okx_leverage(inst_id, OKX_LEVERAGE)
        resp = okx_place_order_with_tp_sl(inst_id, side, entry_f, tp, sl, MAX_RISK_USDT_OKX)
        okx_trade_global_cooldown_until = time.time() + 180
        return jsonify({"status": "ok", "okx_resp": resp}), 200
    except Exception as e:
        print("❌ WEBHOOK OKX ERROR:", e)
        return jsonify({"status": "error"}), 500
    finally:
        release_instrument_lock(inst_id)


# =============== 🧩 СЕРВИСНЫЕ ВОРКЕРЫ ===============
def heartbeat_loop():
    sent_today=None
    while True:
        try:
            now = datetime.now(timezone.utc) + timedelta(hours=2)
            if now.hour==3 and sent_today!=now.date():
                send_telegram(f"💙 *HEARTBEAT*\nServer alive {now.strftime('%H:%M')}")
                sent_today=now.date()
        except Exception as e:
            print("❌ Heartbeat:", e)
        time.sleep(60)

# =============== MAIN ===============
@app.route("/")
def root(): return "OK",200

# =============== HEALTHCHECK ===============
@app.route("/health")
def health():
    return "OK", 200

if __name__=="__main__":
    print("🚀 Starting SCALP-only server")
    threading.Thread(target=heartbeat_loop,daemon=True).start()
    threading.Thread(target=monitor_closed_trades,daemon=True).start()
    port=int(os.getenv("PORT","8080"))
    app.run(host="0.0.0.0",port=port,use_reloader=False)

