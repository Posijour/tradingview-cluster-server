# app.py — рабочий сервер с вебхуком, автоторговлей, кластерами, дашбордом и статистикой
# адаптирован под твой Pine без изменений Pine

import os, time, json, threading, csv, hmac, hashlib, html as _html, re
from datetime import datetime, timedelta
from collections import deque, defaultdict
from time import monotonic
from flask import Flask, request, jsonify
import requests

# =============== 🔧 НАСТРОЙКИ ===============
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID", "766363011")

# Бэкап лога в Telegram (вариант 3)
BACKUP_ENABLED       = os.getenv("BACKUP_ENABLED", "true").lower() == "true"
BACKUP_INTERVAL_MIN  = int(os.getenv("BACKUP_INTERVAL_MIN", "360"))  # раз в 6 часов
BACKUP_ONLY_IF_GROWS = os.getenv("BACKUP_ONLY_IF_GROWS", "true").lower() == "true"

# Кластеры
CLUSTER_WINDOW_MIN     = int(os.getenv("CLUSTER_WINDOW_MIN", "45"))     # окно кластеров в минутах
CLUSTER_WINDOW_5M_MIN     = int(os.getenv("CLUSTER_WINDOW_5M_MIN", "15"))
CLUSTER_THRESHOLD      = int(os.getenv("CLUSTER_THRESHOLD", "6"))       # сколько разных монет в одну сторону, чтобы это считалось кластером
CHECK_INTERVAL_SEC     = int(os.getenv("CHECK_INTERVAL_SEC", "10"))     # как часто воркер проверяет
VALID_TF_5M = os.getenv("VALID_TF_5M", "5m")
VALID_TF_15M = os.getenv("VALID_TF_15M", "15m")
VALID_TF_1H  = os.getenv("VALID_TF_1H", "1h")
WEBHOOK_SECRET         = os.getenv("WEBHOOK_SECRET", "")                # защита /webhook?key=...
CLUSTER_COOLDOWN_SEC = CLUSTER_WINDOW_MIN * 60
CLUSTER_5M_COOLDOWN_SEC   = int(os.getenv("CLUSTER_5M_COOLDOWN_SEC", "900"))
CLUSTER_TRADE_DELAY_SEC = int(os.getenv("CLUSTER_TRADE_DELAY_SEC", "600"))  # 10 минут

# Bybit
BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL   = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
TRADE_ENABLED    = os.getenv("TRADE_ENABLED", "false").lower() == "true"
MAX_RISK_USDT    = float(os.getenv("MAX_RISK_USDT", "1"))
LEVERAGE         = float(os.getenv("LEVERAGE", "20"))

# если хочешь ограничить автоторговлю на конкретные тикеры:
# пример: SYMBOL_WHITELIST=BTCUSDT,ETHUSDT,SOLUSDT
SYMBOL_WHITELIST = set(
    s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST","").split(",") if s.strip()
)


# =============== 🔐 BYBIT SIGN FUNCTION (проверенная) ===============
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

# Лог-файл
LOG_FILE = "/tmp/signals_log.csv"

# =============== 🧠 ГЛОБАЛЬНЫЕ СТРУКТУРЫ СОСТОЯНИЯ ===============
signals_5m = deque(maxlen=10000)
signals_15m = deque(maxlen=5000)
signals_1h = deque(maxlen=2000)
lock = threading.Lock()
state_lock = threading.Lock()
log_lock = threading.Lock()
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}
last_cluster_sent_15m = {"UP": 0.0, "DOWN": 0.0}
last_cluster_trade = {"UP": 0, "DOWN": 0}
last_signals = {}

app = Flask(__name__)

# =============== 🌐 Утилиты форматирования ===============
tg_times = deque(maxlen=20)

MD_ESCAPE = re.compile(r'([_*\[\]()~`>#+\-=|{}.!])')
def md_escape(text: str) -> str:
    return MD_ESCAPE.sub(r'\\\1', text)

def html_esc(x):
    return _html.escape(str(x), quote=True)

# =============== 🔐 Верификация подписи (используется в /simulate) ===============
def verify_signature(secret, body, signature):
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)

# =============== 📩 Telegram отправка с антифлудом ===============
tg_times = deque(maxlen=20)
tg_times_5m = deque(maxlen=20)  # отдельный буфер для 5m уведомлений

def send_telegram(text: str, channel: str = "default"):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return

    safe_text = md_escape(text)

    def _send_with_rate_limit():
        try:
            tg_queue = tg_times_5m if channel == "5m" else tg_times

            now = monotonic()
            tg_queue.append(now)

            if len(tg_queue) >= 2 and now - tg_queue[-2] < 1.0:
                time.sleep(1.0 - (now - tg_queue[-2]))

            if len(tg_queue) == tg_queue.maxlen and now - tg_queue[0] < 60:
                time.sleep(60 - (now - tg_queue[0]))

            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                params={"chat_id": CHAT_ID, "text": safe_text, "parse_mode": "MarkdownV2"},
                timeout=8,
            )
            print(f"✅ Sent to Telegram ({channel})")
        except Exception as e:
            print(f"❌ Telegram error ({channel}):", e)

    threading.Thread(target=_send_with_rate_limit, daemon=True).start()

def send_telegram_document(filepath: str, caption: str = ""):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return False
    try:
        if not os.path.exists(filepath):
            print(f"⚠️ Document not found: {filepath}")
            return False

        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if size_mb > 49.5:
            print(f"⚠️ File too large for Telegram: {size_mb:.1f} MB")
            return False

        with open(filepath, "rb") as f:
            files = {"document": (os.path.basename(filepath), f)}
            data = {"chat_id": CHAT_ID, "caption": caption[:1024]}
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                data=data,
                files=files,
                timeout=20
            )

        ok = (r.status_code == 200)
        print("✅ Sent CSV to Telegram" if ok else f"❌ Telegram sendDocument error: {r.text}")
        return ok

    except Exception as e:
        print("❌ Telegram sendDocument exception:", e)
        return False

# =============== 📝 ЛОГИРОВАНИЕ сигналов В CSV ===============
def log_signal(ticker, direction, tf, sig_type, entry=None, stop=None, target=None):
    row = [
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ticker,
        direction,
        tf,
        sig_type,
        entry or "",
        stop or "",
        target or ""
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

import math

def _decimals_from_step(step_str: str) -> int:
    s = str(step_str)
    if "e" in s or "E" in s:
        try:
            return max(0, -int(s.split("e")[-1]))
        except Exception:
            return 0
    if "." in s:
        return len(s.split(".")[1].rstrip("0"))
    return 0

def normalize_qty(symbol: str, qty: float) -> float:
    try:
        r = requests.get(
            f"{BYBIT_BASE_URL}/v5/market/instruments-info",
            params={"category": "linear", "symbol": symbol}, timeout=5
        ).json()
        info = (((r or {}).get("result") or {}).get("list") or [])[0]
        lot_info = info.get("lotSizeFilter", {}) or {}

        step_str = lot_info.get("qtyStep", "0.001")
        min_qty_str = lot_info.get("minOrderQty", step_str)

        step = float(step_str)
        min_qty = float(min_qty_str)
        decimals = _decimals_from_step(step_str)

        stepped = math.floor(qty / step) * step
        normalized = max(min_qty, stepped)
        return float(f"{normalized:.{decimals}f}")
    except Exception as e:
        print("❌ normalize_qty error:", e)
        return float(f"{qty:.6f}")

def calc_qty_from_risk(entry: float, stop: float, risk_usdt: float, symbol: str) -> float:
    try:
        entry = float(entry)
        stop = float(stop)
        risk_usdt = float(risk_usdt)
    except Exception:
        return 0.0

    if entry <= 0 or stop <= 0 or risk_usdt <= 0:
        return 0.0

    risk_per_unit = abs(entry - stop)
    if not math.isfinite(risk_per_unit) or risk_per_unit < 1e-12:
        return 0.0

    raw_qty = risk_usdt / risk_per_unit
    if not math.isfinite(raw_qty) or raw_qty <= 0:
        return 0.0

    qty = normalize_qty(symbol, raw_qty)
    if not math.isfinite(qty) or qty <= 0:
        return 0.0

    return qty

def set_leverage(symbol, leverage):
    try:
        payload = {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage)
        }
        headers, body = _bybit_sign(payload)
        url = BYBIT_BASE_URL.rstrip("/") + "/v5/position/set-leverage"
        r = requests.post(url, headers=headers, data=body, timeout=5)
        resp = r.json()

        if resp.get("retCode") not in (0, 110043):
            print("❌ Bybit leverage set error:", resp)
        else:
            print(f"✅ Leverage {leverage}x set for {symbol}")

    except Exception as e:
        print("❌ Leverage set exception:", e)

# === мониторинг и умная чистка «осиротевших» ордеров ===
def monitor_and_cleanup(symbol, check_every=10, max_checks=360):
    """
    Следит за позицией. Как только позиция по symbol закрыта, удаляет все оставшиеся ордера.
    Защищает от повторного открытия после TP из-за висящего условного SL.
    """
    try:
        print(f"🧪 Cleanup check for {symbol}: signed headers ready")

        for _ in range(max_checks):
            time.sleep(check_every)

            r = requests.get(
                f"{BYBIT_BASE_URL}/v5/position/list",
                params={"category": "linear", "symbol": symbol},
                timeout=5
            )
            if r.status_code != 200:
                print(f"⚠️ monitor_and_cleanup HTTP {r.status_code}: {r.text}")
                continue

            try:
                j = r.json()
            except Exception:
                print(f"⚠️ monitor_and_cleanup JSON decode error: {r.text[:200]}")
                continue

            pos_list = ((j.get("result") or {}).get("list") or [])
            size = sum(abs(float(p.get("size", 0))) for p in pos_list if p.get("symbol") == symbol)
            if size == 0:
                cancel_payload = {"category": "linear", "symbol": symbol}
                headers, body = _bybit_sign(cancel_payload)
                r2 = requests.post(
                    f"{BYBIT_BASE_URL}/v5/order/cancel-all",
                    headers=headers,
                    data=body,
                    timeout=5
                )

                # улучшенная обработка ответов
                if r2.status_code == 401:
                    print(f"⚠️ monitor_and_cleanup HTTP 401 for {symbol}: check API key permissions.")
                    print("   (need: Trade + Position Write access)")
                    continue

                if r2.text.strip() == "":
                    print(f"🧹 Cleanup {symbol}: empty response (likely success despite 401)")
                else:
                    try:
                        j2 = r2.json()
                        print(f"🧹 Cleanup response for {symbol}:", j2)
                    except Exception:
                        print(f"🧹 Cleanup raw response ({r2.status_code}):", r2.text)
                return

        print(f"⏳ Cleanup monitor ended for {symbol} (position still open)")

    except Exception as e:
        print(f"❌ monitor_and_cleanup error ({symbol}): {e}")

def place_order_market_with_limit_tp_sl(symbol: str, side: str, qty: float, tp_price: float, sl_price: float):
    """
    Открывает рыночную позицию и ставит лимитный TP и условный SL.
    Исправлены ошибки Bybit:
    - TP: timeInForce -> PostOnly
    - SL: корректная логика triggerDirection (Buy=2, Sell=1)
    Также запускает монитор, который удалит «осиротевшие» ордера после закрытия позиции.
    """
    try:
        print(f"\n🚀 === NEW TRADE START {symbol} {side} qty={qty} ===")
        print(f"Entry TP={tp_price}  SL={sl_price}")

        # === 1. Открытие позиции ===
        resp_open = bybit_post("/v5/order/create", {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(qty),
            "timeInForce": "ImmediateOrCancel"
        })
        print("✅ Market entry response:", resp_open)

        # === 2. Проверка позиции ===
        time.sleep(1)
        for attempt in range(10):
            try:
                resp = requests.get(
                    f"{BYBIT_BASE_URL}/v5/position/list",
                    params={"category": "linear", "symbol": symbol},
                    timeout=5
                )
                if resp.status_code != 200 or not resp.text.strip():
                    raise ValueError(f"empty response ({resp.status_code})")
                r = resp.json()
                pos_list = ((r.get("result") or {}).get("list") or [])
                open_size = sum(abs(float(p.get("size", 0))) for p in pos_list if p.get("symbol") == symbol)
                if open_size > 0:
                    print(f"✅ Position confirmed after {attempt+1} tries, size={open_size}")
                    break
            except Exception as e:
                print(f"⚠️ Position check error {attempt}: {e}")
            time.sleep(0.8)

        # === 3. Текущая цена ===
        try:
            r = requests.get(
                f"{BYBIT_BASE_URL}/v5/market/tickers",
                params={"category": "linear", "symbol": symbol},
                timeout=5
            ).json()
            current_price = float(r["result"]["list"][0]["lastPrice"])
        except Exception:
            current_price = float(tp_price) if side == "Buy" else float(sl_price)
        print(f"💰 Current price: {current_price}")

        exit_side = "Sell" if side == "Buy" else "Buy"

        # === 4. TP ===
        buffer_tp = 0.0015 if current_price > 1 else 0.003
        tp_safe = float(tp_price)
        if side == "Buy" and tp_safe <= current_price:
            tp_safe = round(current_price * (1 + buffer_tp), 6)
        elif side == "Sell" and tp_safe >= current_price:
            tp_safe = round(current_price * (1 - buffer_tp), 6)

        tp_payload = {
            "category": "linear",
            "symbol": symbol,
            "side": exit_side,
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(tp_safe),
            "reduceOnly": True,
            "timeInForce": "PostOnly"
        }
        print("📦 TP payload:", tp_payload)
        resp_tp = bybit_post("/v5/order/create", tp_payload)
        print("📩 TP response:", resp_tp)

        if resp_tp.get("retCode", 0) != 0:
            print("❌ TP error:", resp_tp.get("retMsg", "Unknown"))

        # === 5. SL ===
        buffer_mult = 0.002 if current_price > 1 else 0.005
        if side == "Buy":
            trigger_dir = 2  # ждём падения
            if sl_price >= current_price:
                sl_price = round(current_price * (1 - buffer_mult), 6)
        else:
            trigger_dir = 1  # ждём роста
            if sl_price <= current_price:
                sl_price = round(current_price * (1 + buffer_mult), 6)

        if abs(current_price - sl_price) / current_price < 0.001:
            sl_price = round(current_price * (1 - 0.0015), 6) if side == "Buy" else round(current_price * (1 + 0.0015), 6)

        sl_payload = {
            "category": "linear",
            "symbol": symbol,
            "side": exit_side,
            "orderType": "Market",
            "qty": str(qty),
            "triggerPrice": str(sl_price),
            "triggerBy": "LastPrice",
            "reduceOnly": True,
            "closeOnTrigger": True,
            "triggerDirection": trigger_dir
        }
        print("📦 SL payload:", sl_payload)
        resp_sl = bybit_post("/v5/order/create", sl_payload)
        print("📩 SL response:", resp_sl)

        if resp_sl.get("retCode", 0) != 0:
            print("❌ SL error:", resp_sl.get("retMsg", "Unknown"))

        print(f"✅ TP/SL placed successfully for {symbol}")

        # === 6. Старт мониторинга для умной очистки ===
        threading.Thread(target=monitor_and_cleanup, args=(symbol,), daemon=True).start()

        return {"entry": resp_open, "tp": resp_tp, "sl": resp_sl}

    except Exception as e:
        print("💀 place_order_market_with_limit_tp_sl exception:", e)
        return None

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

        trs = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i]  - closes[i - 1])
            )
            trs.append(tr)

        if not trs:
            return 0.0

        lookback = min(period, len(trs))
        return sum(trs[-lookback:]) / lookback
    except Exception as e:
        print("ATR fetch error:", e)
        return 0.0

# =============== 🔍 ПАРСЕР ВХОДЯЩЕГО PAYLOAD ===============
def parse_payload(req) -> dict:
    data = request.get_json(silent=True) or {}
    if not data:
        try:
            data = json.loads(req.data)
        except Exception:
            data = {}

    ticker_clean = (
        data.get("ticker","")
        .replace("BYBIT:", "")
        .replace(".P", "")
        .upper()
    )

    return {
        "type":      str(data.get("type", "")).upper(),
        "ticker":    ticker_clean,
        "direction": str(data.get("direction", "")).upper(),
        "tf":        str(data.get("tf", "15m")).lower(),
        "message":   data.get("message", ""),
        "entry":     data.get("entry"),
        "stop":      data.get("stop"),
        "target":    data.get("target"),
    }

# =============== 🔔 ВЕБХУК ОТ TRADINGVIEW (MTF / SCALP / CLUSTER / FAIL) ===============
@app.route("/webhook", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET:
        key = request.args.get("key", "")
        if key != WEBHOOK_SECRET:
            return "forbidden", 403

    payload = parse_payload(request)
    typ        = payload.get("type", "")
    tf         = payload.get("tf", "")
    msg        = payload.get("message", "")
    ticker     = payload.get("ticker", "")
    direction  = payload.get("direction", "")
    entry      = payload.get("entry")
    stop       = payload.get("stop")
    target     = payload.get("target")

    MTF_ENABLED     = os.getenv("MTF_ENABLED", "false").lower() == "true"
    CLUSTER_ENABLED = os.getenv("CLUSTER_ENABLED", "false").lower() == "true"
    SCALP_ENABLED   = os.getenv("SCALP_ENABLED", "false").lower() == "true"
    FAIL_ENABLED    = os.getenv("FAIL_ENABLED", "false").lower() == "true"

    global last_signals
    now = time.time()
    sig_id = f"{typ}_{ticker}_{direction}_{tf}"
    if sig_id in last_signals and now - last_signals[sig_id] < 180:
        print(f"⚠️ Duplicate signal ignored: {sig_id}")
        return jsonify({"status": "duplicate"}), 200
    last_signals[sig_id] = now

    if msg:
        MAX_SIGNAL_AGE_SEC = 3600
        signal_time = float(payload.get("time", time.time()))
        age = time.time() - signal_time

        if age > MAX_SIGNAL_AGE_SEC:
            print(f"⏳ Old signal ({int(age)}s) — skip Telegram alert")
        else:
            send_telegram(msg)
            print(f"📨 Forwarded alert: {ticker} {direction}")

        if CLUSTER_ENABLED and ticker and direction in ("UP", "DOWN"):
            with lock:
                if tf == VALID_TF_5M:
                    signals_5m.append((time.time(), ticker, direction, tf))
                    print(f"[WH] queued {ticker} {direction} ({tf}) for 5m cluster window")
                elif tf == VALID_TF_15M:
                    signals_15m.append((time.time(), ticker, direction, tf))
                    print(f"[WH] queued {ticker} {direction} ({tf}) for 15m cluster window")
                elif tf == VALID_TF_1H:
                    signals_1h.append((time.time(), ticker, direction, tf))
                    print(f"[WH] queued {ticker} {direction} ({tf}) for 1h cluster window")

        if typ != "SCALP":
            log_signal(ticker, direction, tf, typ or "WEBHOOK", entry, stop, target)

    if typ == "MTF":
        if not MTF_ENABLED:
            print(f"⏸ MTF trade disabled by env. {ticker} {direction}")
            return jsonify({"status": "paused"}), 200

        if TRADE_ENABLED:
            try:
                entry_f, stop_f, target_f = float(entry), float(stop), float(target)
                side = "Sell" if direction == "UP" else "Buy"
                set_leverage(ticker, LEVERAGE)
                qty = calc_qty_from_risk(entry_f, stop_f, MAX_RISK_USDT, ticker)
                resp = place_order_market_with_limit_tp_sl(ticker, side, qty, target_f, stop_f)
                print("✅ AUTO-TRADE (MTF) result:", resp)
                send_telegram(f"🚀 *AUTO-TRADE (MTF)* {ticker} {side}\nTP {target}\nSL {stop}")
                log_signal(ticker, direction, tf, "MTF", entry, stop, target)
            except Exception as e:
                print("❌ Trade error (MTF):", e)
        return jsonify({"status": "forwarded"}), 200

# =============== 3️⃣ SCALP (тренд + адаптивный ATR с нормализатором) ===============
    if typ == "SCALP":
        if not SCALP_ENABLED:
            print(f"⏸ SCALP trade disabled by env. {ticker} {direction}")
            return jsonify({"status": "paused"}), 200

    if TRADE_ENABLED:
        try:
            # === БАЗОВЫЕ НАСТРОЙКИ ===
            atr_period = 14
            tf = "1m"
            target_sl_pct = 0.004  # желаемый стоп около 0.4%
            rr_ratio = 1.75          # тейк в 3 раза больше стопа

            # === ПОЛУЧАЕМ ЦЕНУ ВХОДА ===
            entry_f = float(entry) if entry else get_last_price(ticker)
            if not entry_f:
                print(f"⚠️ Нет entry и не удалось получить цену для {ticker}")
                return jsonify({"status": "error"}), 400

            # === ATR ===
            atr = get_atr(ticker, period=atr_period, interval="5")
            if atr <= 0:
                atr = entry_f * 0.002  # fallback, если не удалось получить ATR
                print(f"[ATR warn] {ticker}: fallback ATR {atr:.6f}")

            # === Расчёт стопа и тейка ===
            atr_rel = atr / entry_f
            atr_mult_sl = target_sl_pct / max(atr_rel, 1e-6)
            atr_mult_tp = atr_mult_sl * rr_ratio

            stop_atr = atr * atr_mult_sl
            stop_min = entry_f * 0.003  # минимум 0.3%
            stop_size = max(stop_atr, stop_min)
            take_size = stop_size * rr_ratio

            if direction == "UP":
                stop_f = round(entry_f - stop_size, 6)
                target_f = round(entry_f + take_size, 6)
                side = "Buy"
            else:
                stop_f = round(entry_f + stop_size, 6)
                target_f = round(entry_f - take_size, 6)
                side = "Sell"

            # === проценты ===
            sl_pct = round(abs((entry_f - stop_f) / entry_f) * 100, 3)
            tp_pct = round(abs((target_f - entry_f) / entry_f) * 100, 3)

            msg = (
                f"⚡ SCALP {ticker} {side} | Entry={entry_f:.6f} "
                f"Stop={stop_f:.6f} Target={target_f:.6f} "
                f"(SL={sl_pct}%, TP={tp_pct}%)"
            )
            print(msg)

            # === торговля ===
            set_leverage(ticker, 20)
            qty = calc_qty_from_risk(entry_f, stop_f, MAX_RISK_USDT * 0.5, ticker)
            if qty <= 0:
                print("⚠️ Qty <= 0 — торговля пропущена")
                return jsonify({"status": "skipped"}), 200

            resp = place_order_market_with_limit_tp_sl(ticker, side, qty, target_f, stop_f)
            print("✅ AUTO-TRADE (SCALP) result:", resp)

            # === уведомление ===
            send_telegram(
                f"⚡ *AUTO-TRADE (SCALP)*\n"
                f"{ticker} {side}\n"
                f"Entry~{entry_f}\n"
                f"TP:{target_f} ({tp_pct}%)\n"
                f"SL:{stop_f} ({sl_pct}%)"
            )

            log_signal(ticker, direction, tf, "SCALP", entry_f, stop_f, target_f)

        except Exception as e:
            print("❌ Trade error (SCALP):", e)

    return jsonify({"status": "forwarded"}), 200

# === 5️⃣ FAIL MODE ===
    if typ == "FAIL":
        if not FAIL_ENABLED:
            print(f"⏸ FAIL MODE disabled by env. {ticker} {direction}")
            log_signal(ticker, direction, tf, "FAIL", entry, stop, target)
            return jsonify({"status": "paused"}), 200

        if TRADE_ENABLED:
            try:
                entry_f, stop_f, target_f = float(entry), float(stop), float(target)
                side = "Buy" if direction == "UP" else "Sell"

                set_leverage(ticker, LEVERAGE)
                qty = calc_qty_from_risk(entry_f, stop_f, MAX_RISK_USDT, ticker)
                if qty <= 0:
                    print(f"⚠️ FAIL skipped for {ticker} — qty=0")
                    return jsonify({"status": "skipped"}), 200

                resp = place_order_market_with_limit_tp_sl(
                    ticker, side, qty, target_f, stop_f
                )
                print("✅ AUTO-TRADE (FAIL MODE) result:", resp)

                sl_pct = round(abs(stop_f - entry_f) / entry_f * 100, 3)
                tp_pct = round(abs(target_f - entry_f) / entry_f * 100, 3)

                send_telegram(
                    f"⚡ *AUTO-TRADE (FAIL MODE)*\n"
                    f"{ticker} {side}\n"
                    f"Entry:{entry_f}\n"
                    f"TP:{target_f} ({tp_pct}%)\n"
                    f"SL:{stop_f} ({sl_pct}%)"
                )

                log_signal(ticker, direction, tf, "FAIL", entry, stop, target)

            except Exception as e:
                print("💀 FAIL MODE error:", e)
                return jsonify({"status": "error", "error": str(e)}), 500

        return jsonify({"status": "ok"}), 200

    if typ == "CLUSTER" and CLUSTER_ENABLED and tf in (VALID_TF_5M, VALID_TF_15M, VALID_TF_1H):
        if ticker and direction in ("UP", "DOWN"):
            with lock:
                if tf == VALID_TF_5M:
                    signals_5m.append((time.time(), ticker, direction, tf))
                    print(f"[WH] queued {ticker} {direction} ({tf}) for 5m cluster window")
                elif tf == VALID_TF_15M:
                    signals_15m.append((time.time(), ticker, direction, tf))
                    print(f"[WH] queued {ticker} {direction} ({tf}) for 15m cluster window")
                elif tf == VALID_TF_1H:
                    signals_1h.append((time.time(), ticker, direction, tf))
                    print(f"[WH] queued {ticker} {direction} ({tf}) for 1h cluster window")
            log_signal(ticker, direction, tf, "CLUSTER", entry, stop, target)
            return jsonify({"status": "ok"}), 200

    return jsonify({"status": "ignored"}), 200

# =============== 🧠 КЛАСТЕР-ВОРКЕР 15M ===============

def cluster_worker_15m():
    print("⚙️ cluster_worker_15m started")
    while True:
        try:
            time.sleep(1)
            now = time.time()
            cutoff = now - CLUSTER_WINDOW_MIN * 60

            with lock:
                while signals_15m and signals_15m[0][0] < cutoff:
                    signals_15m.popleft()
                snapshot = list(signals_15m)

            sig_count = len(snapshot)
            if sig_count == 0:
                time.sleep(CHECK_INTERVAL_SEC)
                continue

            try:
                tickers_dbg = [s[1] for s in snapshot]
                dirs_dbg = [s[2] for s in snapshot]
                print(f"[15m][DEBUG] signals len={sig_count}")
                print(f"[15m][DEBUG] Tickers: {tickers_dbg}")
                print(f"[15m][DEBUG] Directions: {dirs_dbg}")
                print(f"[15m][DEBUG] cutoff={cutoff}, first={snapshot[0][0]}, last={snapshot[-1][0]}")
            except Exception:
                pass

            ups, downs, tickers_seen = set(), set(), set()
            for (_, t, d, _) in snapshot:
                tickers_seen.add(t)
                if d == "UP":
                    ups.add(t)
                elif d == "DOWN":
                    downs.add(t)

            print(f"[15m][DEBUG] total={sig_count}, ups={len(ups)}, downs={len(downs)}")

            if len(ups) >= CLUSTER_THRESHOLD:
                if now - last_cluster_sent_15m["UP"] > CLUSTER_WINDOW_MIN * 60:
                    send_telegram(
                        f"🟢 *CLUSTER UP (15m)* — {len(ups)} из {len(tickers_seen)} монет "
                        f"(TF {VALID_TF_15M}, {CLUSTER_WINDOW_MIN} мин)\n"
                        f"📈 {', '.join(sorted(list(ups)))}"
                    )
                    log_signal(",".join(sorted(list(ups))), "UP", VALID_TF_15M, "CLUSTER")
                    last_cluster_sent_15m["UP"] = now
                else:
                    print("[COOLDOWN] skip UP cluster notify")

            if len(downs) >= CLUSTER_THRESHOLD:
                if now - last_cluster_sent_15m["DOWN"] > CLUSTER_WINDOW_MIN * 60:
                    send_telegram(
                        f"🔴 *CLUSTER DOWN (15m)* — {len(downs)} из {len(tickers_seen)} монет "
                        f"(TF {VALID_TF_15M}, {CLUSTER_WINDOW_MIN} мин)\n"
                        f"📉 {', '.join(sorted(list(downs)))}"
                    )
                    log_signal(",".join(sorted(list(downs))), "DOWN", VALID_TF_15M, "CLUSTER")
                    last_cluster_sent_15m["DOWN"] = now
                else:
                    print("[COOLDOWN] skip DOWN cluster notify")

            if TRADE_ENABLED:
                try:
                    direction, ticker = None, None

                    if len(ups) >= CLUSTER_THRESHOLD and ups:
                        direction = "UP"
                        for ts, t, d, _ in reversed(snapshot):
                            if d == "UP" and t in ups:
                                ticker = t
                                break
                        if ticker is None:
                            ticker = next(iter(ups))
                    elif len(downs) >= CLUSTER_THRESHOLD and downs:
                        direction = "DOWN"
                        for ts, t, d, _ in reversed(snapshot):
                            if d == "DOWN" and t in downs:
                                ticker = t
                                break
                        if ticker is None:
                            ticker = next(iter(downs))

                    if not direction:
                        continue

                    if now - last_cluster_trade[direction] < CLUSTER_COOLDOWN_SEC:
                        print(f"[COOLDOWN] Skipping {direction} trade — waiting cooldown.")
                        continue

                    atr_val_local = get_atr(ticker, period=14, interval="15")
                    atr_base_local = get_atr(ticker, period=100, interval="15")
                    ratio_local = atr_val_local / max(atr_base_local, 0.0001)
                    print(f"[LOCAL VOL] {ticker} ratio_local={ratio_local:.2f}")

                    risk_factor = 1.0
                    if ratio_local > 2.2:
                        risk_factor = 0.4
                    elif ratio_local > 1.8:
                        risk_factor = 0.6

                    if risk_factor < 1.0:
                        print(f"[RISK] {ticker} high volatility ({ratio_local:.2f}) — risk scaled ×{risk_factor}")
                        send_telegram(
                            f"⚠️ *CLUSTER AUTO-RISK ADJUST*\n"
                            f"{ticker}: ATR ratio {ratio_local:.2f}\n"
                            f"Risk scaled ×{risk_factor}"
                        )

                    last_cluster_trade[direction] = now

                    if SYMBOL_WHITELIST and ticker not in SYMBOL_WHITELIST:
                        print(f"⛔ {ticker} вне белого списка — пропуск")
                        continue

                    resp = requests.get(
                        f"{BYBIT_BASE_URL}/v5/market/tickers",
                        params={"category": "linear", "symbol": ticker},
                        timeout=5
                    ).json()
                    entry_price = float(resp["result"]["list"][0]["lastPrice"])

                    atr_val = get_atr(ticker, period=14, interval="15")
                    atr_base = get_atr(ticker, period=100, interval="15")

                    raw_scale = atr_val / max(atr_base, 0.0001)
                    vol_scale = 1 + (raw_scale - 1) * 0.5
                    vol_scale = max(0.8, min(vol_scale, 1.2))

                    rr_stop = atr_val * 0.8 * vol_scale
                    rr_target = atr_val * 3.0 * vol_scale

                    stop_price = entry_price + rr_stop if direction == "UP" else entry_price - rr_stop
                    target_price = entry_price - rr_target if direction == "UP" else entry_price + rr_target
                    side = "Sell" if direction == "UP" else "Buy"

                    set_leverage(ticker, LEVERAGE)
                    effective_risk_usdt = MAX_RISK_USDT * risk_factor
                    qty = calc_qty_from_risk(entry_price, stop_price, effective_risk_usdt, ticker)
                    if qty <= 0:
                        raise ValueError("Qty <= 0 after normalization")

                    place_order_market_with_limit_tp_sl(ticker, side, qty, target_price, stop_price)
                    print(f"💥 Cluster auto-trade {ticker} {side} -> TP:{target_price}, SL:{stop_price}")
                    send_telegram(
                        f"⚡ *CLUSTER AUTO-TRADE (15m)*\n"
                        f"{ticker} {side}\n"
                        f"Qty: {qty}\n"
                        f"Entry~{entry_price}\nTP: {target_price}\nSL: {stop_price}\n"
                        f"Volatility ratio: {ratio_local:.2f}, Risk ×{risk_factor}"
                    )

                except Exception as e:
                    print("❌ Cluster auto-trade error (15m):", e)

            time.sleep(CHECK_INTERVAL_SEC)

        except Exception as e:
            print("💀 cluster_worker_15m crashed, restarting in 10s:", e)
            time.sleep(10)

# =============== 🧠 КЛАСТЕР-ВОРКЕР (5M уведомления, без торговли, с настройками из .env) ===============
def cluster_worker_5m():
    global last_cluster_trade
    print("⚙️ cluster_worker_5m started")
    last_cluster_sent_5m = {"UP": 0, "DOWN": 0}
    last_cluster_composition = {"UP": set(), "DOWN": set()}

    while True:
        try:
            time.sleep(CHECK_INTERVAL_SEC)
            now = time.time()
            cutoff = now - CLUSTER_WINDOW_5M_MIN * 60

            with lock:
                while signals_5m and signals_5m[0][0] < cutoff:
                    signals_5m.popleft()
                snapshot = list(signals_5m)

            if not snapshot:
                time.sleep(CHECK_INTERVAL_SEC * 2)
                continue

            ups, downs, tickers_seen = set(), set(), set()
            for (_, t, d, _) in snapshot:
                tickers_seen.add(t)
                if d == "UP":
                    ups.add(t)
                elif d == "DOWN":
                    downs.add(t)

            print(f"[5M DEBUG] signals={len(snapshot)}, ups={len(ups)}, downs={len(downs)}")

            if len(ups) >= CLUSTER_THRESHOLD:
                same_composition = ups == last_cluster_composition["UP"]
                if (now - last_cluster_sent_5m["UP"] > CLUSTER_5M_COOLDOWN_SEC) and not same_composition:
                    send_telegram(
                        f"🟢 *CLUSTER 5M UP* — {len(ups)} из {len(tickers_seen)} монет "
                        f"(TF 5M, окно {CLUSTER_WINDOW_5M_MIN} мин)\n"
                        f"📈 {', '.join(sorted(list(ups)))}",
                        channel="5m"
                    )
                    log_signal(",".join(sorted(list(ups))), "UP", VALID_TF_5M, "CLUSTER_5M")
                    last_cluster_sent_5m["UP"] = now
                    last_cluster_composition["UP"] = set(ups)
                else:
                    print("[5M COOL] skip UP cluster notify")
            
            if len(downs) >= CLUSTER_THRESHOLD:
                same_composition = downs == last_cluster_composition["DOWN"]
                if (now - last_cluster_sent_5m["DOWN"] > CLUSTER_5M_COOLDOWN_SEC) and not same_composition:
                    send_telegram(
                        f"🔴 *CLUSTER 5M DOWN* — {len(downs)} из {len(tickers_seen)} монет "
                        f"(TF 5M, окно {CLUSTER_WINDOW_5M_MIN} мин)\n"
                        f"📉 {', '.join(sorted(list(downs)))}",
                        channel="5m"
                    )
                    log_signal(",".join(sorted(list(downs))), "DOWN", VALID_TF_5M, "CLUSTER_5M")
                    last_cluster_sent_5m["DOWN"] = now
                    last_cluster_composition["DOWN"] = set(downs)
                else:
                    print("[5M COOL] skip DOWN cluster notify")

        except Exception as e:
            print("💀 cluster_worker_5m crashed, restarting in 10s:", e)
            time.sleep(10)

from datetime import datetime, timezone

@app.route("/scalp", methods=["POST"])

# =============== ВОРКЕР БЕКАПА ===============
def backup_log_worker():
    if not BACKUP_ENABLED:
        print("ℹ️ Backup disabled by BACKUP_ENABLED=false")
        return

    last_size = -1
    while True:
        try:
            if os.path.exists(LOG_FILE):
                size_now = os.path.getsize(LOG_FILE)
                should_send = True
                if BACKUP_ONLY_IF_GROWS:
                    should_send = (last_size < 0) or (size_now > last_size)

                if should_send and size_now > 0:
                    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    caption = f"📦 signals_log.csv backup ({ts}) | size: {size_now//1024} KB"
                    ok = send_telegram_document(LOG_FILE, caption=caption)
                    if ok:
                        last_size = size_now
            else:
                print("ℹ️ No log file yet, skipping backup.")
        except Exception as e:
            print("❌ Backup worker error:", e)

        time.sleep(max(60, BACKUP_INTERVAL_MIN * 60))

# =============== 💙 HEARTBEAT В ТЕЛЕГУ ===============
def heartbeat_loop():
    sent_today = None
    while True:
        try:
            now_utc = datetime.utcnow()
            local_time = now_utc + timedelta(hours=2)
            if local_time.hour == 3 and sent_today != local_time.date():
                msg = (
                    f"🩵 *HEARTBEAT*\n"
                    f"Server alive ✅\n"
                    f"⏰ {local_time.strftime('%H:%M %d-%m-%Y')} UTC+2"
                )
                send_telegram(msg)
                print("💬 Heartbeat sent to Telegram.")
                sent_today = local_time.date()
        except Exception as e:
            print("❌ Heartbeat error:", e)
        time.sleep(60)

# =============== 📊 DASHBOARD (последние сигналы) ===============
@app.route("/dashboard")
def dashboard():
    html = [
        "<h2>📈 Active Signals Dashboard</h2>",
        "<table border='1' cellpadding='4'>",
        "<tr><th>Time (UTC+2)</th><th>Ticker</th><th>Direction</th><th>TF</th>"
        "<th>Type</th><th>Entry</th><th>Stop</th><th>Target</th></tr>"
    ]

    try:
        if not os.path.exists(LOG_FILE):
            html.append("<tr><td colspan='8'>⚠️ No log file found</td></tr>")
        else:
            with log_lock:
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()[-50:]

            rows = []
            for line in lines:
                parts = [p.strip() for p in line.strip().split(",")]

                if len(parts) < 5:
                    continue

                while len(parts) < 8:
                    parts.append("")

                t, ticker, direction, tf, sig_type, entry, stop, target = parts[:8]

                try:
                    dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S") + timedelta(hours=2)
                    t = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

                color = "#d4ffd4" if direction == "UP" else "#ffd4d4" if direction == "DOWN" else "#f4f4f4"

                row = (
                    f"<tr style='background-color:{color}'>"
                    f"<td>{html_esc(t)}</td>"
                    f"<td>{html_esc(ticker)}</td>"
                    f"<td>{html_esc(direction)}</td>"
                    f"<td>{html_esc(tf)}</td>"
                    f"<td>{html_esc(sig_type)}</td>"
                    f"<td>{html_esc(entry)}</td>"
                    f"<td>{html_esc(stop)}</td>"
                    f"<td>{html_esc(target)}</td>"
                    f"</tr>"
                )

                rows.append(row)

            if rows:
                html.extend(reversed(rows))
            else:
                html.append("<tr><td colspan='8'>⚠️ No valid log entries</td></tr>")

    except Exception as e:
        html.append(f"<tr><td colspan='8'>⚠️ Error reading log: {html_esc(e)}</td></tr>")

    html.append("</table>")

    local_time = datetime.utcnow() + timedelta(hours=2)
    html.append(
        f"<p style='color:gray'>Updated {html_esc(local_time.strftime('%Y-%m-%d %H:%M:%S UTC+2'))}</p>"
    )
    html.append(
        "<p style='font-size:small;color:gray'>Showing last 50 entries from log</p>"
    )

    return "\n".join(html)

# =============== 📊 /stats (агрегированная статистика с TP/SL) ===============
@app.route("/stats")
def stats():
    if not os.path.exists(LOG_FILE):
        return "<h3>⚠️ Нет данных для анализа</h3>", 200

    try:
        rows = []
        with log_lock:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                for r in csv.reader(f):
                    if len(r) >= 5:
                        rows.append(r)

        parsed = []
        for r in rows:
            try:
                ts = datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S")
                ticker, direction, tf, typ = r[1], r[2], r[3], r[4]
                entry  = float(r[5]) if len(r) > 5 and r[5] else None
                stop   = float(r[6]) if len(r) > 6 and r[6] else None
                target = float(r[7]) if len(r) > 7 and r[7] else None
                result = r[8] if len(r) > 8 else None
                parsed.append((ts, ticker, direction, tf, typ, entry, stop, target, result))
            except Exception:
                continue

        if not parsed:
            return "<h3>⚠️ Лог пуст или повреждён</h3>", 200

        # фильтруем только реальные сделки (SCALP/MTF/FAIL)
        trades = [x for x in parsed if x[4] in ("SCALP", "MTF", "FAIL")]

        total = len(trades)
        if total == 0:
            return "<h3>⚠️ Нет торговых записей</h3>", 200

        # считаем успешные
        success = sum(1 for x in trades if x[8] == "TP")
        fail = sum(1 for x in trades if x[8] == "SL")
        unknown = total - success - fail

        winrate = round(success / total * 100, 2) if total > 0 else 0.0

        # разбивка по направлению
        long_trades = [x for x in trades if x[2] == "UP"]
        short_trades = [x for x in trades if x[2] == "DOWN"]

        long_win = sum(1 for x in long_trades if x[8] == "TP")
        short_win = sum(1 for x in short_trades if x[8] == "TP")

        long_rate = round(long_win / len(long_trades) * 100, 2) if long_trades else 0.0
        short_rate = round(short_win / len(short_trades) * 100, 2) if short_trades else 0.0

        # разбивка по тикерам
        per_ticker = {}
        for x in trades:
            t = x[1]
            res = x[8]
            per_ticker.setdefault(t, {"total": 0, "tp": 0, "sl": 0})
            per_ticker[t]["total"] += 1
            if res == "TP":
                per_ticker[t]["tp"] += 1
            elif res == "SL":
                per_ticker[t]["sl"] += 1

        # таблица по тикерам
        ticker_rows = ""
        for t, v in sorted(per_ticker.items()):
            total_t = v["total"]
            tp = v["tp"]
            sl = v["sl"]
            rate = round(tp / total_t * 100, 2) if total_t > 0 else 0.0
            ticker_rows += (
                f"<tr>"
                f"<td>{html_esc(t)}</td>"
                f"<td>{total_t}</td>"
                f"<td>{tp}</td>"
                f"<td>{sl}</td>"
                f"<td>{rate}%</td>"
                f"</tr>"
            )

        html = f"""
        <h2>📊 Trade Stats (с TP/SL результатами)</h2>
        <ul>
          <li>Всего сделок: <b>{total}</b></li>
          <li>Успешных (TP): <b>{success}</b></li>
          <li>Неудачных (SL): <b>{fail}</b></li>
          <li>Неизвестных: <b>{unknown}</b></li>
          <li>Winrate общий: <b>{winrate}%</b></li>
        </ul>

        <h3>📈 Разбивка по направлению</h3>
        <ul>
          <li>🟢 LONG (UP): {len(long_trades)} | TP {long_win} | Winrate {long_rate}%</li>
          <li>🔴 SHORT (DOWN): {len(short_trades)} | TP {short_win} | Winrate {short_rate}%</li>
        </ul>

        <h3>💎 По тикерам</h3>
        <table border="1" cellpadding="4">
          <tr><th>Ticker</th><th>Всего</th><th>TP</th><th>SL</th><th>Winrate</th></tr>
          {ticker_rows if ticker_rows else '<tr><td colspan="5">Нет данных</td></tr>'}
        </table>

        <p style='color:gray'>Обновлено: {html_esc(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'))}</p>
        """
        return html, 200

    except Exception as e:
        return f"<h3>❌ Ошибка статистики: {html_esc(e)}</h3>", 500

@app.route("/performance")
def performance():
    """
    📊 Сводная статистика по всем сделкам:
    1. Общее количество и % успешных
    2. Разбивка по направлениям (лонг/шорт)
    3. Разбивка по тикерам
    """
    if not os.path.exists(LOG_FILE):
        return "<h3>⚠️ Нет данных для анализа</h3>", 200

    try:
        with log_lock:
            rows = list(csv.reader(open(LOG_FILE, "r", encoding="utf-8")))

        # Пропускаем заголовок, если есть
        header = rows[0]
        if "time_utc" in header[0].lower():
            rows = rows[1:]

        trades = []
        for r in rows:
            try:
                ticker, direction, entry, stop, target = r[1], r[2], float(r[5]), float(r[6]), float(r[7])
                trades.append((ticker, direction, entry, stop, target))
            except Exception:
                continue

        if not trades:
            return "<h3>⚠️ Нет валидных сделок для статистики</h3>", 200

        def trade_result(t):
            _, direction, entry, stop, target = t
            # успех, если движение пошло в сторону тейка (ориентируемся на соотношение)
            profit = (target - entry) if direction == "UP" else (entry - target)
            loss   = (entry - stop)  if direction == "UP" else (stop - entry)
            return profit > loss

        total = len(trades)
        wins = sum(trade_result(t) for t in trades)
        winrate = (wins / total * 100) if total else 0

        # Разбивка по направлениям
        longs = [t for t in trades if t[1] == "UP"]
        shorts = [t for t in trades if t[1] == "DOWN"]
        long_wr = (sum(trade_result(t) for t in longs) / len(longs) * 100) if longs else 0
        short_wr = (sum(trade_result(t) for t in shorts) / len(shorts) * 100) if shorts else 0

        # Разбивка по тикерам
        ticker_stats = {}
        for t in trades:
            ticker = t[0]
            ticker_stats.setdefault(ticker, []).append(t)

        ticker_html = ""
        for ticker, trds in ticker_stats.items():
            w = sum(trade_result(t) for t in trds)
            rate = w / len(trds) * 100
            ticker_html += f"<tr><td>{ticker}</td><td>{len(trds)}</td><td>{w}</td><td>{rate:.1f}%</td></tr>"

        html = f"""
        <h2>📊 Итоговая статистика</h2>
        <ul>
          <li>Всего сделок: <b>{total}</b></li>
          <li>Успешных: <b>{wins}</b> ({winrate:.1f}%)</li>
        </ul>

        <h3>🟩 Лонги</h3>
        <ul><li>{len(longs)} сделок, успешных {long_wr:.1f}%</li></ul>

        <h3>🟥 Шорты</h3>
        <ul><li>{len(shorts)} сделок, успешных {short_wr:.1f}%</li></ul>

        <h3>📈 По тикерам</h3>
        <table border="1" cellpadding="4">
          <tr><th>Монета</th><th>Всего</th><th>Успешных</th><th>Winrate</th></tr>
          {ticker_html}
        </table>
        """
        return html, 200

    except Exception as e:
        return f"<h3>❌ Ошибка анализа: {e}</h3>", 500

# =============== 🧪 SIMULATE (15m + 5m) ===============
@app.route("/simulate", methods=["POST"])
def simulate():
    if WEBHOOK_SECRET:
        key = request.args.get("key", "")
        if key != WEBHOOK_SECRET:
            return "forbidden", 403

    try:
        data = request.get_json(force=True, silent=True) or {}
        ticker    = str(data.get("ticker", "BTCUSDT")).upper()
        direction = str(data.get("direction", "UP")).upper()
        entry     = float(data.get("entry", 68000))
        stop      = float(data.get("stop", 67500))
        target    = float(data.get("target", 69000))
        tf        = str(data.get("tf", VALID_TF_15M))

        msg = (
            f"📊 *SIMULATED SIGNAL*\n"
            f"{ticker} {direction} ({tf})\n"
            f"Entry: {entry}\nStop: {stop}\nTarget: {target}\n"
            f"⏰ {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )
        log_signal(ticker, direction, tf, "SIMULATED", entry, stop, target)
        send_telegram(msg)

        now = time.time()
        if direction in ("UP", "DOWN"):
            with lock:
                if tf == VALID_TF_15M:
                    signals_15m.append((now, ticker, direction, tf))
                    print(f"🧪 [SIM] queued {ticker} {direction} (15m) for cluster window")
                elif tf == VALID_TF_5M:
                    signals_5m.append((now, ticker, direction, tf))
                    print(f"🧪 [SIM] queued {ticker} {direction} (5m) for cluster window")
                else:
                    print(f"⚠️ [SIM] Unknown TF {tf}, ignored")

        return jsonify({
            "status": "ok",
            "ticker": ticker,
            "direction": direction,
            "entry": entry,
            "stop": stop,
            "target": target,
            "tf": tf
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# =============== HEALTH / HEARTBEAT ===============
@app.route("/")
def root():
    return "OK", 200

@app.route("/health")
def health():
    return "OK", 200
    
def monitor_closed_trades():
    """
    🔍 Мониторинг Bybit: ищет закрытые сделки и записывает результат (TP/SL) в лог.
    Работает по всем тикерам, найденным в signals_log.csv.
    """
    print("⚙️ Trade monitor started")
    checked = set()

    while True:
        try:
            time.sleep(60)  # опрос раз в минуту

            if not os.path.exists(LOG_FILE):
                continue

            # читаем логи
            with log_lock:
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    rows = list(csv.reader(f))

            if not rows or len(rows) < 2:
                continue

            header = rows[0]
            if "time_utc" in header[0].lower():
                rows = rows[1:]

            # берём все сделки без RESULT
            open_trades = []
            for r in rows:
                if len(r) < 5:
                    continue
                if len(r) >= 9 and r[8] in ("TP", "SL"):
                    continue
                try:
                    t, ticker, direction, tf, typ, entry, stop, target = r[:8]
                    open_trades.append((ticker, direction, float(entry or 0), float(stop or 0), float(target or 0)))
                except Exception:
                    continue

            if not open_trades:
                continue

            for ticker, direction, entry, stop, target in open_trades:
                key = f"{ticker}_{direction}_{entry}"
                if key in checked:
                    continue
                checked.add(key)

                try:
                    r = requests.get(
                        f"{BYBIT_BASE_URL}/v5/position/list",
                        params={"category": "linear", "symbol": ticker},
                        timeout=5
                    ).json()

                    positions = ((r.get("result") or {}).get("list") or [])
                    pos_size = sum(abs(float(p.get("size", 0))) for p in positions if p.get("symbol") == ticker)
                    if pos_size > 0:
                        continue  # позиция ещё открыта

                    # если позиции нет — проверяем историю ордеров
                    hist = requests.get(
                        f"{BYBIT_BASE_URL}/v5/order/history",
                        params={"category": "linear", "symbol": ticker, "limit": 10},
                        timeout=5
                    ).json()
                    orders = ((hist.get("result") or {}).get("list") or [])

                    # === определяем, чем закрылась позиция ===
                    result = None
                    for o in orders:
                        if o.get("orderStatus") != "Filled":
                            continue

                        side = o.get("side", "")
                        otype = o.get("orderType", "")
                        trig_by = o.get("triggerBy", "")
                        close_trigger = o.get("closeOnTrigger", False)
                        reduce_only = o.get("reduceOnly", False)

                        # лимитный reduceOnly → TP
                        if reduce_only and otype == "Limit":
                            result = "TP"
                            break

                        # маркет с closeOnTrigger → SL
                        if close_trigger:
                            result = "SL"
                            break

                        # fallback по направлению, если Bybit не проставил флаги
                        if direction == "UP" and side == "Sell":
                            result = "TP" if "Limit" in otype else "SL"
                            break
                        elif direction == "DOWN" and side == "Buy":
                            result = "TP" if "Limit" in otype else "SL"
                            break

                    if not result:
                        continue

                    # === дописываем результат в лог ===
                    with log_lock:
                        updated = []
                        with open(LOG_FILE, "r", encoding="utf-8") as f:
                            for line in csv.reader(f):
                                updated.append(line)

                        for row in updated:
                            if len(row) < 8:
                                continue
                            if row[1] == ticker and row[2] == direction and row[5] == str(entry):
                                if len(row) < 9:
                                    row.append(result)
                                else:
                                    row[8] = result
                                break

                        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
                            w = csv.writer(f)
                            for row in updated:
                                w.writerow(row)

                    if result == "TP":
                        threading.Thread(target=monitor_and_cleanup, args=(ticker,), daemon=True).start()
                    send_telegram(f"📘 *Trade closed* {ticker} {direction} → {result}")

                except Exception as e:
                    print(f"❌ Monitor error {ticker}: {e}")

        except Exception as e:
            print("💀 Trade monitor crashed, restarting in 15s:", e)
            time.sleep(15)

# =============== MAIN ===============
if __name__ == "__main__":
    print("🚀 Starting Flask app in single-process mode")

    # Монитор закрытия сделок
    threading.Thread(target=monitor_closed_trades, daemon=True).start()

    # Остальные воркеры
    threading.Thread(target=cluster_worker_15m, daemon=True).start()
    threading.Thread(target=cluster_worker_5m, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=backup_log_worker, daemon=True).start()

    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, use_reloader=False)















