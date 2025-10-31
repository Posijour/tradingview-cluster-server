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
CLUSTER_WINDOW_MIN     = int(os.getenv("CLUSTER_WINDOW_MIN", "60"))     # окно кластеров в минутах
CLUSTER_THRESHOLD      = int(os.getenv("CLUSTER_THRESHOLD", "6"))       # сколько разных монет в одну сторону, чтобы это считалось кластером
CHECK_INTERVAL_SEC     = int(os.getenv("CHECK_INTERVAL_SEC", "60"))     # как часто воркер проверяет
VALID_TF               = os.getenv("VALID_TF", "15m")                   # какой tf мы вообще учитываем
WEBHOOK_SECRET         = os.getenv("WEBHOOK_SECRET", "")                # защита /webhook?key=...
CLUSTER_COOLDOWN_SEC   = int(os.getenv("CLUSTER_COOLDOWN_SEC", "300"))  # антиспам для кластеров

# Bybit
BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL   = os.getenv("BYBIT_BASE_URL", "https://api-testnet.bybit.com")
TRADE_ENABLED    = os.getenv("TRADE_ENABLED", "false").lower() == "true"
MAX_RISK_USDT    = float(os.getenv("MAX_RISK_USDT", "50"))
LEVERAGE         = float(os.getenv("LEVERAGE", "5"))

# если хочешь ограничить автоторговлю на конкретные тикеры:
# пример: SYMBOL_WHITELIST=BTCUSDT,ETHUSDT,SOLUSDT
SYMBOL_WHITELIST = set(
    s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST","").split(",") if s.strip()
)

# Лог-файл
LOG_FILE = "signals_log.csv"

# =============== 🧠 ГЛОБАЛЬНЫЕ СТРУКТУРЫ СОСТОЯНИЯ ===============
signals = deque(maxlen=5000)  # (timestamp, ticker, direction, tf)
lock = threading.Lock()
state_lock = threading.Lock()
log_lock = threading.Lock()
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}

app = Flask(__name__)

# =============== 🌐 Утилиты форматирования ===============
tg_times = deque(maxlen=20)

MD_ESCAPE = re.compile(r'([_*\[\]()~`>#+\-=|{}.!])')
def md_escape(text: str) -> str:
    # телеграм-вечный ад: экранировать markdownv2
    return MD_ESCAPE.sub(r'\\\1', text)

def html_esc(x):
    # для html-дэшбордов
    return _html.escape(str(x), quote=True)

# =============== 🔐 Верификация подписи (используется в /simulate) ===============
def verify_signature(secret, body, signature):
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)

# =============== 📩 Telegram отправка с антифлудом ===============
def send_telegram(text: str):
    """
    Отправляет сообщение в Telegram в ФОНОВОМ потоке.
    Весь антифлуд (не чаще 1/сек, не более 20/мин) теперь внутри потока,
    чтобы основной HTTP-обработчик не зависал на sleep().
    """
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return

    safe_text = md_escape(text)
    
    def send_telegram_document(filepath: str, caption: str = ""):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return False
    try:
        if not os.path.exists(filepath):
            print(f"⚠️ Document not found: {filepath}")
            return False

        # Telegram ограничение ~50 МБ на файл. Проверим на всякий.
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if size_mb > 49.5:
            print(f"⚠️ File too large for Telegram: {size_mb:.1f} MB")
            return False

        files = {"document": (os.path.basename(filepath), open(filepath, "rb"))}
        data = {"chat_id": CHAT_ID, "caption": caption[:1024]}  # safety: caption <= 1024
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

    def _send_with_rate_limit():
        try:
            # антифлуд вынесен сюда
            now = monotonic()
            tg_times.append(now)

            # не чаще 1 сообщения в секунду
            if len(tg_times) >= 2 and now - tg_times[-2] < 1.0:
                time.sleep(1.0 - (now - tg_times[-2]))

            # и не более 20 за минуту
            if len(tg_times) == tg_times.maxlen and now - tg_times[0] < 60:
                time.sleep(60 - (now - tg_times[0]))

            requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                params={"chat_id": CHAT_ID, "text": safe_text, "parse_mode": "MarkdownV2"},
                timeout=8,
            )
            print("✅ Sent to Telegram")
        except Exception as e:
            print("❌ Telegram error:", e)

    threading.Thread(target=_send_with_rate_limit, daemon=True).start()

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

import math

def _decimals_from_step(step_str: str) -> int:
    """
    Определяет, сколько знаков после запятой нужно оставить,
    исходя из шага qtyStep. Корректно работает даже с 1e-3.
    """
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
    """
    Нормализует количество до минимального шага Bybit (qtyStep).
    Учитывает minOrderQty и корректно округляет вниз.
    """
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

import math

def calc_qty_from_risk(entry: float, stop: float, risk_usdt: float, symbol: str) -> float:
    """
    Возвращает количество контрактов под риск в USDT.
    Жёстко фильтрует нули/NaN/inf, не даёт отрицательных значений,
    перед нормализацией приводит всё к float.
    """
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

def set_leverage(symbol: str, leverage: float):
    payload = {"category":"linear", "symbol":symbol, "buyLeverage":str(leverage), "sellLeverage":str(leverage)}
    return bybit_post("/v5/position/set-leverage", payload)

def place_order_market_with_tp_sl(symbol: str, side: str, qty: float, tp: float, sl: float):
    payload = {
        "category": "linear",
        "symbol": symbol,
        "side": side,            # "Buy" | "Sell"
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
    """
    ATR по реальным свечам Bybit.
    Bybit даёт свечи newest-first, мы пересортировываем по времени по возрастанию.
    """
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        r = requests.get(url, params=params, timeout=5).json()
        candles = (((r or {}).get("result") or {}).get("list") or [])
        if not candles:
            return 0.0

        # сортируем по timestamp (поле [0])
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
    """
    Поддерживает ТВОЙ текщий формат Pine:
    {
      "type":"MTF",
      "ticker":"BYBIT:BTCUSDT.P",
      "direction":"UP"|"DOWN",
      "entry":12345,
      "stop":12300,
      "target":12400,
      "message":"строка для телеги"
      // tf иногда есть, иногда нет
    }
    """
    data = request.get_json(silent=True) or {}
    if not data:
        try:
            data = json.loads(req.data)
        except Exception:
            data = {}

    # Приводим тикер в формат для Bybit:
    # "BYBIT:BTCUSDT.P" -> "BTCUSDT"
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
        "tf":        str(data.get("tf", "15m")).lower(),  # если tf не пришёл из Pine, считаем что 15m
        "message":   data.get("message", ""),
        "entry":     data.get("entry"),
        "stop":      data.get("stop"),
        "target":    data.get("target"),
    }

# =============== 🔔 ВЕБХУК ОТ TRADINGVIEW ===============
@app.route("/webhook", methods=["POST"])
def webhook():
    # простейшая защита url ?key=SECRET
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

    # 1) У тебя Pine всегда присылает красивый текст в message.
    # Если message есть — это «боевой» сигнал.
    if msg:
        # кинуть в телеграм
        send_telegram(msg)
        print(f"📨 Forwarded MTF alert: {ticker} {direction}")

        # лог + добавить в очередь для кластеров
        if ticker and direction in ("UP","DOWN") and tf == VALID_TF:
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))

            log_signal(
                ticker,
                direction,
                tf,
                "MTF",
                entry,
                stop,
                target
            )

        # автоторговля по одиночному MTF сигналу
        if TRADE_ENABLED and typ == "MTF":
            try:
                if not (ticker and direction in ("UP","DOWN")):
                    print("⛔ Нет symbol/direction — пропуск торговли")
                elif SYMBOL_WHITELIST and ticker not in SYMBOL_WHITELIST:
                    print(f"⛔ {ticker} не в белом списке — пропуск")
                elif entry and stop and target:
                    # твоя логика контртренда:
                    # direction "UP" -> шорт
                    # direction "DOWN" -> лонг
                    side = "Sell" if direction == "UP" else "Buy"

                    set_leverage(ticker, LEVERAGE)

                    qty = calc_qty_from_risk(
                        float(entry),
                        float(stop),
                        MAX_RISK_USDT,
                        ticker
                    )

                    if qty > 0:
                        resp = place_order_market_with_tp_sl(
                            ticker,
                            side,
                            qty,
                            float(target),
                            float(stop)
                        )
                        print("Bybit order resp:", resp)
                        send_telegram(
                            f"🚀 *AUTO-TRADE*\n"
                            f"{ticker} {side}\n"
                            f"Qty: {qty}\n"
                            f"Entry~{entry}\nTP: {target}\nSL: {stop}"
                        )
                else:
                    print("ℹ️ Нет entry/stop/target — пропуск автоторговли")
            except Exception as e:
                print("Trade error:", e)

        return jsonify({"status": "forwarded"}), 200

    # 2) fallback: вдруг Pine пришлёт без message
    if typ == "MTF" and tf == VALID_TF:
        if ticker and direction in ("UP", "DOWN"):
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))
            log_signal(ticker, direction, tf, "MTF", entry, stop, target)
            print(f"✅ {ticker} {direction} ({tf}) added for cluster window")
            return jsonify({"status": "ok"}), 200

    # ничего полезного
    return jsonify({"status": "ignored"}), 200

# =============== 🧠 КЛАСТЕР-ВОРКЕР ===============
def cluster_worker():
    while True:
        try:
            now = time.time()
            cutoff = now - CLUSTER_WINDOW_MIN * 60

            # чистим старые сигналы и считаем апы/дауны
            with lock:
                while signals and signals[0][0] < cutoff:
                    signals.popleft()

                ups, downs, tickers_seen = set(), set(), set()
                for (_, t, d, _) in signals:
                    tickers_seen.add(t)
                    if d == "UP":
                        ups.add(t)
                    elif d == "DOWN":
                        downs.add(t)

            # шлём кластеры в телегу и лог, но не чаще cooldown
            with state_lock:
                if len(ups) >= CLUSTER_THRESHOLD:
                    if now - last_cluster_sent["UP"] >= CLUSTER_COOLDOWN_SEC:
                        msg = (
                            f"🟢 *CLUSTER UP* — {len(ups)} из {len(tickers_seen)} монет "
                            f"(TF {VALID_TF}, {CLUSTER_WINDOW_MIN} мин)\n"
                            f"📈 {', '.join(sorted(list(ups)))}"
                        )
                        send_telegram(msg)
                        log_signal(",".join(sorted(list(ups))), "UP", VALID_TF, "CLUSTER")
                        last_cluster_sent["UP"] = now

                if len(downs) >= CLUSTER_THRESHOLD:
                    if now - last_cluster_sent["DOWN"] >= CLUSTER_COOLDOWN_SEC:
                        msg = (
                            f"🔴 *CLUSTER DOWN* — {len(downs)} из {len(tickers_seen)} монет "
                            f"(TF {VALID_TF}, {CLUSTER_WINDOW_MIN} мин)\n"
                            f"📉 {', '.join(sorted(list(downs)))}"
                        )
                        send_telegram(msg)
                        log_signal(",".join(sorted(list(downs))), "DOWN", VALID_TF, "CLUSTER")
                        last_cluster_sent["DOWN"] = now

            # автоторговля по кластерам (если включена)
            if TRADE_ENABLED:
                try:
                    direction, ticker = None, None

                    # выбираем любую монету из кластера
                    if len(ups) >= CLUSTER_THRESHOLD and ups:
                        direction = "UP"
                        ticker = list(ups)[0]
                    elif len(downs) >= CLUSTER_THRESHOLD and downs:
                        direction = "DOWN"
                        ticker = list(downs)[0]

                    if ticker and direction:
                        if SYMBOL_WHITELIST and ticker not in SYMBOL_WHITELIST:
                            print(f"⛔ {ticker} не в белом списке — пропуск кластерной торговли")
                        else:
                            # текущая цена
                            resp = requests.get(
                                f"{BYBIT_BASE_URL}/v5/market/tickers",
                                params={"category": "linear", "symbol": ticker},
                                timeout=5
                            ).json()
                            entry_price = float(resp["result"]["list"][0]["lastPrice"])

                            # ATR-логика для стопа/таргета
                            atr_val = get_atr(ticker, period=14, interval="15")
                            atr_base = get_atr(ticker, period=100, interval="15")
                            vol_scale = max(0.7, min(atr_val / max(atr_base, 0.0001), 1.3))

                            rr_stop   = atr_val * 0.8 * vol_scale
                            rr_target = atr_val * 2.4 * vol_scale

                            stop_price   = entry_price + rr_stop   if direction == "UP"   else entry_price - rr_stop
                            target_price = entry_price - rr_target if direction == "UP"   else entry_price + rr_target

                            side = "Sell" if direction == "UP" else "Buy"

                            set_leverage(ticker, LEVERAGE)
                            qty = calc_qty_from_risk(entry_price, stop_price, MAX_RISK_USDT, ticker)

                            if qty <= 0:
                                raise ValueError("Qty <= 0 after normalization")

                            resp2 = place_order_market_with_tp_sl(
                                ticker,
                                side,
                                qty,
                                target_price,
                                stop_price
                            )
                            print(f"💥 Cluster auto-trade {ticker} {side} -> TP:{target_price}, SL:{stop_price}")
                            send_telegram(
                                f"⚡ *CLUSTER AUTO-TRADE*\n"
                                f"{ticker} {side}\n"
                                f"Qty: {qty}\n"
                                f"Entry~{entry_price}\nTP: {target_price}\nSL: {stop_price}"
                            )

                except Exception as e:
                    print("❌ Cluster auto-trade error:", e)

            time.sleep(CHECK_INTERVAL_SEC)

        except Exception as e:
            # не умирать вообще никогда
            print("💀 cluster_worker crashed, restarting in 10s:", e)
            time.sleep(10)

# =============== ВОРКЕР БЕКАПА ===============
def backup_log_worker():
    """
    Раз в BACKUP_INTERVAL_MIN минут шлёт файл signals_log.csv в Телеграм.
    Если BACKUP_ONLY_IF_GROWS=true — шлёт только если размер файла увеличился
    с последнего раза (защита от спама одинаковыми копиями).
    """
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
                    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
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
            local_time = now_utc + timedelta(hours=2)  # UTC+2 фиксированно
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
        "<tr><th>Time (UTC)</th><th>Ticker</th><th>Direction</th><th>TF</th>"
        "<th>Type</th><th>Entry</th><th>Stop</th><th>Target</th></tr>"
    ]

    try:
        if not os.path.exists(LOG_FILE):
            html.append("<tr><td colspan='8'>⚠️ No log file found</td></tr>")
        else:
            with log_lock:
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()[-50:]  # последние 50 строк

            rows = []
            for line in lines:
                parts = [p.strip() for p in line.strip().split(",")]

                if len(parts) < 5:
                    continue

                # старые строки могут быть короче
                while len(parts) < 8:
                    parts.append("")

                t, ticker, direction, tf, sig_type, entry, stop, target = parts[:8]

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
    html.append(
        f"<p style='color:gray'>Updated {html_esc(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'))}</p>"
    )
    html.append(
        "<p style='font-size:small;color:gray'>Showing last 50 entries from log</p>"
    )

    return "\n".join(html)

# =============== 📊 /stats (агрегированная статистика) ===============
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
                parsed.append((ts, ticker, direction, tf, typ, entry, stop, target))
            except Exception:
                continue

        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        last_7d  = now - timedelta(days=7)

        total         = len(parsed)
        total_24h     = sum(1 for x in parsed if x[0] >= last_24h)
        mtf_count     = sum(1 for x in parsed if x[4] == "MTF")
        cluster_count = sum(1 for x in parsed if x[4] == "CLUSTER")
        up_count      = sum(1 for x in parsed if x[2] == "UP")
        down_count    = sum(1 for x in parsed if x[2] == "DOWN")

        with_prices = [x for x in parsed if x[5] and x[6] and x[7]]
        avg_entry  = sum(x[5] for x in with_prices) / len(with_prices) if with_prices else 0
        avg_stop   = sum(x[6] for x in with_prices) / len(with_prices) if with_prices else 0
        avg_target = sum(x[7] for x in with_prices) / len(with_prices) if with_prices else 0

        # последние 10 сигналов с ценами
        last_signals = with_prices[-10:]
        last_rows_html = "".join(
            f"<tr>"
            f"<td>{html_esc(x[0].strftime('%Y-%m-%d %H:%M'))}</td>"
            f"<td>{html_esc(x[1])}</td>"
            f"<td>{html_esc(x[2])}</td>"
            f"<td>{x[5]}</td>"
            f"<td>{x[6]}</td>"
            f"<td>{x[7]}</td>"
            f"<td>{html_esc(x[4])}</td>"
            f"</tr>"
            for x in reversed(last_signals)
        )

        # активность по дням за 7 дней
        daily = defaultdict(lambda: {"MTF":0, "CLUSTER":0})
        for ts, _, _, _, typ, *_ in parsed:
            if ts >= last_7d:
                key = ts.date().isoformat()
                if typ in ("MTF","CLUSTER"):
                    daily[key][typ] += 1

        daily_html = "".join(
            f"<tr>"
            f"<td>{html_esc(d)}</td>"
            f"<td>{v['MTF']}</td>"
            f"<td>{v['CLUSTER']}</td>"
            f"</tr>"
            for d, v in sorted(daily.items())
        )

        html = f"""
        <h2>📊 TradingView Signals Stats (7d)</h2>
        <ul>
          <li>Всего сигналов: <b>{total}</b></li>
          <li>За 24 часа: <b>{total_24h}</b></li>
          <li>MTF: <b>{mtf_count}</b> | Cluster: <b>{cluster_count}</b></li>
          <li>Направление — 🟢 UP: <b>{up_count}</b> | 🔴 DOWN: <b>{down_count}</b></li>
        </ul>

        <h3>📈 Средние цены сигналов</h3>
        <ul>
          <li>Entry: <b>{avg_entry:.2f}</b></li>
          <li>Stop: <b>{avg_stop:.2f}</b></li>
          <li>Target: <b>{avg_target:.2f}</b></li>
        </ul>

        <h3>🕒 Последние 10 сигналов</h3>
        <table border="1" cellpadding="4">
          <tr><th>Время (UTC)</th><th>Тикер</th><th>Направление</th><th>Entry</th><th>Stop</th><th>Target</th><th>Тип</th></tr>
          {last_rows_html if last_rows_html else '<tr><td colspan="7">Нет сигналов</td></tr>'}
        </table>

        <h4>📅 По дням (последние 7):</h4>
        <table border="1" cellpadding="4">
          <tr><th>Дата (UTC)</th><th>MTF</th><th>CLUSTER</th></tr>
          {daily_html if daily_html else '<tr><td colspan="3">Нет данных</td></tr>'}
        </table>

        <p style='color:gray'>Обновлено: {html_esc(now.strftime("%H:%M:%S UTC"))}</p>
        """
        return html, 200

    except Exception as e:
        return f"<h3>❌ Ошибка анализа: {html_esc(e)}</h3>", 500

# =============== 🧪 /simulate — тест руками без TradingView ===============
@app.route("/simulate", methods=["POST"])
def simulate():
    # разрешаем тот же ?key=... что и в /webhook
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
        tf        = str(data.get("tf", VALID_TF))

        msg = (
            f"📊 *SIMULATED SIGNAL*\n"
            f"{ticker} {direction} ({tf})\n"
            f"Entry: {entry}\nStop: {stop}\nTarget: {target}\n"
            f"⏰ {datetime.utcnow().strftime('%H:%M:%S UTC')}"
        )

        log_signal(ticker, direction, tf, "SIMULATED", entry, stop, target)
        send_telegram(msg)

        print(f"🧪 Simulated signal sent for {ticker} {direction}")
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

# =============== MAIN ===============
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))

    # воркеры запускаются в фоне
    threading.Thread(target=cluster_worker, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=backup_log_worker, daemon=True).start()

    # веб-сервер
    app.run(host="0.0.0.0", port=port)



