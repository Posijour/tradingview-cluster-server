# app.py
import os, time, json, threading, csv, hmac, hashlib
from datetime import datetime, timedelta
from collections import deque, defaultdict
from flask import Flask, request, jsonify
import requests
from collections import deque
from time import monotonic
tg_times = deque(maxlen=20)
import html as _html
def esc(x): return _html.escape(str(x), quote=True)
import re
MD_ESCAPE = re.compile(r'([_*\[\]()~`>#+\-=|{}.!])')
def md_escape(text: str) -> str:
    return MD_ESCAPE.sub(r'\\\1', text)

# =========================
# 🔧 НАСТРОЙКИ
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID", "766363011")

# Кластеры
CLUSTER_WINDOW_MIN = int(os.getenv("CLUSTER_WINDOW_MIN", "60"))   # окно X мин
CLUSTER_THRESHOLD  = int(os.getenv("CLUSTER_THRESHOLD", "6"))     # N монет
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "60"))   # проверка раз в N сек
VALID_TF = os.getenv("VALID_TF", "15m")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")                  # ?key=... для защиты
CLUSTER_COOLDOWN_SEC = int(os.getenv("CLUSTER_COOLDOWN_SEC", "300"))

# Bybit (опционально)
BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL   = os.getenv("BYBIT_BASE_URL", "https://api-testnet.bybit.com")
TRADE_ENABLED    = os.getenv("TRADE_ENABLED", "false").lower() == "true"
MAX_RISK_USDT    = float(os.getenv("MAX_RISK_USDT", "50"))
LEVERAGE         = float(os.getenv("LEVERAGE", "5"))
SYMBOL_WHITELIST = set(s.strip().upper() for s in os.getenv("SYMBOL_WHITELIST","").split(",") if s.strip())

# Лог
LOG_FILE = "signals_log.csv"  # единое имя используется везде

# =========================
# 🧠 ГЛОБАЛЬНЫЕ СТРУКТУРЫ
signals = deque(maxlen=5000)  # было без maxlen  # элементы: (epoch_sec, ticker, direction, tf)
lock = threading.Lock()
state_lock = threading.Lock()
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}
log_lock = threading.Lock()
app = Flask(__name__)
stop_event = threading.Event()

# =========================
# 📩 Telegram
# =========================
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    try:
        safe = md_escape(text)
        now = monotonic()
        tg_times.append(now)
        # Не чаще 1 сообщения в секунду
        if len(tg_times) >= 2 and now - tg_times[-2] < 1.0:
            time.sleep(1.0 - (now - tg_times[-2]))
        # И не более 20 за минуту
        if len(tg_times) == tg_times.maxlen and now - tg_times[0] < 60:
            time.sleep(60 - (now - tg_times[0]))
        requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": safe, "parse_mode": "MarkdownV2"},
            timeout=8,
        )
        print("✅ Sent to Telegram")
    except Exception as e:
        print("❌ Telegram error:", e)

# =========================
# 📝 ЛОГИРОВАНИЕ
# =========================
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
                f.flush()
        print(f"📝 Logged {sig_type} {ticker} {direction} {tf}")
    except Exception as e:
        print("❌ Log error:", e)

# =========================
# 🔐 BYBIT HELPERS (v5)
# =========================
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

def safe_last_price(symbol: str) -> float:
    try:
        r = requests.get(
            f"{BYBIT_BASE_URL}/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
            timeout=5
        ).json()
        lst = (((r or {}).get("result") or {}).get("list") or [])
        if not lst:
            return 0.0
        return float(lst[0]["lastPrice"])
    except Exception:
        return 0.0

def set_leverage(symbol: str, leverage: float):
    payload = {"category":"linear", "symbol":symbol, "buyLeverage":str(leverage), "sellLeverage":str(leverage)}
    return bybit_post("/v5/position/set-leverage", payload)

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

def calc_qty_from_risk(entry: float, stop: float, risk_usdt: float) -> float:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0
    qty = normalize_qty(symbol_or_ticker, qty)
    return float(f"{qty:.6f}")

# === 📈 Реальный ATR из свечей Bybit (исправлено: сортировка по времени) ===
def get_atr(symbol: str, period: int = 14, interval: str = "15", limit: int = 100) -> float:
    """
    Рассчитывает ATR по последним свечам Bybit (15m по умолчанию).
    Bybit v5 /v5/market/kline возвращает свечи в обратном порядке (новые сначала),
    поэтому сортируем по времени по возрастанию.
    """
    try:
        url = f"{BYBIT_BASE_URL}/v5/market/kline"
        params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
        r = requests.get(url, params=params, timeout=5).json()
        if not isinstance(r, dict) or "result" not in r or not r["result"] or "list" not in r["result"]:
            return 0.0

        candles = r["result"]["list"]  # [ [start, open, high, low, close, volume, ...], ... ]
        if not candles:
            return 0.0

        # Сортируем по времени (start) по возрастанию
        candles.sort(key=lambda c: int(c[0]))

        highs  = [float(c[2]) for c in candles]
        lows   = [float(c[3]) for c in candles]
        closes = [float(c[4]) for c in candles]

        trs = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )
            trs.append(tr)

        if not trs:
            return 0.0

        lookback = min(period, len(trs))
        return sum(trs[-lookback:]) / lookback
    except Exception as e:
        print("ATR fetch error:", e)
        return 0.0

def place_order_market_with_tp_sl(symbol: str, side: str, qty: float, tp: float, sl: float):
    payload = {
        "category": "linear",
        "symbol": symbol,
        "side": side,                # "Buy" | "Sell"
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": "GoodTillCancel",
        "tpSlMode": "Full",
        "takeProfit": str(tp),
        "stopLoss": str(sl),
        "reduceOnly": False
    }
    return bybit_post("/v5/order/create", payload)

# =========================
# 🔍 Парсер входящего payload
# =========================
def parse_payload(req) -> dict:
    # JSON
    data = request.get_json(silent=True) or {}
    # если пусто — пробуем распарсить raw
    if not data:
        raw = req.get_data(as_text=True) or ""
        try:
            data = json.loads(raw)
        except:
            data = {}
    return {
        "type": data.get("type", "").upper(),
        "ticker": data.get("ticker", ""),
        "direction": data.get("direction", "").upper(),
        "tf": data.get("tf", "").lower(),
        "message": data.get("message", ""),
        "entry": data.get("entry"),
        "stop": data.get("stop"),
        "target": data.get("target"),
    }

# =========================
# 🔔 ВЕБХУК ОТ TRADINGVIEW
# =========================
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

    # 1) MTF сигнал, приходящий с готовым текстом + (опц) ценами
    if msg:
        send_telegram(msg)
        print(f"📨 Forwarded MTF alert: {payload.get('ticker')} {payload.get('direction')}")

        # Добавляем в очередь/лог для кластеров (если TF подходит)
        ticker    = payload.get("ticker", "")
        direction = payload.get("direction", "")
        if ticker and direction in ("UP", "DOWN") and tf == VALID_TF:
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))
            log_signal(
                ticker,
                direction,
                tf,
                "MTF",
                payload.get("entry"),
                payload.get("stop"),
                payload.get("target")
            )

        # Попытка автоторговли (если включена)
        if TRADE_ENABLED and typ == "MTF":
            try:
                symbol    = ticker.upper() if ticker else ""
                direction = direction.upper() if direction else ""

                entry  = float(payload.get("entry") or 0)
                stop   = float(payload.get("stop") or 0)
                target = float(payload.get("target") or 0)

                if not (symbol and direction in ("UP","DOWN")):
                    print("⛔ Нет symbol/direction — пропуск торговли")
                elif SYMBOL_WHITELIST and symbol not in SYMBOL_WHITELIST:
                    print(f"⛔ {symbol} не в белом списке — пропуск")
                elif entry > 0 and stop > 0 and target > 0:
                    side = "Sell" if direction == "UP" else "Buy"  # контртренд
                    set_leverage(symbol, LEVERAGE)
                    qty = calc_qty_from_risk(entry, stop, MAX_RISK_USDT)
                    if qty > 0:
                        resp = place_order_market_with_tp_sl(symbol, side, qty, target, stop)
                        print("Bybit order resp:", resp)
                        send_telegram(f"🚀 *AUTO-TRADE*\n{symbol} {side}\nQty: {qty}\nEntry~{entry}\nTP: {target}\nSL: {stop}")
                else:
                    print("ℹ️ Нет entry/stop/target — автоторговля пропущена")
            except Exception as e:
                print("Trade error:", e)

        return jsonify({"status": "forwarded"}), 200

    # 2) Старый/минимальный формат (без message), но с type/dir/tf
    if typ == "MTF" and tf == VALID_TF:
        ticker    = payload.get("ticker", "")
        direction = payload.get("direction", "")
        if ticker and direction in ("UP", "DOWN"):
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))
            log_signal(
                ticker,
                direction,
                tf,
                "MTF",
                payload.get("entry"),
                payload.get("stop"),
                payload.get("target")
            )

            print(f"✅ {ticker} {direction} ({tf}) added for cluster window")
            return jsonify({"status": "ok"}), 200

    return jsonify({"status": "ignored"}), 200

# 🧠 КЛАСТЕР-ВОРКЕР
def cluster_worker():
    while True:
        try:
            try:
                now = time.time()
                cutoff = now - CLUSTER_WINDOW_MIN * 60

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
                    # UP кластер
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

                        # DOWN кластер
                        with state_lock:
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
                        if entry_price <= 0 or atr_val <= 0 or atr_base <= 0:
                            raise ValueError("Bad market/ATR data, skip auto-trade")

                # === 🚀 АВТОТОРГОВЛЯ ПО КЛАСТЕРАМ (с реальным ATR) ===
                if TRADE_ENABLED:
                    try:
                        if len(ups) >= CLUSTER_THRESHOLD and ups:
                            direction = "UP"
                            ticker = list(ups)[0]
                        elif len(downs) >= CLUSTER_THRESHOLD and downs:
                            direction = "DOWN"
                            ticker = list(downs)[0]
                        else:
                            ticker, direction = None, None

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

                                stop_price = entry_price + rr_stop if direction == "UP" else entry_price - rr_stop
                                target_price = entry_price - rr_target if direction == "UP" else entry_price + rr_target

                                side = "Sell" if direction == "UP" else "Buy"
                                qty = normalize_qty(symbol, qty)
                                set_leverage(ticker, LEVERAGE)
                                qty = calc_qty_from_risk(entry_price, stop_price, MAX_RISK_USDT)

                                if qty <= 0:
                                    raise ValueError("Qty <= 0 after normalization")
                                if qty > 0:
                                    resp = place_order_market_with_tp_sl(ticker, side, qty, target_price, stop_price)
                                    print(f"💥 Cluster auto-trade {ticker} {side} -> TP:{target_price}, SL:{stop_price}")
                                    send_telegram(
                                        f"⚡ *CLUSTER AUTO-TRADE*\n{ticker} {side}\nQty: {qty}\n"
                                        f"Entry~{entry_price}\nTP: {target_price}\nSL: {stop_price}"
                                    )
                    except Exception as e:
                        print("❌ Cluster auto-trade error:", e)

            except Exception as inner_e:
                print("❌ cluster_worker internal error:", inner_e)

            time.sleep(CHECK_INTERVAL_SEC)

        except Exception as outer_e:
            print("💀 cluster_worker crashed, restarting in 10s:", outer_e)
            time.sleep(10)

# =========================
# ⏰ ЕЖЕДНЕВНЫЙ HEARTBEAT (03:00 UTC+2)
# =========================
def heartbeat_loop():
    sent_today = None
    while True:
        try:
            now_utc = datetime.utcnow()
            local_time = now_utc + timedelta(hours=2)  # UTC+2
            if local_time.hour == 3 and (sent_today != local_time.date()):
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

# =========================
# =========================
@app.route("/dashboard")
def dashboard():
    html = [
        "<h2>📈 Active Signals Dashboard</h2>",
        "<table border='1' cellpadding='4'>",
        "<tr><th>Time (UTC)</th><th>Ticker</th><th>Direction</th><th>TF</th><th>Type</th>"
        "<th>Entry</th><th>Stop</th><th>Target</th></tr>"
    ]

    try:
        if not os.path.exists(LOG_FILE):
            html.append("<tr><td colspan='8'>⚠️ No log file found</td></tr>")
        else:
            with log_lock:  # ← вот тут магия синхронизации
                with open(LOG_FILE, "r") as f:
                    lines = f.readlines()[-50:]  # последние 50 строк, чтобы не тормозило

            rows = []
            for line in lines:
                parts = [p.strip() for p in line.strip().split(",")]
                # поддержка старых и новых логов
                if len(parts) < 5:
                    continue

                # заполняем недостающие поля, если их нет (для старых логов)
                while len(parts) < 8:
                    parts.append("")

                t, ticker, direction, tf, sig_type, entry, stop, target = parts[:8]

                # красивая подсветка направлений
                color = "#d4ffd4" if direction == "UP" else "#ffd4d4" if direction == "DOWN" else "#f4f4f4"

                row = (
                    f"<tr style='background-color:{color}'>"
                    f"<td>{esc(t)}</td>"
                    f"<td>{esc(ticker)}</td>"
                    f"<td>{esc(direction)}</td>"
                    f"<td>{esc(tf)}</td>"
                    f"<td>{esc(sig_type)}</td>"
                    f"<td>{esc(entry)}</td>"
                    f"<td>{esc(stop)}</td>"
                    f"<td>{esc(target)}</td>"
                    f"</tr>"
                )

                rows.append(row)

            if rows:
                html.extend(reversed(rows))
            else:
                html.append("<tr><td colspan='8'>⚠️ No valid log entries</td></tr>")

    except Exception as e:
        html.append(f"<tr><td colspan='8'>⚠️ Error reading log: {e}</td></tr>")

    html.append("</table>")
    html.append(
        f"<p style='color:gray'>Updated {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>"
    )
    html.append(
        "<p style='font-size:small;color:gray'>Showing last 50 entries from log</p>"
    )
    return "\n".join(html)

# =========================
# =========================
from markupsafe import escape as esc

@app.route("/stats")
def stats():
    if not os.path.exists(LOG_FILE):
        return "<h3>⚠️ Нет данных для анализа</h3>", 200

    try:
        rows = []
        # аккуратно читаем лог с блокировкой
        with log_lock:
            with open(LOG_FILE, "r") as f:
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

        total = len(parsed)
        total_24h = sum(1 for x in parsed if x[0] >= last_24h)
        mtf = sum(1 for x in parsed if x[4] == "MTF")
        cluster = sum(1 for x in parsed if x[4] == "CLUSTER")
        up_count = sum(1 for x in parsed if x[2] == "UP")
        down_count = sum(1 for x in parsed if x[2] == "DOWN")

        # торговая статистика
        with_prices = [x for x in parsed if x[5] and x[6] and x[7]]
        avg_entry  = sum(x[5] for x in with_prices) / len(with_prices) if with_prices else 0
        avg_stop   = sum(x[6] for x in with_prices) / len(with_prices) if with_prices else 0
        avg_target = sum(x[7] for x in with_prices) / len(with_prices) if with_prices else 0

        # последние 10 сигналов с ценами
        last_signals = [x for x in with_prices][-10:]
        last_rows_html = "".join(
            f"<tr><td>{esc(x[0].strftime('%Y-%m-%d %H:%M'))}</td>"
            f"<td>{esc(x[1])}</td><td>{esc(x[2])}</td>"
            f"<td>{x[5]}</td><td>{x[6]}</td><td>{x[7]}</td><td>{esc(x[4])}</td></tr>"
            for x in reversed(last_signals)
        )

        # по дням/типам за 7 дней
        daily = defaultdict(lambda: {"MTF":0, "CLUSTER":0})
        for ts, _, _, _, typ, *_ in parsed:
            if ts >= last_7d:
                key = ts.date().isoformat()
                if typ in ("MTF","CLUSTER"):
                    daily[key][typ] += 1

        daily_html = "".join(
            f"<tr><td>{esc(d)}</td><td>{v['MTF']}</td><td>{v['CLUSTER']}</td></tr>"
            for d, v in sorted(daily.items())
        )

        html = f"""
        <h2>📊 TradingView Signals Stats (7d)</h2>
        <ul>
          <li>Всего сигналов: <b>{total}</b></li>
          <li>За 24 часа: <b>{total_24h}</b></li>
          <li>MTF: <b>{mtf}</b> | Cluster: <b>{cluster}</b></li>
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
          <tr><th>Время</th><th>Тикер</th><th>Направление</th><th>Entry</th><th>Stop</th><th>Target</th><th>Тип</th></tr>
          {last_rows_html if last_rows_html else '<tr><td colspan="7">Нет сигналов</td></tr>'}
        </table>

        <h4>📅 По дням (последние 7):</h4>
        <table border="1" cellpadding="4">
          <tr><th>Дата (UTC)</th><th>MTF</th><th>CLUSTER</th></tr>
          {daily_html if daily_html else '<tr><td colspan="3">Нет данных</td></tr>'}
        </table>
        <p style='color:gray'>Обновлено: {esc(now.strftime("%H:%M:%S UTC"))}</p>
        """
        return html
    except Exception as e:
        return f"<h3>❌ Ошибка анализа: {esc(e)}</h3>", 500

# =========================
@app.route("/simulate", methods=["POST"])
def simulate():
    # простая защита: тот же секрет в заголовке
    if WEBHOOK_SECRET:
        sig = request.headers.get("X-Webhook-Signature", "")
        raw = request.get_data()
        if not verify_signature(WEBHOOK_SECRET, raw, sig):
            return "forbidden", 403
    try:
        data = request.get_json(force=True, silent=True) or {}
        ticker = str(data.get("ticker", "BTCUSDT")).upper()
        direction = str(data.get("direction", "UP")).upper()
        entry = float(data.get("entry", 68000))
        stop = float(data.get("stop", 67500))
        target = float(data.get("target", 69000))
        tf = str(data.get("tf", VALID_TF))

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
            "status": "ok", "ticker": ticker, "direction": direction,
            "entry": entry, "stop": stop, "target": target, "tf": tf
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# =========================
# 🔎 HEALTH / TEST
# =========================
@app.route("/")
def root():
    return "OK", 200

@app.route("/health")
def health():
    return "OK", 200

@app.route("/test")
def test_ping():
    send_telegram("🧪 Test ping from Render server — connection OK.")
    return "Test sent", 200

# app.py (в самый низ)
if __name__ == "__main__":
    import os
    port = int(os.getenv("PORT", "8080"))
    threading.Thread(target=cluster_worker, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=port)


