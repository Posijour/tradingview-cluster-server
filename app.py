# app.py
import os, time, json, threading, csv, hmac, hashlib
from datetime import datetime, timedelta
from collections import deque, defaultdict
from flask import Flask, request, jsonify
import requests

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
# =========================
signals = deque()  # элементы: (epoch_sec, ticker, direction, tf)
lock = threading.Lock()
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}

app = Flask(__name__)

# =========================
# 📩 Telegram
# =========================
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    try:
        requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=8,
        )
        print("✅ Sent to Telegram")
    except Exception as e:
        print("❌ Telegram error:", e)

# =========================
# 📝 ЛОГИРОВАНИЕ
# =========================
def log_signal(ticker, direction, tf, sig_type):
    """Сохраняем сигнал в CSV: time,ticker,direction,tf,type"""
    try:
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ticker,
                direction,
                tf,
                sig_type
            ])
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
        return r.json()
    except:
        return {"http": r.status_code, "text": r.text}

def set_leverage(symbol: str, leverage: float):
    payload = {"category":"linear", "symbol":symbol, "buyLeverage":str(leverage), "sellLeverage":str(leverage)}
    return bybit_post("/v5/position/set-leverage", payload)

def calc_qty_from_risk(entry: float, stop: float, risk_usdt: float) -> float:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0.0
    qty = risk_usdt / risk_per_unit
    return float(f"{qty:.6f}")

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
            log_signal(ticker, direction, tf, "MTF")

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
            log_signal(ticker, direction, tf, "MTF")
            print(f"✅ {ticker} {direction} ({tf}) added for cluster window")
            return jsonify({"status": "ok"}), 200

    return jsonify({"status": "ignored"}), 200

# =========================
# 🧠 КЛАСТЕР-ВОРКЕР
# =========================
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
                    if d == "UP":
                        ups.add(t)
                    elif d == "DOWN":
                        downs.add(t)

            # UP кластер
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

        except Exception as e:
            print("❌ cluster_worker error:", e)

        time.sleep(CHECK_INTERVAL_SEC)

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
# 🌐 ПРОСТОЙ DASHBOARD
# =========================
@app.route("/dashboard")
def dashboard():
    html = ["<h2>📈 Active Signals Dashboard</h2><table border='1' cellpadding='4'>"]
    html.append("<tr><th>Time (UTC)</th><th>Ticker</th><th>Direction</th><th>TF</th><th>Type</th></tr>")
    try:
        rows = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                lines = f.readlines()[-50:]
            for line in lines:
                parts = [p.strip() for p in line.strip().split(",")]
                if len(parts) != 5:
                    continue
                t, ticker, direction, tf, sig_type = parts
                rows.append(f"<tr><td>{t}</td><td>{ticker}</td><td>{direction}</td><td>{tf}</td><td>{sig_type}</td></tr>")
        else:
            rows.append("<tr><td colspan='5'>⚠️ No logs yet</td></tr>")
        html.extend(rows)
    except Exception as e:
        html.append(f"<tr><td colspan='5'>⚠️ Error: {e}</td></tr>")
    html.append("</table><p style='color:gray'>Updated {}</p>".format(datetime.utcnow().strftime("%H:%M:%S UTC")))
    return "\n".join(html)

# =========================
# 📊 ЛЁГКАЯ СТАТИСТИКА (без pandas)
# =========================
@app.route("/stats")
def stats():
    if not os.path.exists(LOG_FILE):
        return "<h3>⚠️ Нет данных для анализа</h3>", 200

    try:
        # читаем CSV
        rows = []
        with open(LOG_FILE, "r") as f:
            for r in csv.reader(f):
                if len(r) == 5:
                    rows.append(r)

        # парсим и отбрасываем битые строки
        parsed = []
        for t, ticker, direction, tf, typ in rows:
            try:
                ts = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
                parsed.append((ts, ticker, direction, tf, typ))
            except:
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

        # по дням/типам за 7 дней
        daily = defaultdict(lambda: {"MTF":0, "CLUSTER":0})
        for ts, _, _, _, typ in parsed:
            if ts >= last_7d:
                key = ts.date().isoformat()
                if typ in ("MTF","CLUSTER"):
                    daily[key][typ] += 1

        daily_html = "".join(
            f"<tr><td>{d}</td><td>{v['MTF']}</td><td>{v['CLUSTER']}</td></tr>"
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
        <h4>📅 По дням (последние 7):</h4>
        <table border="1" cellpadding="4">
          <tr><th>Дата (UTC)</th><th>MTF</th><th>CLUSTER</th></tr>
          {daily_html if daily_html else '<tr><td colspan="3">Нет данных</td></tr>'}
        </table>
        <p style='color:gray'>Обновлено: {now.strftime("%H:%M:%S UTC")}</p>
        """
        return html
    except Exception as e:
        return f"<h3>❌ Ошибка анализа: {e}</h3>", 500

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

# =========================
# 🚀 Запуск (локально)
# =========================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

