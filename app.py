# app.py
import os, time, json, threading, csv
from datetime import datetime, timedelta
from collections import deque
from flask import Flask, request, jsonify
import requests

# === 🔧 НАСТРОЙКИ ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID", "766363011")

# === ПАРАМЕТРЫ КЛАСТЕРНОГО АНАЛИЗА ===
CLUSTER_WINDOW_MIN = int(os.getenv("CLUSTER_WINDOW_MIN", "60"))   # окно X мин
CLUSTER_THRESHOLD  = int(os.getenv("CLUSTER_THRESHOLD", "6"))     # N монет
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "60"))   # проверка раз в N сек
VALID_TF = os.getenv("VALID_TF", "15m")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")                  # ?key=... для защиты

# === ХРАНИЛИЩЕ СИГНАЛОВ ===
signals = deque()  # элементы: (time, ticker, direction, tf)
lock = threading.Lock()

# Антидубль кластеров
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}
CLUSTER_COOLDOWN_SEC = int(os.getenv("CLUSTER_COOLDOWN_SEC", "300"))

# === ПУТЬ К ЛОГУ ===
LOG_FILE = "signals_log.csv"

app = Flask(__name__)

# === 📩 ФУНКЦИЯ ОТПРАВКИ В TELEGRAM ===
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    try:
        requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=5,
        )
        print("✅ Sent to Telegram")
    except Exception as e:
        print("❌ Telegram error:", e)

# === 📝 ЛОГИРОВАНИЕ СИГНАЛОВ ===
def log_signal(ticker, direction, tf, sig_type):
    """Сохраняем сигнал в CSV"""
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

# === 🔍 РАЗБОР ПОЛУЧЕННОГО PAYLOAD ===
def parse_payload(req) -> dict:
    try:
        data = request.get_json(silent=True) or {}
    except:
        data = {}
    # если JSON пустой — пробуем как текст
    if not data:
        raw = req.get_data(as_text=True) or ""
        try:
            data = json.loads(raw)
        except:
            data = {}

    # нормализуем ключи
    return {
        "type": data.get("type", "").upper(),
        "ticker": data.get("ticker", ""),
        "direction": data.get("direction", "").upper(),
        "tf": data.get("tf", "").lower(),
        "message": data.get("message", ""),
    }

# === 🔔 ОБРАБОТКА ВЕБХУКОВ ОТ TRADINGVIEW ===
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

    # 1️⃣ Если пришло сообщение с "message" — отправляем прямо в Telegram
    if msg:
        send_telegram(msg)
        print(f"📨 Forwarded MTF alert: {payload.get('ticker')} {payload.get('direction')}")
        # при этом тоже добавляем в очередь для кластера
        ticker    = payload.get("ticker", "")
        direction = payload.get("direction", "")
        if ticker and direction in ("UP", "DOWN") and tf == VALID_TF:
            now = time.time()
            with lock:
                signals.append((now, ticker, direction, tf))
            log_signal(ticker, direction, tf, "MTF")
        return jsonify({"status": "forwarded"}), 200

    # 2️⃣ Старый формат (если message нет, но пришёл базовый сигнал)
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

# === 🧠 ФОНОВЫЙ КЛАСТЕРНЫЙ АНАЛИЗ ===
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
                    if d == "DOWN":
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

# === ⏰ ЕЖЕДНЕВНЫЙ HEARTBEAT (Telegram ping в 03:00 UTC+2) ===
def heartbeat_loop():
    import datetime

    sent_today = None
    while True:
        try:
            now_utc = datetime.datetime.utcnow()
            local_time = now_utc + datetime.timedelta(hours=2)
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

# === 🌐 ПРОСТОЙ DASHBOARD ===
@app.route("/dashboard")
def dashboard():
    html = ["<h2>📈 Active Signals Dashboard</h2><table border='1' cellpadding='4'>"]
    html.append("<tr><th>Time (UTC)</th><th>Ticker</th><th>Direction</th><th>TF</th><th>Type</th></tr>")
    try:
        rows = []
        with open(LOG_FILE, "r") as f:
            for line in f.readlines()[-30:]:
                t, ticker, direction, tf, sig_type = line.strip().split(",")
                rows.append(f"<tr><td>{t}</td><td>{ticker}</td><td>{direction}</td><td>{tf}</td><td>{sig_type}</td></tr>")
        html.extend(rows)
    except Exception as e:
        html.append(f"<tr><td colspan='5'>⚠️ No logs yet ({e})</td></tr>")
    html.append("</table><p style='color:gray'>Updated {}</p>".format(datetime.utcnow().strftime("%H:%M:%S UTC")))
    return "\n".join(html)

# === ЗАПУСК ФОНОВЫХ ПОТОКОВ ===
threading.Thread(target=cluster_worker, daemon=True).start()
threading.Thread(target=heartbeat_loop, daemon=True).start()

@app.route("/")
def root():
    return "OK", 200

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/test")
def test_ping():
    send_telegram("🧪 Test ping from Render server — connection OK.")
    return "Test sent", 200

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)












