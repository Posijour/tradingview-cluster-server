# app.py
import os, time, json, threading, hashlib
from collections import deque, defaultdict
from flask import Flask, request, jsonify
import requests

# === Настройки (лучше через ENV) ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID", "766363011")

# Окно кластера и порог
CLUSTER_WINDOW_MIN = int(os.getenv("CLUSTER_WINDOW_MIN", "60"))  # X минут окна
CLUSTER_THRESHOLD  = int(os.getenv("CLUSTER_THRESHOLD", "6"))    # N монет
CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "60"))  # переоценка раз в N сек

# Фильтрация по ТФ (кластер считаем только по 15m)
VALID_TF = os.getenv("VALID_TF", "15m")

# Секрет для вебхука (опционально: добавь ?key=... в TV)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # если пусто — не проверяем

# === Хранилище последних сигналов ===
# Будем хранить (time, ticker, direction, tf)
signals = deque()  # за окно CLUSTER_WINDOW_MIN
lock = threading.Lock()

# Для антидублирования кластер-сообщений (не слать одно и то же каждые N сек)
last_cluster_sent = {"UP": 0.0, "DOWN": 0.0}
CLUSTER_COOLDOWN_SEC = int(os.getenv("CLUSTER_COOLDOWN_SEC", "300"))  # 5 минут

app = Flask(__name__)

def send_telegram(text: str):
    try:
        requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": text},
            timeout=5,
        )
    except Exception as e:
        print("Telegram error:", e)

def parse_payload(req) -> dict:
    """
    Попытка достать из запроса поля:
    type, ticker, direction, tf
    Понимает и JSON, и raw-текст (старый формат).
    """
    data = {}
    try:
        # Сначала пробуем JSON
        data = request.get_json(silent=True) or {}
        # Если совсем пусто, пробуем как текст
        if not data:
            raw = req.get_data(as_text=True) or ""
            # Попытаться выдернуть поля простыми эвристиками
            # Пример: {"type":"MTF","ticker":"BTCUSDT","tf":"15m","direction":"UP"}
            # Если это строка, то хоть как-то распарсить
            for key in ["type", "ticker", "tf", "direction"]:
                # грубый парсер на случай текстов
                marker = f"\"{key}\":"
                if marker in raw:
                    try:
                        # очень простая эвристика: выцепить следующее в кавычках
                        part = raw.split(marker, 1)[1]
                        val = part.split('"')[1]
                        data[key] = val
                    except:
                        pass
    except:
        pass

    # приведение к ожидаемым ключам + дефолты
    return {
        "type": data.get("type", "").upper(),
        "ticker": data.get("ticker", ""),
        "direction": data.get("direction", "").upper(),
        "tf": data.get("tf", "").lower(),
    }

@app.route("/webhook", methods=["POST"])
def webhook():
    # простой секрет через query-параметр ?key=...
    if WEBHOOK_SECRET:
        key = request.args.get("key", "")
        if key != WEBHOOK_SECRET:
            return "forbidden", 403

    payload = parse_payload(request)
    typ = payload.get("type", "")
    tf  = payload.get("tf", "")

    # Нужны только MTF-сигналы на 15m
    if typ != "MTF" or tf != VALID_TF:
        return jsonify({"status":"ignored", "why":"type/tf filter"}), 200

    ticker    = payload.get("ticker", "")
    direction = payload.get("direction", "")

    if not ticker or direction not in ("UP","DOWN"):
        return jsonify({"status":"ignored", "why":"bad payload"}), 200

    now = time.time()
    with lock:
        signals.append((now, ticker, direction, tf))

    print(f"✅ {ticker} {direction} on {tf} at {time.strftime('%H:%M:%S')}")
    return jsonify({"status":"ok"}), 200

def cluster_worker():
    while True:
        try:
            now = time.time()
            cutoff = now - CLUSTER_WINDOW_MIN * 60

            with lock:
                # чистим старые
                while signals and signals[0][0] < cutoff:
                    signals.popleft()

                # агрегируем по направлению с учётом разных тикеров (множество)
                ups  = set()
                downs= set()
                tickers_seen = set()
                for (_, t, d, _) in signals:
                    tickers_seen.add(t)
                    if d == "UP": ups.add(t)
                    if d == "DOWN": downs.add(t)

            # проверка кластера UP
            if len(ups) >= CLUSTER_THRESHOLD:
                if now - last_cluster_sent["UP"] >= CLUSTER_COOLDOWN_SEC:
                    msg = (f"🟢 CLUSTER UP — {len(ups)} из {len(tickers_seen)} монет (TF {VALID_TF}) "
                           f"за {CLUSTER_WINDOW_MIN} мин.\n"
                           f"Tickers: {', '.join(sorted(list(ups)))}")
                    send_telegram(msg)
                    last_cluster_sent["UP"] = now

            # проверка кластера DOWN
            if len(downs) >= CLUSTER_THRESHOLD:
                if now - last_cluster_sent["DOWN"] >= CLUSTER_COOLDOWN_SEC:
                    msg = (f"🔴 CLUSTER DOWN — {len(downs)} из {len(tickers_seen)} монет (TF {VALID_TF}) "
                           f"за {CLUSTER_WINDOW_MIN} мин.\n"
                           f"Tickers: {', '.join(sorted(list(downs)))}")
                    send_telegram(msg)
                    last_cluster_sent["DOWN"] = now

        except Exception as e:
            print("cluster_worker error:", e)

        time.sleep(CHECK_INTERVAL_SEC)

# фоновая проверка кластеров
threading.Thread(target=cluster_worker, daemon=True).start()

@app.route("/")
def health():
    return "OK", 200

if __name__ == "__main__":
    # для локального теста; на проде (Render/Railway) будет WSGI/ASGI
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
