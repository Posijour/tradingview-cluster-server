import os
import json
import time
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# === ENV ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID        = os.getenv("CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET_3WAVES", "")

# окно совпадения, секунд (±10 минут)
WINDOW_SEC = int(os.getenv("WINDOW_SEC", "600"))

# хранение последних сигналов
events_3m = deque()  # [{"ticker":..., "time_ms":...}, ...]
events_5m = deque()


# === Telegram ===
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    try:
        requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": text},
            timeout=8,
        )
    except Exception as e:
        print("❌ Telegram error:", e)


def ms_to_str(ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ms)


def prune(queue: deque, now_ms: int, window_ms: int):
    while queue and (now_ms - queue[0]["time_ms"] > window_ms):
        queue.popleft()


def handle_event(ticker: str, tf: str, time_ms: int):
    """
    tf: "3" или "5" (из timeframe.period)
    time_ms: время бара из Pine (ms)
    """
    window_ms = WINDOW_SEC * 1000

    # чистим старые
    prune(events_3m, time_ms, window_ms)
    prune(events_5m, time_ms, window_ms)

    if tf == "5":
        # новый 5m-сигнал → ищем 3m-сигналы вокруг
        matches = [
            e for e in events_3m
            if e["ticker"] == ticker and abs(e["time_ms"] - time_ms) <= window_ms
        ]
        events_5m.append({"ticker": ticker, "time_ms": time_ms})

        if matches:
            # берём ближайший по времени
            best = min(matches, key=lambda e: abs(e["time_ms"] - time_ms))
            send_cluster_alert(ticker, time_ms_5m=time_ms, time_ms_3m=best["time_ms"])

    elif tf == "3":
        # новый 3m-сигнал → ищем 5m-сигналы вокруг
        matches = [
            e for e in events_5m
            if e["ticker"] == ticker and abs(e["time_ms"] - time_ms) <= window_ms
        ]
        events_3m.append({"ticker": ticker, "time_ms": time_ms})

        if matches:
            best = min(matches, key=lambda e: abs(e["time_ms"] - time_ms))
            send_cluster_alert(ticker, time_ms_5m=best["time_ms"], time_ms_3m=time_ms)
    else:
        print(f"⚠️ unknown tf: {tf}")


def send_cluster_alert(ticker: str, time_ms_5m: int, time_ms_3m: int):
    txt = (
        "⚡ 3 WAVES UP CLUSTER\n"
        f"{ticker}\n"
        f"5m: {ms_to_str(time_ms_5m)}\n"
        f"3m: {ms_to_str(time_ms_3m)}\n"
        f"Окно: ±{WINDOW_SEC // 60} минут"
    )
    print(txt)
    send_telegram(txt)


# === Webhook ===
@app.route("/webhook_3waves", methods=["POST"])
def webhook_3waves():
    # простой секрет в query
    if WEBHOOK_SECRET and request.args.get("key", "") != WEBHOOK_SECRET:
        return "forbidden", 403

    raw = request.get_data(as_text=True)
    print("\n=== 3WAVES WEBHOOK RAW ===")
    print(raw)

    data = request.get_json(silent=True) or {}
    print("JSON:", data)

    typ    = str(data.get("type", "")).upper()
    ticker = str(data.get("ticker", ""))
    tf     = str(data.get("tf", ""))
    t_ms   = data.get("time")

    # минимальная валидация
    if typ != "3WAVESUP":
        return jsonify({"status": "ignored_type"}), 200

    try:
        t_ms = int(t_ms)
    except Exception:
        print("⚠️ bad time in payload:", t_ms)
        return jsonify({"status": "bad_time"}), 200

    if tf not in ("3", "5"):
        print("⚠️ unexpected tf:", tf)
        # всё равно примем, но кластер логика может пропустить
    handle_event(ticker, tf, t_ms)

    return jsonify({"status": "ok"}), 200


@app.route("/")
def root():
    return "3WAVES CLUSTER OK", 200


@app.route("/health")
def health():
    return "OK", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    print(f"🚀 Starting 3WAVES cluster server on {port}")
    app.run(host="0.0.0.0", port=port, use_reloader=False)
