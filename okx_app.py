# okx_app.py — минимальный автотрейд-сервер под OKX (SCALP)

import os, time, json, math, hmac, base64, threading, requests
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify

app = Flask(__name__)

DEBUG = False

# === ENV ===
OKX_API_KEY       = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET    = os.getenv("OKX_API_SECRET", "")
OKX_PASSPHRASE    = os.getenv("OKX_PASSPHRASE", "")
OKX_BASE_URL      = os.getenv("OKX_BASE_URL", "https://www.okx.com")

WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET_OKX", "")  # можно другой, чтобы не путать с Bybit
TRADE_ENABLED     = os.getenv("TRADE_ENABLED_OKX", "false").lower() == "true"

MAX_RISK_USDT     = float(os.getenv("MAX_RISK_USDT_OKX", "1"))
LEVERAGE          = float(os.getenv("OKX_LEVERAGE", "20"))
BASE_SL_PCT       = float(os.getenv("OKX_BASE_SL_PCT", "0.003"))  # 0.3%
RR_RATIO          = float(os.getenv("OKX_RR_RATIO", "2.4"))       # TP = SL * 2.4

# глобальный кулдаун, как у тебя в bybit-коде
trade_global_cooldown_until = 0


# =============== ВСПОМОГАТЕЛЬНОЕ ===============
def _okx_timestamp() -> str:
    # ISO8601 в UTC, как любит OKX: 2025-11-30T12:34:56.789Z
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _okx_sign(method: str, path: str, body: str = ""):
    """
    method: 'GET' / 'POST'
    path: '/api/v5/trade/order'
    body: строка JSON (для GET обычно пустая)
    """
    ts = _okx_timestamp()
    prehash = f"{ts}{method.upper()}{path}{body}"
    sign = base64.b64encode(
        hmac.new(OKX_API_SECRET.encode(), prehash.encode(), digestmod="sha256").digest()
    ).decode()
    headers = {
        "OK-ACCESS-KEY": OKX_API_KEY,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
        "Content-Type": "application/json",
    }
    return headers


def okx_private_get(path: str, params: dict = None, timeout: int = 10):
    qs = ""
    if params:
        # OKX для приватных GET разрешает querystring просто в URL
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
    if DEBUG:
        print("POST", url, "payload:", payload, "resp:", r.status_code, r.text[:400])
    try:
        j = r.json()
    except Exception:
        return {"http": r.status_code, "text": r.text}
    if j.get("code") not in ("0", 0):
        print("❌ OKX error:", j)
    return j


# === преобразование тикера из Pine -> instId OKX ===
def tv_ticker_to_okx_inst_id(tv_ticker: str) -> str:
    """
    'OKX:ETHUSDT.P' / 'OKX:ETHUSDT' -> 'ETH-USDT-SWAP'
    """
    s = tv_ticker.upper()
    s = s.replace("OKX:", "").replace(".P", "")
    # предполагаем только USDT-кроссы
    if s.endswith("USDT"):
        base = s[:-4]
        quote = "USDT"
        return f"{base}-{quote}-SWAP"
    # fallback, если ты будешь чудить с другими инструментами
    return s


# === получаем размер контракта и minSz, чтобы считать количество ===
_okx_inst_cache = {}

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


def calc_sz_from_risk_okx(entry, stop, risk_usdt, inst_id: str) -> float:
    try:
        entry, stop, risk_usdt = float(entry), float(stop), float(risk_usdt)
    except Exception:
        return 0.0
    if entry <= 0 or stop <= 0 or risk_usdt <= 0:
        return 0.0

    info = get_okx_inst_info(inst_id)
    ct_val = float(info.get("ctVal", "0.001"))         # размер контракта в базовой монете
    min_sz = float(info.get("minSz", "1"))
    lot_sz = float(info.get("lotSz", min_sz))

    price_risk = abs(entry - stop)
    if price_risk <= 1e-12:
        return 0.0

    # риск на один контракт в USDT
    risk_per_contract = price_risk * ct_val
    raw_sz = risk_usdt / risk_per_contract

    # приводим к шагу
    stepped = math.floor(raw_sz / lot_sz) * lot_sz
    sz = max(min_sz, stepped)
    return float(f"{sz:.4f}")


# === простая проверка: есть ли уже открытая позиция по инструменту ===
def okx_position_size(inst_id: str) -> float:
    j = okx_private_get("/api/v5/account/positions", {"instType": "SWAP", "instId": inst_id})
    data = j.get("data") or []
    total = 0.0
    for p in data:
        sz = float(p.get("pos", "0"))
        total += abs(sz)
    return total


# === размещение сделки: Market entry + TP/SL как attachAlgoOrds ===
def okx_place_order_with_tp_sl(inst_id: str, side: str, entry: float, tp: float, sl: float, risk_usdt: float):
    """
    side: 'buy' / 'sell'
    """
    # считаем размер
    sz = calc_sz_from_risk_okx(entry, sl, risk_usdt, inst_id)
    if sz <= 0:
        print(f"⚠️ {inst_id}: sz <= 0, сделка пропущена")
        return {"error": "bad_size"}

    print(f"\n🚀 OKX NEW TRADE {inst_id} {side} sz={sz}, entry≈{entry}, tp={tp}, sl={sl}")

    # tdMode: cross / isolated (предположим cross, как в доке) :contentReference[oaicite:3]{index=3}
    payload = {
        "instId": inst_id,
        "tdMode": "cross",
        "side": side,                 # buy / sell
        "ordType": "market",
        "sz": str(sz),
        # прикручиваем TP/SL к ордеру
        "attachAlgoOrds": [
            {
                "tpTriggerPx": str(tp),
                "tpTriggerPxType": "last",
                "slTriggerPx": str(sl),
                "slTriggerPxType": "last"
            }
        ]
    }

    resp = okx_private_post("/api/v5/trade/order", payload)
    return resp


# =============== ПАРСИНГ PAYLOAD ИЗ PINE ===============
def parse_payload(req):
    data = request.get_json(silent=True) or {}
    raw_ticker = str(data.get("ticker", "")).upper()
    return {
        "type": str(data.get("type", "")).upper(),
        "tv_ticker": raw_ticker,
        "instId": tv_ticker_to_okx_inst_id(raw_ticker),
        "direction": str(data.get("direction", "")).upper(),
        "entry": data.get("entry"),
        "tf": str(data.get("tf", "1m")).lower()
    }


# =============== ВЕБХУК ПОД OKX ===============
@app.route("/webhook_okx", methods=["POST"])
def webhook_okx():
    global trade_global_cooldown_until

    if WEBHOOK_SECRET and request.args.get("key", "") != WEBHOOK_SECRET:
        return "forbidden", 403

    payload = parse_payload(request)
    typ        = payload["type"]
    inst_id    = payload["instId"]
    direction  = payload["direction"]
    entry      = payload["entry"]

    print("\n==== OKX WEBHOOK ====")
    print("RAW JSON:", request.get_json(silent=True))
    print("PARSED:", payload)

    if typ != "SCALP":
        return jsonify({"status": "ignored"}), 200

    now = time.time()
    if now < trade_global_cooldown_until:
        remaining = int(trade_global_cooldown_until - now)
        print(f"⛔ GLOBAL COOLDOWN {remaining}s, сигнал по {inst_id} блокирован")
        return jsonify({"status": "cooldown"}), 200

    # Проверка открытой позиции
    try:
        pos_sz = okx_position_size(inst_id)
        if pos_sz > 0:
            print(f"⏸ {inst_id}: уже есть позиция ({pos_sz}), сигнал пропущен")
            return jsonify({"status": "skipped_open_position"}), 200
    except Exception as e:
        print(f"⚠️ Ошибка чтения позиций OKX: {e}")

    if not TRADE_ENABLED:
        print("🚫 TRADE_DISABLED_OKX")
        return jsonify({"status": "trade_disabled"}), 200

    try:
        entry_f = float(entry)
    except Exception:
        print("⚠️ entry некорректный:", entry)
        return jsonify({"status": "bad_entry"}), 200

    # 0.3% стоп
    stop_size = entry_f * BASE_SL_PCT
    take_size = stop_size * RR_RATIO

    if direction == "UP":
        sl = round(entry_f - stop_size, 6)
        tp = round(entry_f + take_size, 6)
        side = "buy"
    else:
        sl = round(entry_f + stop_size, 6)
        tp = round(entry_f - take_size, 6)
        side = "sell"

    print(f"⚡ OKX SCALP {inst_id} {side} entry={entry_f} sl={sl} tp={tp}")

    resp = okx_place_order_with_tp_sl(
        inst_id=inst_id,
        side=side,
        entry=entry_f,
        tp=tp,
        sl=sl,
        risk_usdt=MAX_RISK_USDT
    )

    # глобальный кулдаун
    trade_global_cooldown_until = time.time() + 180
    print("🕒 GLOBAL COOLDOWN ACTIVATED (OKX) 180s")

    return jsonify({"status": "ok", "okx_resp": resp}), 200


@app.route("/")
def root():
    return "OKX AUTOTRADE OK", 200


if __name__ == "__main__":
    print("🚀 Starting OKX SCALP server")
    port = int(os.getenv("PORT", "8090"))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
