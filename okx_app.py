# okx_app.py — минимальный автотрейд-сервер под OKX (SCALP)

import os, time, json, math, hmac, base64, threading, requests, re
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify

app = Flask(__name__)

DEBUG = False

# === ENV ===
OKX_API_KEY       = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET    = os.getenv("OKX_API_SECRET", "")
OKX_PASSPHRASE    = os.getenv("OKX_PASSPHRASE", "")
OKX_POS_MODE      = os.getenv("OKX_POS_MODE", "net")  # 'net' или 'hedge'
OKX_BASE_URL      = os.getenv("OKX_BASE_URL", "https://www.okx.com")
OKX_LONG_DAYS_ENV  = os.getenv("OKX_LONG_DAYS",  "0,1,2,3,4,5,6")
OKX_SHORT_DAYS_ENV = os.getenv("OKX_SHORT_DAYS", "0,1,2,3,4,5,6")
OKX_LONG_HOURS_ENV  = os.getenv("OKX_LONG_HOURS",  "0-3,3-6,6-9,9-12,12-15,15-18,18-21,21-24")
OKX_SHORT_HOURS_ENV = os.getenv("OKX_SHORT_HOURS", "0-3,3-6,6-9,9-12,12-15,15-18,18-21,21-24")

WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET_OKX", "")  # можно другой, чтобы не путать с Bybit
TRADE_ENABLED     = os.getenv("TRADE_ENABLED_OKX", "false").lower() == "true"

MAX_RISK_USDT     = float(os.getenv("MAX_RISK_USDT_OKX", "1"))
LEVERAGE          = float(os.getenv("OKX_LEVERAGE", "20"))
BASE_SL_PCT       = float(os.getenv("OKX_BASE_SL_PCT", "0.003"))  # 0.3%
RR_RATIO          = float(os.getenv("OKX_RR_RATIO", "2.4"))       # TP = SL * 2.4

def _parse_days(txt: str):
    try:
        return {int(x.strip()) for x in txt.split(",") if x.strip() != ""}
    except Exception:
        return set()  # если мусор в env — считаем, что ограничений по дням нет

def _parse_hour_ranges(txt: str):
    """
    Парсит строки вида "0-2,3-5,6-8" → [(0,2), (3,5), (6,8)]
    Окна трактуются как [start, end), то есть 0-2 = часы 0 и 1.
    """
    ranges = []
    try:
        parts = txt.split(",")
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "-" not in part:
                continue
            a, b = part.split("-", 1)
            start = int(a.strip())
            end   = int(b.strip())
            # простая защита от бреда
            if 0 <= start <= 23 and 1 <= end <= 24 and start < end:
                ranges.append((start, end))
    except Exception:
        return []
    return ranges

def _hour_allowed(hour: int, ranges):
    """
    Возвращает True, если час попадает хотя бы в один диапазон.
    Если ranges пустой → ограничений по часам нет.
    """
    if not ranges:
        return True
    for start, end in ranges:
        if start <= hour < end:
            return True
    return False

OKX_LONG_DAYS_SET       = _parse_days(OKX_LONG_DAYS_ENV)
OKX_SHORT_DAYS_SET      = _parse_days(OKX_SHORT_DAYS_ENV)
OKX_LONG_HOUR_RANGES    = _parse_hour_ranges(OKX_LONG_HOURS_ENV)
OKX_SHORT_HOUR_RANGES   = _parse_hour_ranges(OKX_SHORT_HOURS_ENV)

# === Telegram (тот же бот, что у Bybit) ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
CHAT_ID        = os.getenv("CHAT_ID", "")

MD_ESCAPE = re.compile(r'([_*\[\]()~>#+\-=|{}.!])')

def md_escape(text: str) -> str:
    return MD_ESCAPE.sub(r'\\\1', text)

def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing.")
        return
    safe_text = md_escape(text)
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            params={"chat_id": CHAT_ID, "text": safe_text, "parse_mode": "MarkdownV2"},
            timeout=8,
        )
        if r.status_code != 200:
            print("❌ Telegram error:", r.text[:300])
    except Exception as e:
        print("❌ Telegram exception:", e)

# глобальный кулдаун, как у тебя в bybit-коде
trade_global_cooldown_until = 0

def set_okx_leverage(inst_id: str, leverage: float):
    try:
        payload = {
            "instId": inst_id,
            "lever": str(leverage),
            "mgnMode": "cross",  # у тебя cross
        }
        resp = okx_private_post("/api/v5/account/set-leverage", payload, timeout=10)
        print("🔧 set_leverage resp:", resp)
        return resp
    except Exception as e:
        print("⚠️ set_okx_leverage error:", e)
        return None

# =============== ВСПОМОГАТЕЛЬНОЕ ===============
def _okx_timestamp() -> str:
    # корректный вариант без DeprecationWarning
    now = datetime.now(timezone.utc)
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
_okx_pos_mode = None  # 'net' или 'long_short'

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
    """
    Определяем текущий режим позиций аккаунта:
    - 'net'       — односторонний
    - 'long_short' — двусторонний (long/short)
    """
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
def okx_has_position(inst_id: str) -> bool:
    j = okx_private_get(
        "/api/v5/account/positions",
        {"instType": "SWAP", "instId": inst_id}
    )
    for p in j.get("data", []):
        pos = float(p.get("pos", "0"))
        avail = float(p.get("availPos", "0"))
        if abs(pos) > 0 or abs(avail) > 0:
            return True
    return False

def okx_has_open_orders(inst_id: str) -> bool:
    j = okx_private_get(
        "/api/v5/trade/orders-pending",
        {"instType": "SWAP", "instId": inst_id}
    )
    return bool(j.get("data"))

def okx_has_algo_orders(inst_id: str) -> bool:
    j = okx_private_get(
        "/api/v5/trade/orders-algo-pending",
        {"instType": "SWAP", "instId": inst_id}
    )
    return bool(j.get("data"))

# === размещение сделки: Market entry + TP/SL как attachAlgoOrds ===
def okx_place_order_with_tp_sl(inst_id: str, side: str, entry: float, tp: float, sl: float, risk_usdt: float):
    """
    side: 'buy' / 'sell'
    """
    # считаем размер
    sz = calc_sz_from_risk_okx(entry, sl, risk_usdt, inst_id)
    if sz <= 0:
        msg = f"{inst_id}: sz <= 0, сделка пропущена (risk={risk_usdt}, entry={entry}, sl={sl})"
        print("⚠️", msg)
        try:
            send_telegram("⚠️ *OKX SIZE ERROR*\n" + msg)
        except Exception:
            pass
        return {"error": "bad_size"}

    print(f"\n🚀 OKX NEW TRADE {inst_id} {side} sz={sz}, entry≈{entry}, tp={tp}, sl={sl}")

    payload = {
        "instId": inst_id,
        "tdMode": "cross",
        "side": side,                 # buy / sell
        "ordType": "market",
        "sz": str(sz),
        "attachAlgoOrds": [
            {
                "tpTriggerPx": str(tp),
                "tpOrdPx": str(tp),
                "tpTriggerPxType": "last",

                "slTriggerPx": str(sl),
                "slOrdPx": str(sl),
                "slTriggerPxType": "last"
            }
        ]
    }

    # автоопределяем режим позиций
    pos_mode = get_okx_pos_mode()  # 'net' или 'long_short'

    # в long/short режиме нужен posSide = long/short
    if pos_mode == "long_short":
        payload["posSide"] = "long" if side == "buy" else "short"
    # в net-режиме posSide либо 'net', либо вообще не передаётся – safer не отправлять

    resp = okx_private_post("/api/v5/trade/order", payload)
    print("📨 OKX ORDER RESPONSE:", resp)

    # разбираем детальную ошибку
    code = str(resp.get("code", ""))
    data = resp.get("data") or []
    sCode = sMsg = ""
    if isinstance(data, list) and data:
        d0 = data[0]
        sCode = str(d0.get("sCode", ""))
        sMsg = str(d0.get("sMsg", ""))

    if code not in ("0", "00000"):
        msg = (
            "❌ *OKX ORDER FAILED*\n"
            f"{inst_id} {side.upper()}\n"
            f"code: {code}\n"
            f"msg: {resp.get('msg','')}"
        )
        if sCode or sMsg:
            msg += f"\n*sCode*: `{sCode}`\n*sMsg*: {sMsg}"
        try:
            send_telegram(msg)
        except Exception:
            pass

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

    # === TIME & DAY FILTERS (UTC+2 mode) ===
    tz_local = timezone(timedelta(hours=2))
    now_dt_utc2 = datetime.now(timezone.utc).astimezone(tz_local)

    wd   = now_dt_utc2.weekday()
    hour = now_dt_utc2.hour

    is_long = (direction == "UP")

    if is_long:
        days_set    = OKX_LONG_DAYS_SET
        hour_ranges = OKX_LONG_HOUR_RANGES
        side_label  = "LONG"
    else:
        days_set    = OKX_SHORT_DAYS_SET
        hour_ranges = OKX_SHORT_HOUR_RANGES
        side_label  = "SHORT"

    # фильтр дней
    if days_set and wd not in days_set:
        print(f"⛔ OKX {side_label} blocked by weekday (UTC+2): wd={wd}, allowed={sorted(list(days_set))}")
        return jsonify({"status": "blocked_day"}), 200

    # фильтр часовых диапазонов в UTC+2
    if not _hour_allowed(hour, hour_ranges):
        print(f"⛔ OKX {side_label} blocked by hour (UTC+2): hour={hour}, allowed={hour_ranges}")
        return jsonify({"status": "blocked_hour"}), 200

    if typ != "SCALP":
        return jsonify({"status": "ignored"}), 200

    now = time.time()
    if now < trade_global_cooldown_until:
        remaining = int(trade_global_cooldown_until - now)
        print(f"⛔ GLOBAL COOLDOWN {remaining}s, сигнал по {inst_id} блокирован")

        # уведомление о блокировке по кулдауну
        try:
            send_telegram(
                f"⛔ *OKX TRADE BLOCKED*\n"
                f"{inst_id} {direction}\n"
                f"Cooldown {remaining}s"
            )
        except Exception as e:
            print("⚠️ Telegram cooldown notify error:", e)

        return jsonify({"status": "cooldown"}), 200

    # Проверка открытой позиции (БЛОКИРУЕМ независимо от направления)
    try:
        if (
            okx_has_position(inst_id)
            or okx_has_open_orders(inst_id)
            or okx_has_algo_orders(inst_id)
        ):
            print(f"⛔ {inst_id}: позиция или ордера уже существуют, сигнал заблокирован")
    
            send_telegram(
                "⛔ *OKX TRADE BLOCKED*\n"
                f"{inst_id}\n"
                f"Direction: {direction}\n"
                f"Reason: POSITION OR ORDERS EXIST"
            )
    
            return jsonify({"status": "blocked_existing_state"}), 200
    except Exception as e:
        print(f"⚠️ Ошибка проверки состояния OKX: {e}")

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
    
    set_okx_leverage(inst_id, LEVERAGE)

    resp = okx_place_order_with_tp_sl(
        inst_id=inst_id,
        side=side,
        entry=entry_f,
        tp=tp,
        sl=sl,
        risk_usdt=MAX_RISK_USDT
    )

    # отправка уведомления о сделке в тот же Telegram, но с пометкой OKX
    try:
        msg = (
            "⚡ *OKX TRADE*\n"
            f"{inst_id} {side.upper()}\n"
            f"Entry~{entry_f}\n"
            f"TP: {tp}\n"
            f"SL: {sl}"
        )
        send_telegram(msg)
    except Exception as e:
        print("⚠️ Telegram trade notify error:", e)

    # глобальный кулдаун
    trade_global_cooldown_until = time.time() + 180
    print("🕒 GLOBAL COOLDOWN ACTIVATED (OKX) 180s")

    return jsonify({"status": "ok", "okx_resp": resp}), 200


@app.route("/")
def root():
    return "OKX AUTOTRADE OK", 200
    
@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    print("🚀 Starting OKX SCALP server")
    port = int(os.getenv("PORT", "8090"))
    app.run(host="0.0.0.0", port=port, use_reloader=False)



