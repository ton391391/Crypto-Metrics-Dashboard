#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto Metrics Dashboard  (single file Flask app)
=================================================

แดชบอร์ดกราฟ + ตารางเปรียบเทียบเหรียญ crypto ทั้งตลาด (USDT perpetual)
เมตริกที่แสดง:
    * Daily Return          - ผลตอบแทนรายวัน (เฉลี่ย / สะสม / รายวันเป็นกราฟ)
    * Standard Deviation    - ส่วนเบี่ยงเบนมาตรฐานของ daily return (+ annualized vol)
    * Open Interest         - มูลค่าสัญญาคงค้างฝั่ง futures
    * Implied Volatility    - DVOL ของ Deribit (ถ้ามี) มิฉะนั้นใช้ realized vol เป็น proxy

ผู้ใช้เลือกช่วงเวลาได้ (7 / 30 / 90 / 180 / 365 วัน) จัดเรียงและกรองตามทุกเมตริกได้

วิธีรัน:
    pip install flask requests
    python app.py
    เปิด http://127.0.0.1:5000

ตัวแปรแวดล้อมที่ปรับได้:
    PORT=5000            พอร์ตของเซิร์ฟเวอร์
    UNIVERSE_SIZE=80     จำนวนเหรียญ (เรียงตาม 24h quote volume)
    CACHE_TTL=300        อายุแคช (วินาที)
    DEMO=1               บังคับใช้ข้อมูลจำลอง (ใช้ตอนไม่มีเน็ต)
"""

from __future__ import annotations

import json
import math
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from concurrent.futures import TimeoutError as FutureTimeout
from datetime import datetime, timezone

import requests
from flask import Flask, Response, jsonify, request

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

BINANCE_FAPI = os.environ.get("BINANCE_FAPI", "https://fapi.binance.com")
DERIBIT_API = os.environ.get("DERIBIT_API", "https://www.deribit.com/api/v2")

UNIVERSE_SIZE = int(os.environ.get("UNIVERSE_SIZE", "80"))
CACHE_TTL = int(os.environ.get("CACHE_TTL", "300"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "10"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "12"))
# เพดานเวลาของ /data/coins หนึ่งครั้ง ครบแล้วตอบเท่าที่ได้ทันที
# ส่วนที่ยังไม่เสร็จทำงานต่อเบื้องหลัง แล้วหน้าเว็บจะขอซ้ำเพื่อเก็บส่วนที่เหลือ
# ค่าต่ำ ๆ สำคัญมาก เพราะถ้าปล่อยให้เบราว์เซอร์รอนานจะโดนตัดทิ้ง (อาการ HTTP 499)
REQUEST_DEADLINE = float(os.environ.get("REQUEST_DEADLINE", "8"))
FORCE_DEMO = os.environ.get("DEMO", "0") == "1"

ALLOWED_DAYS = (7, 30, 90, 180, 365)
DEFAULT_DAYS = 90

# Binance เก็บประวัติ open interest แบบราย 1 วันไว้ย้อนหลังได้ ~30 วันเท่านั้น
OI_MAX_DAYS = 30
# จำนวนวันที่ใช้คำนวณ realized volatility แบบ rolling (ใช้เป็น proxy ของ IV)
RV_WINDOW = 30
# ปีละ 365 วันเทรด (คริปโตเทรด 24/7)
YEAR_DAYS = 365
# สกุลเงินที่จะลองขอ volatility index (DVOL) จาก Deribit
# ปัจจุบัน Deribit เผยแพร่ DVOL จริงเฉพาะ BTC และ ETH ส่วนที่เหลือจะได้ข้อมูลว่าง
# แล้วระบบจะ fallback ไปใช้ realized volatility เป็น proxy ให้อัตโนมัติ
DVOL_CURRENCIES = ("BTC", "ETH", "SOL", "XRP")

STATE = {"source": "unknown", "last_error": None, "checked_at": None}
_state_lock = threading.Lock()


# คอนโซล Windows ปกติเป็น cp874/cp1252 ซึ่ง print ภาษาไทยแล้วจะ UnicodeEncodeError
# บังคับ stdout/stderr เป็น UTF-8 ตั้งแต่ต้น และยังกัน error ซ้ำอีกชั้นใน log()
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001 - บาง environment ไม่มี reconfigure
        pass


def log(message: str) -> None:
    try:
        print("[dashboard] " + message, flush=True)
    except Exception:  # noqa: BLE001 - log ต้องไม่ทำให้รีเควสต์พัง
        try:
            print("[dashboard] " + message.encode("ascii", "replace").decode(), flush=True)
        except Exception:
            pass


def set_state(source: str, error: str | None = None) -> None:
    with _state_lock:
        STATE["source"] = source
        STATE["last_error"] = error
        STATE["checked_at"] = time.time()


# --------------------------------------------------------------------------- #
# Cache + HTTP
# --------------------------------------------------------------------------- #


class TTLCache:
    """แคชในหน่วยความจำแบบง่าย ๆ พร้อมอายุหมดอายุรายคีย์"""

    def __init__(self) -> None:
        self._data: dict = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return None
            expires, value = item
            if expires < time.time():
                self._data.pop(key, None)
                return None
            return value

    def set(self, key, value, ttl: int = CACHE_TTL):
        with self._lock:
            self._data[key] = (time.time() + ttl, value)
        return value

    def clear(self):
        with self._lock:
            self._data.clear()


CACHE = TTLCache()

_session = requests.Session()
_session.headers.update({"User-Agent": "crypto-metrics-dashboard/1.0"})


class DataUnavailable(Exception):
    """ยิง API ไม่สำเร็จ (เน็ตล่ม / โดน rate limit / ภูมิภาคถูกบล็อก)"""


def http_json(url: str, params: dict | None = None, tries: int = 2):
    last_error = None
    for attempt in range(tries):
        try:
            resp = _session.get(url, params=params, timeout=(5, HTTP_TIMEOUT))
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = RuntimeError("HTTP %s" % resp.status_code)
                time.sleep(0.6 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - รวบทุก transport error เป็นชนิดเดียว
            last_error = exc
            time.sleep(0.4 * (attempt + 1))
    raise DataUnavailable(str(last_error))


# --------------------------------------------------------------------------- #
# Stat helpers
# --------------------------------------------------------------------------- #


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def stdev(xs):
    """ส่วนเบี่ยงเบนมาตรฐานแบบ sample (n-1)"""
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def annualize(daily_std_pct: float) -> float:
    return daily_std_pct * math.sqrt(YEAR_DAYS)


def pct_returns(closes):
    out = []
    for prev, cur in zip(closes, closes[1:]):
        out.append((cur / prev - 1.0) * 100.0 if prev else 0.0)
    return out


def max_drawdown(closes) -> float:
    peak, worst = None, 0.0
    for c in closes:
        peak = c if peak is None else max(peak, c)
        if peak:
            worst = min(worst, (c / peak - 1.0) * 100.0)
    return worst


def rolling_stdev(returns, window: int):
    """คืนลิสต์ความยาวเท่า returns; ช่องที่ข้อมูลไม่พอจะเป็น None"""
    out = []
    for i in range(len(returns)):
        if i + 1 < min(window, 5):
            out.append(None)
            continue
        chunk = returns[max(0, i + 1 - window):i + 1]
        out.append(round(stdev(chunk), 4))
    return out


def iso_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- #
# Live data sources : Binance USDT-M futures + Deribit DVOL
# --------------------------------------------------------------------------- #


def fetch_universe_live():
    """รายชื่อ perpetual USDT ทั้งหมด เรียงตามมูลค่าซื้อขาย 24 ชม."""
    info = http_json(BINANCE_FAPI + "/fapi/v1/exchangeInfo")
    perps = {
        s["symbol"]
        for s in info.get("symbols", [])
        if s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("status") == "TRADING"
    }
    tickers = http_json(BINANCE_FAPI + "/fapi/v1/ticker/24hr")
    rows = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in perps:
            continue
        try:
            rows.append(
                {
                    "symbol": sym,
                    "base": sym[:-4],
                    "last_price": float(t["lastPrice"]),
                    "change_24h": float(t["priceChangePercent"]),
                    "quote_volume": float(t["quoteVolume"]),
                }
            )
        except (KeyError, ValueError):
            continue
    rows.sort(key=lambda r: -r["quote_volume"])
    return rows[:UNIVERSE_SIZE]


def fetch_klines(symbol: str, days: int):
    """แท่งเทียนราย 1 วัน (ตัดแท่งของวันปัจจุบันที่ยังไม่ปิดออก)"""
    limit = min(days + 2, 1000)
    raw = http_json(
        BINANCE_FAPI + "/fapi/v1/klines",
        {"symbol": symbol, "interval": "1d", "limit": limit},
    )
    now_ms = time.time() * 1000
    candles = []
    for k in raw:
        if float(k[6]) > now_ms:  # close time ยังไม่ถึง = แท่งยังไม่ปิด
            continue
        candles.append(
            {"date": iso_date(int(k[0])), "close": float(k[4]), "volume": float(k[7])}
        )
    return candles[-(days + 1):]


def fetch_oi_hist(symbol: str):
    """ประวัติ open interest ราย 1 วัน (Binance เก็บย้อนหลังได้ราว 30 วัน)"""
    raw = http_json(
        BINANCE_FAPI + "/futures/data/openInterestHist",
        {"symbol": symbol, "period": "1d", "limit": OI_MAX_DAYS},
    )
    out = {}
    for row in raw or []:
        try:
            out[iso_date(int(row["timestamp"]))] = float(row["sumOpenInterestValue"])
        except (KeyError, ValueError, TypeError):
            continue
    return out


def fetch_dvol(currency: str, days: int):
    """DVOL ของ Deribit = implied volatility index (หน่วยเป็น % ต่อปี)"""
    key = ("dvol", currency, days)
    cached = CACHE.get(key)
    if cached is not None:
        return cached or None
    end = int(time.time() * 1000)
    start = end - (days + 2) * 86400 * 1000
    try:
        payload = http_json(
            DERIBIT_API + "/public/get_volatility_index_data",
            {
                "currency": currency,
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": 43200,
            },
        )
        data = (payload.get("result") or {}).get("data") or []
    except DataUnavailable:
        CACHE.set(key, {}, 900)
        return None
    daily = {}
    for point in data:
        try:
            daily[iso_date(int(point[0]))] = float(point[4])
        except (IndexError, ValueError, TypeError):
            continue
    CACHE.set(key, daily, CACHE_TTL)
    return daily or None


# --------------------------------------------------------------------------- #
# Demo data (ใช้เมื่อยิง API จริงไม่ได้ เช่น ไม่มีเน็ต หรือถูกบล็อกตามภูมิภาค)
# --------------------------------------------------------------------------- #

DEMO_COINS = [
    ("BTC", 68000.0, 0.028, 12.0e9), ("ETH", 3550.0, 0.036, 6.5e9),
    ("SOL", 172.0, 0.052, 2.1e9), ("XRP", 0.62, 0.048, 1.4e9),
    ("BNB", 585.0, 0.031, 900e6), ("DOGE", 0.155, 0.062, 780e6),
    ("ADA", 0.46, 0.047, 420e6), ("AVAX", 33.5, 0.055, 380e6),
    ("LINK", 16.9, 0.050, 350e6), ("TON", 7.4, 0.049, 300e6),
    ("TRX", 0.128, 0.030, 260e6), ("DOT", 6.3, 0.048, 240e6),
    ("MATIC", 0.71, 0.053, 230e6), ("NEAR", 6.1, 0.058, 220e6),
    ("LTC", 84.0, 0.038, 210e6), ("ICP", 11.2, 0.061, 190e6),
    ("APT", 8.9, 0.059, 180e6), ("ARB", 0.98, 0.063, 175e6),
    ("OP", 2.35, 0.062, 170e6), ("SUI", 1.42, 0.070, 165e6),
    ("SEI", 0.48, 0.072, 150e6), ("INJ", 26.5, 0.066, 145e6),
    ("FIL", 5.2, 0.057, 140e6), ("ATOM", 8.1, 0.050, 130e6),
    ("TIA", 8.8, 0.074, 125e6), ("PEPE", 0.0000112, 0.095, 120e6),
    ("WIF", 2.35, 0.098, 115e6), ("BONK", 0.0000255, 0.101, 110e6),
    ("UNI", 8.4, 0.052, 105e6), ("AAVE", 92.0, 0.055, 100e6),
    ("ETC", 26.4, 0.045, 95e6), ("BCH", 415.0, 0.043, 92e6),
    ("STX", 1.85, 0.068, 88e6), ("RUNE", 4.4, 0.071, 84e6),
    ("FTM", 0.63, 0.069, 80e6), ("ORDI", 38.0, 0.086, 76e6),
    ("GALA", 0.027, 0.075, 70e6), ("SAND", 0.31, 0.064, 66e6),
    ("AXS", 6.2, 0.067, 62e6), ("LDO", 2.05, 0.065, 58e6),
]


def demo_universe():
    rows = []
    for base, price, vol, qv in DEMO_COINS:
        rng = random.Random("uni-" + base)
        rows.append(
            {
                "symbol": base + "USDT",
                "base": base,
                "last_price": price,
                "change_24h": round(rng.uniform(-1, 1) * vol * 100, 2),
                "quote_volume": qv,
            }
        )
    rows.sort(key=lambda r: -r["quote_volume"])
    return rows[:UNIVERSE_SIZE]


def demo_series(symbol: str, base: str, days: int):
    """สร้าง random walk แบบคงที่ต่อเหรียญ (seed จากชื่อเหรียญ)"""
    spec = next((c for c in DEMO_COINS if c[0] == base), None)
    price, dvol, qv = (spec[1], spec[2], spec[3]) if spec else (10.0, 0.05, 5e7)
    rng = random.Random(symbol)
    n = days + 1
    drift = rng.uniform(-0.0012, 0.0022)

    closes, p = [], price
    for _ in range(n):
        p *= 1 + rng.gauss(drift, dvol)
        closes.append(max(p, 1e-12))
    scale = price / closes[-1]
    closes = [c * scale for c in closes]

    today = int(time.time() // 86400) * 86400 * 1000
    dates = [iso_date(today - (n - 1 - i) * 86400 * 1000) for i in range(n)]

    oi_base = qv * rng.uniform(0.25, 0.9)
    oi_map, level = {}, oi_base
    for d in dates[-OI_MAX_DAYS:]:
        level *= 1 + rng.gauss(0.002, 0.05)
        oi_map[d] = max(level, oi_base * 0.2)

    iv_map = None
    if base in ("BTC", "ETH"):  # เลียนแบบความจริง: Deribit มี DVOL แค่สองสกุลนี้
        iv_map = {}
        iv_level = dvol * math.sqrt(YEAR_DAYS) * 100 * 1.15
        for d in dates:
            iv_level = max(8.0, iv_level * (1 + rng.gauss(0, 0.045)))
            iv_map[d] = iv_level
    return dates, closes, oi_map, iv_map, "dvol" if iv_map else "realized"


# --------------------------------------------------------------------------- #
# Series assembly + summary metrics
# --------------------------------------------------------------------------- #


def build_series(symbol: str, base: str, days: int, demo: bool):
    """รวมราคา / daily return / SD / OI / IV เป็นชุดข้อมูลเดียว (มีแคช)"""
    key = ("series", symbol, days, demo)
    cached = CACHE.get(key)
    if cached is not None:
        return cached

    if demo:
        dates, closes, oi_map, iv_map, iv_source = demo_series(symbol, base, days)
    else:
        candles = fetch_klines(symbol, days)
        if len(candles) < 3:
            raise DataUnavailable("ข้อมูลราคาไม่พอสำหรับ " + symbol)
        dates = [c["date"] for c in candles]
        closes = [c["close"] for c in candles]
        try:
            oi_map = fetch_oi_hist(symbol)
        except DataUnavailable:
            oi_map = {}
        iv_map = fetch_dvol(base, days) if base in DVOL_CURRENCIES else None
        iv_source = "dvol" if iv_map else "realized"

    returns = pct_returns(closes)
    ret_series = [None] + [round(r, 4) for r in returns]
    roll_sd = [None] + rolling_stdev(returns, 14)
    rv_roll = [None] + [
        (round(annualize(s), 3) if s is not None else None)
        for s in rolling_stdev(returns, RV_WINDOW)
    ]

    if iv_map:
        iv_series = [round(iv_map[d], 3) if d in iv_map else None for d in dates]
        if not any(v is not None for v in iv_series):
            iv_series, iv_source = list(rv_roll), "realized"
    else:
        iv_series = list(rv_roll)

    oi_series = [round(oi_map[d], 2) if d in oi_map else None for d in dates]

    series = {
        "symbol": symbol,
        "base": base,
        "dates": dates,
        "close": closes,
        "daily_return": ret_series,
        "rolling_sd": roll_sd,
        "realized_vol": rv_roll,
        "open_interest": oi_series,
        "implied_vol": iv_series,
        "iv_source": iv_source,
    }
    return CACHE.set(key, series)


def summarize(series: dict, ticker: dict) -> dict:
    closes = series["close"]
    returns = [r for r in series["daily_return"] if r is not None]
    sd = stdev(returns)
    avg = mean(returns)
    ann_vol = annualize(sd)

    oi_points = [v for v in series["open_interest"] if v]
    oi_latest = oi_points[-1] if oi_points else None
    oi_change = None
    if len(oi_points) >= 2 and oi_points[0]:
        oi_change = (oi_points[-1] / oi_points[0] - 1.0) * 100.0

    iv_points = [v for v in series["implied_vol"] if v is not None]
    iv_latest = iv_points[-1] if iv_points else None

    def r(x, nd=4):
        return None if x is None else round(x, nd)

    total_return = (closes[-1] / closes[0] - 1.0) * 100.0 if closes[0] else 0.0

    return {
        "symbol": series["symbol"],
        "base": series["base"],
        "last_price": ticker.get("last_price"),
        "change_24h": r(ticker.get("change_24h"), 2),
        "quote_volume": ticker.get("quote_volume"),
        "days_used": len(returns),
        "total_return": r(total_return, 2),
        "avg_daily_return": r(avg, 4),
        "best_day": r(max(returns), 2) if returns else None,
        "worst_day": r(min(returns), 2) if returns else None,
        "win_rate": r(100.0 * sum(1 for x in returns if x > 0) / len(returns), 1) if returns else None,
        "std_daily": r(sd, 4),
        "ann_vol": r(ann_vol, 2),
        "sharpe": r(avg / sd * math.sqrt(YEAR_DAYS), 2) if sd else None,
        "max_drawdown": r(max_drawdown(closes), 2),
        "oi_usd": r(oi_latest, 2),
        "oi_change": r(oi_change, 2),
        "oi_days": len(oi_points),
        "iv": r(iv_latest, 2),
        "iv_avg": r(mean(iv_points), 2) if iv_points else None,
        "iv_premium": r(iv_latest - ann_vol, 2) if iv_latest is not None else None,
        "iv_source": series["iv_source"],
    }


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


_universe_future = None
_universe_lock = threading.Lock()


def _load_universe():
    try:
        started = time.time()
        rows = fetch_universe_live()
        if not rows:
            raise DataUnavailable("ไม่พบสัญญา perpetual")
        set_state("live")
        log("universe: %d เหรียญ ใน %.1f วินาที" % (len(rows), time.time() - started))
        return CACHE.set("universe", (rows, False))
    except DataUnavailable as exc:
        set_state("demo", str(exc))
        log("ดึงรายชื่อเหรียญจาก Binance ไม่สำเร็จ (%s) - สลับไปใช้ข้อมูลจำลอง" % exc)
        return CACHE.set("universe", (demo_universe(), True), 60)


def get_universe(force_refresh: bool = False, timeout: float | None = None):
    """คืน (rows, is_demo) - ถ้ายิง Binance ไม่ได้จะสลับไปใช้ข้อมูลจำลองอัตโนมัติ

    การดึงรายชื่อเหรียญ (exchangeInfo + ticker24hr) เป็นก้อนใหญ่และช้าที่สุดของรอบแรก
    จึงทำในเธรดเบื้องหลัง ถ้ารอเกิน timeout จะคืน None เพื่อให้ HTTP ตอบกลับไปก่อน
    แล้วให้หน้าเว็บถามซ้ำ แทนที่จะค้างจนเบราว์เซอร์ตัดการเชื่อมต่อ
    """
    global _universe_future
    if force_refresh:
        CACHE.clear()
        with _universe_lock:
            _universe_future = None
    if FORCE_DEMO:
        set_state("demo", "DEMO=1")
        return demo_universe(), True

    cached = CACHE.get("universe")
    if cached is not None:
        return cached

    with _universe_lock:
        if _universe_future is None or _universe_future.done():
            cached = CACHE.get("universe")
            if cached is not None:
                return cached
            log("กำลังดึงรายชื่อเหรียญจาก Binance...")
            _universe_future = POOL.submit(_load_universe)
        future = _universe_future

    if timeout is None:
        return future.result()
    try:
        return future.result(timeout=timeout)
    except FutureTimeout:
        return None


# เธรดพูลตัวเดียวใช้ตลอดอายุโปรเซส งานที่ยังดึงไม่เสร็จเมื่อครบ deadline
# จะ "ไม่ถูกยกเลิก" แต่ทำงานต่อเบื้องหลังและเก็บผลลงแคช รอบถัดไปจึงได้ข้อมูลเพิ่มทันที
POOL = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="fetch")
_inflight: dict = {}
_inflight_lock = threading.Lock()


def _work_one(ticker: dict, days: int, demo: bool):
    try:
        series = build_series(ticker["symbol"], ticker["base"], days, demo)
        return summarize(series, ticker)
    except DataUnavailable:
        return None
    except Exception:  # noqa: BLE001 - เหรียญเดียวพังต้องไม่ทำให้ทั้งหน้าพัง
        return None


def _submit(ticker: dict, days: int, demo: bool):
    """ส่งงานเข้าคิว ถ้าเหรียญนี้กำลังดึงอยู่แล้วให้ใช้ future เดิม ไม่ยิงซ้ำ"""
    key = (ticker["symbol"], days, demo)
    with _inflight_lock:
        future = _inflight.get(key)
        if future is None or future.done():
            future = POOL.submit(_work_one, ticker, days, demo)
            _inflight[key] = future
        return future


def collect_rows(days: int, force_refresh: bool = False):
    started = time.time()
    pack = get_universe(force_refresh, timeout=REQUEST_DEADLINE)
    if pack is None:  # ยังดึงรายชื่อเหรียญไม่เสร็จ - ตอบไปก่อนแล้วให้ถามซ้ำ
        return {
            "rows": [], "demo": False, "elapsed": time.time() - started,
            "universe": 0, "failed": 0, "pending": 0,
            "partial": True, "warming": True,
        }
    universe, demo = pack
    futures = [_submit(t, days, demo) for t in universe]
    # เวลาที่เหลือจริงหลังหักส่วนที่ใช้รอรายชื่อเหรียญไป เพื่อให้ทั้งรีเควสต์
    # ไม่เกิน REQUEST_DEADLINE ไม่ว่าจะช้าตรงไหน
    left = max(0.5, started + REQUEST_DEADLINE - time.time())
    done, pending = wait(futures, timeout=left)

    rows, failed = [], 0
    for future in done:
        result = future.result()
        if result:
            rows.append(result)
        else:
            failed += 1

    return {
        "rows": rows,
        "demo": demo,
        "elapsed": time.time() - started,
        "universe": len(universe),
        "failed": failed,
        "pending": len(pending),
        "partial": bool(pending),
        "warming": False,
    }


NUMERIC_FIELDS = {
    "total_return", "avg_daily_return", "std_daily", "ann_vol", "sharpe",
    "oi_usd", "oi_change", "iv", "iv_avg", "iv_premium", "max_drawdown",
    "win_rate", "quote_volume", "change_24h", "last_price", "best_day",
    "worst_day", "days_used",
}


def clamp_days(raw) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_DAYS
    return value if value in ALLOWED_DAYS else DEFAULT_DAYS


# --------------------------------------------------------------------------- #
# Flask app
# --------------------------------------------------------------------------- #

app = Flask(__name__)


@app.get("/")
def index():
    page = HTML_PAGE.replace("__PERIODS__", json.dumps(list(ALLOWED_DAYS)))
    page = page.replace("__DEFAULT_DAYS__", str(DEFAULT_DAYS))
    return Response(page, mimetype="text/html; charset=utf-8")


@app.get("/data/coins")
@app.get("/api/metrics")  # alias เดิม (บาง ad blocker บล็อก path นี้)
def api_metrics():
    """สรุปเมตริกของทุกเหรียญ  ?days=&sort=&order=&min_<field>=&max_<field>=&q="""

    days = clamp_days(request.args.get("days"))
    refresh = request.args.get("refresh") == "1"
    result = collect_rows(days, refresh)
    rows, demo = result["rows"], result["demo"]
    log(
        "metrics %d วัน: พร้อม %d/%d เหรียญ (ข้าม %d, ค้าง %d) ใน %.1f วินาที"
        % (days, len(rows), result["universe"], result["failed"],
           result["pending"], result["elapsed"])
    )

    query = (request.args.get("q") or "").strip().upper()
    if query:
        rows = [r for r in rows if query in r["symbol"]]

    for field in NUMERIC_FIELDS:
        for prefix, keep in (("min_", True), ("max_", False)):
            raw = request.args.get(prefix + field)
            if raw in (None, ""):
                continue
            try:
                bound = float(raw)
            except ValueError:
                continue
            rows = [
                r for r in rows
                if r.get(field) is not None
                and (r[field] >= bound if keep else r[field] <= bound)
            ]

    sort_key = request.args.get("sort", "total_return")
    if sort_key not in NUMERIC_FIELDS:
        sort_key = "total_return"
    reverse = request.args.get("order", "desc") != "asc"
    rows.sort(
        key=lambda r: (r.get(sort_key) is None, r.get(sort_key) or 0.0),
        reverse=reverse,
    )
    if reverse:  # ดันค่าที่เป็น null ไปท้ายตารางเสมอ
        rows.sort(key=lambda r: r.get(sort_key) is None)

    return jsonify(
        {
            "meta": {
                "days": days,
                "source": "demo" if demo else "live",
                "source_error": STATE["last_error"] if demo else None,
                "universe": result["universe"],
                "returned": len(rows),
                "loaded": len(result["rows"]),
                "failed": result["failed"],
                "pending": result["pending"],
                "partial": result["partial"],
                "warming": result["warming"],
                "elapsed_sec": round(result["elapsed"], 2),
                "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "oi_history_days": OI_MAX_DAYS,
                "rv_window": RV_WINDOW,
                "dvol_currencies": list(DVOL_CURRENCIES),
                "sort": sort_key,
                "order": "desc" if reverse else "asc",
            },
            "rows": rows,
        }
    )


@app.get("/data/coin/<symbol>")
@app.get("/api/coin/<symbol>")  # alias เดิม
def api_coin(symbol: str):
    """ชุดข้อมูลรายวันของเหรียญเดียว สำหรับวาดกราฟ"""
    days = clamp_days(request.args.get("days"))
    symbol = symbol.upper()
    pack = get_universe(timeout=REQUEST_DEADLINE)
    if pack is None:
        return jsonify({"error": "กำลังเตรียมรายชื่อเหรียญ กรุณาลองใหม่อีกครั้ง"}), 503
    universe, demo = pack
    ticker = next((t for t in universe if t["symbol"] == symbol), None)
    if ticker is None:
        return jsonify({"error": "ไม่พบเหรียญ " + symbol}), 404
    try:
        series = build_series(symbol, ticker["base"], days, demo)
    except DataUnavailable as exc:
        return jsonify({"error": str(exc)}), 502
    return jsonify(
        {
            "meta": {"days": days, "source": "demo" if demo else "live"},
            "summary": summarize(series, ticker),
            "series": series,
        }
    )


@app.get("/data/health")
@app.get("/api/health")  # alias เดิม
def api_health():
    return jsonify(
        {
            "status": "ok",
            "source": STATE["source"],
            "last_error": STATE["last_error"],
            "cache_ttl": CACHE_TTL,
            "universe_size": UNIVERSE_SIZE,
        }
    )


# --------------------------------------------------------------------------- #
# Front-end (Tailwind CDN + Chart.js)
# --------------------------------------------------------------------------- #

HTML_PAGE = r"""<!doctype html>
<html lang="th" class="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crypto Metrics Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.5.1/chart.umd.min.js"></script>
<script>
  /* สำรอง CDN เผื่อ cdnjs โหลดไม่ได้ */
  if (typeof Chart === 'undefined') {
    document.write('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.1/dist/chart.umd.min.js"><\/script>');
  }
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Thai:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root { color-scheme: dark; }
  body { font-family: 'IBM Plex Sans Thai', system-ui, -apple-system, 'Segoe UI', sans-serif; }
  .mono { font-family: 'JetBrains Mono', ui-monospace, Consolas, monospace; }
  ::-webkit-scrollbar { width: 10px; height: 10px; }
  ::-webkit-scrollbar-track { background: #0b1120; }
  ::-webkit-scrollbar-thumb { background: #27354d; border-radius: 6px; }
  ::-webkit-scrollbar-thumb:hover { background: #37476a; }
  .sticky-head th { position: sticky; top: 0; z-index: 10; background: #0f172a; box-shadow: inset 0 -1px 0 #1e293b; }
  tr.coin-row { cursor: pointer; }
</style>
</head>
<body class="bg-slate-950 text-slate-200 antialiased">

<div class="max-w-[1600px] mx-auto px-4 py-6 space-y-5">

  <!-- Header -->
  <header class="flex flex-wrap items-center justify-between gap-4">
    <div>
      <h1 class="text-2xl md:text-3xl font-bold text-white tracking-tight">
        Crypto Metrics Dashboard
      </h1>
      <p class="text-sm text-slate-400 mt-1">
        Daily return &middot; Standard deviation &middot; Open Interest &middot; Implied Volatility
      </p>
    </div>
    <div class="flex items-center gap-3">
      <span id="sourceBadge" class="text-xs px-3 py-1.5 rounded-full border border-slate-700 bg-slate-900 text-slate-400">
        กำลังเชื่อมต่อ...
      </span>
      <button id="refreshBtn"
        class="text-sm px-4 py-2 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-200 transition">
        รีเฟรชข้อมูล
      </button>
    </div>
  </header>

  <!-- Controls -->
  <section class="bg-slate-900/60 border border-slate-800 rounded-2xl p-4 md:p-5 space-y-4">

    <div class="flex flex-wrap items-center gap-x-6 gap-y-3">
      <div class="flex items-center gap-2">
        <span class="text-xs uppercase tracking-wider text-slate-500 mr-1">ช่วงเวลา</span>
        <div id="periodBtns" class="flex rounded-lg overflow-hidden border border-slate-700"></div>
      </div>

      <div class="flex items-center gap-2">
        <span class="text-xs uppercase tracking-wider text-slate-500">ค้นหา</span>
        <input id="searchInput" type="text" placeholder="BTC, ETH, SOL..."
          class="w-40 bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm
                 placeholder-slate-600 focus:outline-none focus:border-sky-500">
      </div>

      <div class="flex items-center gap-2">
        <span class="text-xs uppercase tracking-wider text-slate-500">เรียงตาม</span>
        <select id="sortSelect"
          class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-sky-500"></select>
        <button id="orderBtn"
          class="px-3 py-1.5 text-sm rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">
          มาก &rarr; น้อย
        </button>
      </div>

      <div class="flex items-center gap-2">
        <span class="text-xs uppercase tracking-wider text-slate-500">แสดง</span>
        <select id="limitSelect"
          class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-sky-500">
          <option value="20">20 อันดับแรก</option>
          <option value="50" selected>50 อันดับแรก</option>
          <option value="0">ทั้งหมด</option>
        </select>
      </div>

      <label class="flex items-center gap-2 text-sm text-slate-300 cursor-pointer select-none">
        <input id="fullOnly" type="checkbox"
          class="w-4 h-4 rounded bg-slate-950 border-slate-600 text-sky-500 focus:ring-0">
        เฉพาะเหรียญที่มีข้อมูลครบช่วง
      </label>
    </div>

    <!-- Filter builder -->
    <div class="flex flex-wrap items-end gap-3 pt-3 border-t border-slate-800">
      <div class="flex items-end gap-2">
        <div>
          <label class="block text-[11px] uppercase tracking-wider text-slate-500 mb-1">กรองด้วยค่า</label>
          <select id="filterField"
            class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-sky-500"></select>
        </div>
        <select id="filterOp"
          class="bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:border-sky-500">
          <option value="gte">&ge; มากกว่าหรือเท่ากับ</option>
          <option value="lte">&le; น้อยกว่าหรือเท่ากับ</option>
        </select>
        <input id="filterValue" type="number" step="any" placeholder="0"
          class="w-28 bg-slate-950 border border-slate-700 rounded-lg px-3 py-1.5 text-sm
                 placeholder-slate-600 focus:outline-none focus:border-sky-500">
        <button id="addFilterBtn"
          class="px-3 py-1.5 text-sm rounded-lg bg-sky-600 hover:bg-sky-500 text-white transition">
          เพิ่มเงื่อนไข
        </button>
      </div>

      <div class="flex flex-wrap items-center gap-2 ml-auto">
        <span class="text-[11px] uppercase tracking-wider text-slate-500">ชุดกรองสำเร็จรูป</span>
        <button data-preset="return" class="preset px-3 py-1.5 text-xs rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">Daily return สูงสุด</button>
        <button data-preset="stable" class="preset px-3 py-1.5 text-xs rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">ผันผวนต่ำ + บวก</button>
        <button data-preset="oi" class="preset px-3 py-1.5 text-xs rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">OI สูงสุด</button>
        <button data-preset="iv" class="preset px-3 py-1.5 text-xs rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">IV สูงสุด</button>
        <button data-preset="clear" class="preset px-3 py-1.5 text-xs rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">ล้างทั้งหมด</button>
      </div>
    </div>

    <div id="chips" class="flex flex-wrap gap-2"></div>
  </section>

  <!-- Market summary tiles -->
  <section id="tiles" class="grid grid-cols-2 lg:grid-cols-5 gap-3"></section>

  <!-- Table -->
  <section class="bg-slate-900/60 border border-slate-800 rounded-2xl overflow-hidden">
    <div class="flex items-center justify-between px-4 py-3 border-b border-slate-800">
      <h2 class="text-sm font-semibold text-slate-300">ตารางเปรียบเทียบเหรียญ</h2>
      <span id="tableMeta" class="text-xs text-slate-500"></span>
    </div>
    <div class="overflow-auto max-h-[65vh]">
      <table class="w-full text-sm">
        <thead class="sticky-head bg-slate-900 text-slate-400 text-xs uppercase tracking-wider">
          <tr id="headRow"></tr>
        </thead>
        <tbody id="tableBody" class="divide-y divide-slate-800/70"></tbody>
      </table>
    </div>
    <div id="tableEmpty" class="hidden px-4 py-10 text-center text-slate-500 text-sm">
      ไม่มีเหรียญที่ตรงกับเงื่อนไข
    </div>
  </section>

  <!-- Detail -->
  <section id="detail" class="hidden bg-slate-900/60 border border-slate-800 rounded-2xl p-4 md:p-5 space-y-4">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h2 id="detailTitle" class="text-xl font-bold text-white"></h2>
        <p id="detailSub" class="text-xs text-slate-400 mt-0.5"></p>
      </div>
      <button id="closeDetail"
        class="text-sm px-3 py-1.5 rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700 transition">
        ปิด
      </button>
    </div>
    <div id="detailStats" class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3"></div>
    <div class="grid grid-cols-1 xl:grid-cols-2 gap-4">
      <div class="bg-slate-950/60 border border-slate-800 rounded-xl p-3">
        <h3 class="text-xs uppercase tracking-wider text-slate-500 mb-2">ราคาปิดรายวัน</h3>
        <div class="h-64"><canvas id="chartPrice"></canvas></div>
      </div>
      <div class="bg-slate-950/60 border border-slate-800 rounded-xl p-3">
        <h3 class="text-xs uppercase tracking-wider text-slate-500 mb-2">Daily return (%) และ SD เคลื่อนที่ 14 วัน</h3>
        <div class="h-64"><canvas id="chartReturn"></canvas></div>
      </div>
      <div class="bg-slate-950/60 border border-slate-800 rounded-xl p-3">
        <h3 class="text-xs uppercase tracking-wider text-slate-500 mb-2">Open Interest (USD)</h3>
        <div class="h-64"><canvas id="chartOI"></canvas></div>
      </div>
      <div class="bg-slate-950/60 border border-slate-800 rounded-xl p-3">
        <h3 class="text-xs uppercase tracking-wider text-slate-500 mb-2">Implied volatility เทียบ realized volatility (% ต่อปี)</h3>
        <div class="h-64"><canvas id="chartIV"></canvas></div>
      </div>
    </div>
  </section>

  <!-- Overlay -->
  <div id="loading" class="hidden fixed inset-0 bg-slate-950/70 backdrop-blur-sm z-50 flex items-center justify-center">
    <div class="bg-slate-900 border border-slate-700 rounded-2xl px-8 py-6 text-center space-y-3">
      <div class="w-8 h-8 border-2 border-sky-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
      <p id="loadingText" class="text-sm text-slate-300">กำลังโหลดข้อมูลตลาด...</p>
    </div>
  </div>

  <div id="errorBox" class="hidden bg-rose-950/50 border border-rose-800 text-rose-200 rounded-xl px-4 py-3 text-sm"></div>

  <footer class="text-xs text-slate-500 leading-relaxed pb-6 space-y-1">
    <p id="footNote"></p>
    <p>
      แหล่งข้อมูล: Binance USDT-M Futures (ราคา / open interest) และ Deribit DVOL (implied volatility)
      &middot; หน้านี้ใช้เพื่อการศึกษาข้อมูลตลาด ไม่ใช่คำแนะนำการลงทุน
    </p>
  </footer>
</div>

<script>
const PERIODS = __PERIODS__;
const PERIOD_LABEL = { 7: '7 วัน', 30: '30 วัน', 90: '90 วัน', 180: '180 วัน', 365: '1 ปี' };

const COLUMNS = [
  { key: 'symbol',           label: 'เหรียญ',            type: 'text',  align: 'left' },
  { key: 'last_price',       label: 'ราคา',              type: 'price' },
  { key: 'change_24h',       label: '24 ชม. %',          type: 'signed' },
  { key: 'total_return',     label: 'ผลตอบแทนสะสม %',    type: 'signed' },
  { key: 'avg_daily_return', label: 'Daily return เฉลี่ย %', type: 'signed4' },
  { key: 'std_daily',        label: 'SD รายวัน %',       type: 'num' },
  { key: 'ann_vol',          label: 'ผันผวนต่อปี %',      type: 'num' },
  { key: 'sharpe',           label: 'Sharpe',            type: 'signed2' },
  { key: 'oi_usd',           label: 'Open Interest',     type: 'usd' },
  { key: 'oi_change',        label: 'OI เปลี่ยน %',       type: 'signed' },
  { key: 'iv',               label: 'IV %',              type: 'num' },
  { key: 'iv_premium',       label: 'IV - RV',           type: 'signed' },
  { key: 'max_drawdown',     label: 'Max drawdown %',    type: 'signed' },
  { key: 'quote_volume',     label: 'Volume 24 ชม.',     type: 'usd' },
  { key: 'days_used',        label: 'ข้อมูล (วัน)',        type: 'int' },
];
const SORTABLE = COLUMNS.filter(c => c.key !== 'symbol');

const state = {
  days: __DEFAULT_DAYS__,
  rows: [],
  meta: {},
  sortKey: 'total_return',
  sortDir: 'desc',
  query: '',
  limit: 50,
  fullOnly: false,
  filters: [],
  selected: null,
};
const charts = {};

const $ = id => document.getElementById(id);

function fmtNum(v, nd) {
  if (v === null || v === undefined || Number.isNaN(v)) return '–';
  return Number(v).toLocaleString('en-US', { minimumFractionDigits: nd, maximumFractionDigits: nd });
}
function fmtPrice(v) {
  if (v === null || v === undefined) return '–';
  const a = Math.abs(v);
  const nd = a >= 1000 ? 2 : a >= 1 ? 3 : a >= 0.01 ? 5 : 8;
  return Number(v).toLocaleString('en-US', { minimumFractionDigits: nd, maximumFractionDigits: nd });
}
function fmtUsd(v) {
  if (v === null || v === undefined) return '–';
  const a = Math.abs(v);
  if (a >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
  if (a >= 1e3) return '$' + (v / 1e3).toFixed(1) + 'K';
  return '$' + Number(v).toFixed(2);
}
function fmtCell(col, v) {
  switch (col.type) {
    case 'price':   return fmtPrice(v);
    case 'usd':     return fmtUsd(v);
    case 'signed':  return v === null || v === undefined ? '–' : (v > 0 ? '+' : '') + fmtNum(v, 2);
    case 'signed2': return v === null || v === undefined ? '–' : (v > 0 ? '+' : '') + fmtNum(v, 2);
    case 'signed4': return v === null || v === undefined ? '–' : (v > 0 ? '+' : '') + fmtNum(v, 4);
    case 'num':     return fmtNum(v, 2);
    case 'int':     return fmtNum(v, 0);
    default:        return v === null || v === undefined ? '–' : v;
  }
}
function toneClass(col, v) {
  if (!col.type.startsWith('signed') || v === null || v === undefined) return 'text-slate-200';
  if (v > 0) return 'text-emerald-400';
  if (v < 0) return 'text-rose-400';
  return 'text-slate-400';
}

/* ------------------------------ controls ------------------------------ */

function buildControls() {
  $('periodBtns').innerHTML = PERIODS.map(d => `
    <button data-days="${d}" class="period px-3.5 py-1.5 text-sm transition
      ${d === state.days ? 'bg-sky-600 text-white' : 'bg-slate-900 text-slate-300 hover:bg-slate-800'}">
      ${PERIOD_LABEL[d] || d + ' วัน'}
    </button>`).join('');
  document.querySelectorAll('.period').forEach(btn => {
    btn.onclick = () => {
      state.days = Number(btn.dataset.days);
      buildControls();
      reloadMetrics();
    };
  });

  const opts = SORTABLE.map(c => `<option value="${c.key}">${c.label}</option>`).join('');
  $('sortSelect').innerHTML = opts;
  $('sortSelect').value = state.sortKey;
  $('filterField').innerHTML = opts;
}

function renderChips() {
  const box = $('chips');
  if (!state.filters.length) { box.innerHTML = ''; return; }
  box.innerHTML = state.filters.map((f, i) => {
    const col = COLUMNS.find(c => c.key === f.field);
    const sign = f.op === 'gte' ? '≥' : '≤';
    return `<span class="inline-flex items-center gap-2 text-xs bg-sky-950/60 border border-sky-800 text-sky-200 rounded-full px-3 py-1">
      ${col ? col.label : f.field} ${sign} ${f.value}
      <button data-idx="${i}" class="chip-x text-sky-400 hover:text-white">&times;</button></span>`;
  }).join('');
  document.querySelectorAll('.chip-x').forEach(b => {
    b.onclick = () => { state.filters.splice(Number(b.dataset.idx), 1); renderChips(); renderTable(); };
  });
}

/* ------------------------------ rendering ------------------------------ */

function visibleRows() {
  let rows = state.rows.slice();
  if (state.fullOnly) {
    // เหรียญที่เพิ่งลิสต์มีข้อมูลไม่กี่วัน ค่าเฉลี่ย/อันดับจะเพี้ยน จึงตัดออกได้
    rows = rows.filter(r => r.days_used >= Math.floor(state.days * 0.9));
  }
  if (state.query) {
    const q = state.query.toUpperCase();
    rows = rows.filter(r => r.symbol.includes(q) || r.base.includes(q));
  }
  for (const f of state.filters) {
    rows = rows.filter(r => {
      const v = r[f.field];
      if (v === null || v === undefined) return false;
      return f.op === 'gte' ? v >= f.value : v <= f.value;
    });
  }
  const dir = state.sortDir === 'desc' ? -1 : 1;
  rows.sort((a, b) => {
    const x = a[state.sortKey], y = b[state.sortKey];
    if (x === null || x === undefined) return 1;
    if (y === null || y === undefined) return -1;
    return (x - y) * dir;
  });
  return state.limit ? rows.slice(0, state.limit) : rows;
}

function renderHead() {
  $('headRow').innerHTML = COLUMNS.map(c => {
    const active = c.key === state.sortKey;
    const arrow = active ? (state.sortDir === 'desc' ? ' ▼' : ' ▲') : '';
    const align = c.align === 'left' ? 'text-left' : 'text-right';
    return `<th data-key="${c.key}" class="col-head ${align} px-3 py-2.5 font-medium whitespace-nowrap select-none
      ${c.key === 'symbol' ? '' : 'cursor-pointer hover:text-sky-300'}
      ${active ? 'text-sky-400' : ''}">${c.label}${arrow}</th>`;
  }).join('');
  document.querySelectorAll('.col-head').forEach(th => {
    const key = th.dataset.key;
    if (key === 'symbol') return;
    th.onclick = () => {
      if (state.sortKey === key) state.sortDir = state.sortDir === 'desc' ? 'asc' : 'desc';
      else { state.sortKey = key; state.sortDir = 'desc'; }
      $('sortSelect').value = state.sortKey;
      updateOrderBtn();
      renderHead(); renderTable();
    };
  });
}

function renderTable() {
  const rows = visibleRows();
  const body = $('tableBody');
  $('tableEmpty').classList.toggle('hidden', rows.length > 0);
  body.innerHTML = rows.map(r => {
    const cells = COLUMNS.map(c => {
      if (c.key === 'symbol') {
        return `<td class="px-3 py-2.5 text-left whitespace-nowrap">
          <span class="font-semibold text-white">${r.base}</span>
          <span class="text-slate-500 text-xs ml-1">${r.symbol}</span>
          ${r.iv_source === 'realized' ? '<span class="ml-1.5 text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-400 align-middle">IV proxy</span>' : ''}
        </td>`;
      }
      const v = r[c.key];
      return `<td class="px-3 py-2.5 text-right mono whitespace-nowrap ${toneClass(c, v)}">${fmtCell(c, v)}</td>`;
    }).join('');
    return `<tr data-symbol="${r.symbol}" class="coin-row hover:bg-slate-800/50 transition
      ${state.selected === r.symbol ? 'bg-slate-800/70' : ''}">${cells}</tr>`;
  }).join('');
  document.querySelectorAll('.coin-row').forEach(tr => {
    tr.onclick = () => loadDetail(tr.dataset.symbol);
  });
  const pending = state.meta.partial ? ` · กำลังโหลดอีก ${state.meta.pending} เหรียญ...` : '';
  $('tableMeta').textContent =
    `${rows.length} / ${state.rows.length} เหรียญ · ช่วง ${PERIOD_LABEL[state.days] || state.days + ' วัน'}${pending}`;
}

function renderTiles() {
  const rows = state.fullOnly
    ? state.rows.filter(r => r.days_used >= Math.floor(state.days * 0.9))
    : state.rows;
  if (!rows.length) { $('tiles').innerHTML = ''; return; }
  const avg = k => {
    const xs = rows.map(r => r[k]).filter(v => v !== null && v !== undefined);
    return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
  };
  const median = k => {
    const xs = rows.map(r => r[k]).filter(v => v !== null && v !== undefined).sort((a, b) => a - b);
    if (!xs.length) return null;
    const mid = Math.floor(xs.length / 2);
    return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
  };
  const totalOI = rows.reduce((a, r) => a + (r.oi_usd || 0), 0);
  const gainers = rows.filter(r => (r.total_return || 0) > 0).length;
  const best = rows.reduce((a, r) => (a && a.total_return >= (r.total_return ?? -1e9) ? a : r), null);
  const tiles = [
    ['ผลตอบแทนสะสม (มัธยฐาน)', fmtCell({ type: 'signed' }, median('total_return')) + ' %', median('total_return')],
    ['SD รายวัน (มัธยฐาน)', fmtNum(median('std_daily'), 2) + ' %', null],
    ['IV เฉลี่ย (ต่อปี)', fmtNum(avg('iv'), 1) + ' %', null],
    ['Open Interest รวม', fmtUsd(totalOI), null],
    ['เหรียญที่ให้ผลบวก', `${gainers} / ${rows.length}` + (best ? ` · นำโดย ${best.base}` : ''), null],
  ];
  $('tiles').innerHTML = tiles.map(([label, value, tone]) => `
    <div class="bg-slate-900/60 border border-slate-800 rounded-xl px-4 py-3">
      <div class="text-[11px] uppercase tracking-wider text-slate-500">${label}</div>
      <div class="mt-1 text-lg font-semibold mono ${tone === null ? 'text-slate-100' : tone > 0 ? 'text-emerald-400' : 'text-rose-400'}">${value}</div>
    </div>`).join('');
}

/* ------------------------------ data ------------------------------ */

function showLoading(on, text) {
  $('loading').classList.toggle('hidden', !on);
  if (text) $('loadingText').textContent = text;
}
function showError(msg, retryFn) {
  const box = $('errorBox');
  box.classList.toggle('hidden', !msg);
  if (!msg) { box.innerHTML = ''; return; }
  box.innerHTML = `<div class="flex flex-wrap items-center justify-between gap-3">
      <span>${msg}</span>
      ${retryFn ? '<button id="retryBtn" class="px-3 py-1.5 text-xs rounded-lg bg-rose-900/70 border border-rose-700 hover:bg-rose-800 transition">ลองใหม่</button>' : ''}
    </div>`;
  if (retryFn) $('retryBtn').onclick = () => { showError(''); retryFn(); };
}

/* ยิง fetch พร้อม retry อัตโนมัติ - กันกรณีรีเควสต์ถูกตัดกลางทาง (เช่น HTTP 499)
   หรือเซิร์ฟเวอร์ตอบ 5xx ชั่วคราวตอนที่ตลาดกำลังโดน rate limit */
async function fetchJSON(url, signal, retries) {
  retries = retries === undefined ? 2 : retries;
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await fetch(url, { signal, cache: 'no-store' });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      return await res.json();
    } catch (err) {
      if (err.name === 'AbortError' || attempt >= retries) throw err;
      await new Promise(r => setTimeout(r, 700 * (attempt + 1)));
    }
  }
}

let metricsAbort = null;
let metricsSeq = 0;
let fillAttempts = 0;

async function loadMetrics(refresh, quiet) {
  // ยกเลิกรอบก่อนหน้าเสมอ ไม่งั้นการกดเปลี่ยนช่วงเวลารัว ๆ จะทำให้คำขอเก่าถูกตัด
  // แล้วเด้ง error ทั้งที่รอบใหม่ยังทำงานได้ปกติ
  if (metricsAbort) metricsAbort.abort();
  metricsAbort = new AbortController();
  const seq = ++metricsSeq;

  if (!quiet) {
    showLoading(true, refresh ? 'กำลังดึงข้อมูลใหม่จากตลาด...' : 'กำลังโหลดข้อมูลตลาด...');
  }
  showError('');
  try {
    const url = `/data/coins?days=${state.days}` + (refresh ? '&refresh=1' : '');
    const data = await fetchJSON(url, metricsAbort.signal);
    if (seq !== metricsSeq) return;          // มีรอบใหม่แซงไปแล้ว ทิ้งผลเก่า
    state.rows = data.rows;
    state.meta = data.meta;
    renderSource();
    renderHead();
    renderTiles();
    renderTable();
    if (state.selected && !data.meta.partial) loadDetail(state.selected, true);

    // เซิร์ฟเวอร์ตอบเท่าที่ดึงทันภายในเวลาที่กำหนด ที่เหลือยังทำงานอยู่เบื้องหลัง
    // จึงขอซ้ำเป็นระยะจนกว่าจะครบ โดยไม่บังหน้าจอด้วย overlay อีก
    if (data.meta.partial && fillAttempts < 40) {
      fillAttempts++;
      setTimeout(() => { if (seq === metricsSeq) loadMetrics(false, true); }, 1200);
    } else {
      fillAttempts = 0;
    }
  } catch (err) {
    if (err.name === 'AbortError' || seq !== metricsSeq) return;
    failSource();
    // 499 / Failed to fetch ทั้งที่เซิร์ฟเวอร์ยังรันอยู่ = คำขอถูกดักก่อนถึงเซิร์ฟเวอร์
    // เกือบทุกครั้งคือส่วนขยายเบราว์เซอร์ (ad blocker) หรือโปรแกรมความปลอดภัย
    const blocked = /HTTP (0|4\d\d)/.test(err.message) || /fetch/i.test(err.message);
    showError(
      'โหลดข้อมูลไม่สำเร็จ: ' + err.message +
      (blocked ? ' — ถ้าเซิร์ฟเวอร์ยังรันอยู่ มักเกิดจากส่วนขยายเบราว์เซอร์ (ad blocker) บล็อกคำขอ ลองเปิดในโหมดไม่ระบุตัวตน' : ''),
      () => reloadMetrics(refresh));
  } finally {
    if (seq === metricsSeq) {
      // ยังไม่มีแถวไหนพร้อมเลย = คงหน้าจอโหลดไว้ก่อน แต่ถ้ามีข้อมูลแล้วให้ดูตารางได้
      // ระหว่างที่เหรียญที่เหลือทยอยเข้ามา
      const empty = state.rows.length === 0 && state.meta.partial;
      showLoading(empty, state.meta.warming
        ? 'กำลังดึงรายชื่อเหรียญจากตลาด...'
        : 'กำลังโหลดข้อมูลเหรียญ...');
    }
  }
}

/* เปิดหน้าใหม่ = เริ่มนับรอบเติมข้อมูลใหม่เสมอ */
function reloadMetrics(refresh) {
  fillAttempts = 0;
  loadMetrics(refresh);
}

function renderSource() {
  const m = state.meta;
  const badge = $('sourceBadge');
  const dvolCoins = state.rows.filter(r => r.iv_source === 'dvol').map(r => r.base).join(', ');
  if (m.source === 'live') {
    badge.className = 'text-xs px-3 py-1.5 rounded-full border border-emerald-800 bg-emerald-950/60 text-emerald-300';
    badge.textContent = `ข้อมูลจริง · ${m.universe} เหรียญ · ${m.elapsed_sec}s`;
  } else {
    badge.className = 'text-xs px-3 py-1.5 rounded-full border border-amber-800 bg-amber-950/60 text-amber-300';
    badge.textContent = 'ข้อมูลจำลอง (เชื่อมต่อตลาดไม่ได้)';
  }
  if (m.partial && m.pending > 0) {
    badge.textContent += ` · กำลังโหลดอีก ${m.pending} เหรียญ`;
  } else if (m.failed) {
    // ส่วนใหญ่คือเหรียญที่เพิ่งลิสต์ไม่กี่วัน ข้อมูลยังไม่พอคำนวณ SD
    badge.textContent += ` · ข้าม ${m.failed} เหรียญ (ข้อมูลไม่พอ)`;
  }
  $('footNote').textContent =
    `หมายเหตุ: Open Interest ย้อนหลังจาก Binance มีให้ราว ${m.oi_history_days} วันล่าสุด ` +
    `ดังนั้นคอลัมน์ OI จะอิงหน้าต่างนั้นเสมอแม้เลือกช่วงเวลายาวกว่า · ` +
    `Implied volatility ใช้ DVOL ของ Deribit เมื่อมีให้ (รอบนี้: ${dvolCoins || 'ไม่มี'}) ` +
    `ส่วนเหรียญอื่นใช้ realized volatility ${m.rv_window} วันแบบ annualized เป็นค่าประมาณ (ป้าย "IV proxy")` +
    (m.source === 'demo' && m.source_error ? ` · สาเหตุที่ใช้ข้อมูลจำลอง: ${m.source_error}` : '');
}

function failSource() {
  const badge = $('sourceBadge');
  badge.className = 'text-xs px-3 py-1.5 rounded-full border border-rose-800 bg-rose-950/60 text-rose-300';
  badge.textContent = 'เชื่อมต่อเซิร์ฟเวอร์ไม่สำเร็จ';
}

/* ------------------------------ charts ------------------------------ */

const GRID = 'rgba(148,163,184,0.12)';
const TICK = '#94a3b8';

function baseOptions(extra) {
  return Object.assign({
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false, labels: { color: TICK, boxWidth: 10 } },
      tooltip: {
        backgroundColor: '#0f172a', borderColor: '#334155', borderWidth: 1,
        titleColor: '#e2e8f0', bodyColor: '#cbd5e1', padding: 10,
      },
    },
    scales: {
      x: { grid: { color: GRID }, ticks: { color: TICK, maxTicksLimit: 8, font: { size: 10 } } },
      y: { grid: { color: GRID }, ticks: { color: TICK, font: { size: 10 } } },
    },
  }, extra || {});
}

function drawChart(id, config) {
  if (typeof Chart === 'undefined') {
    showError('โหลดไลบรารีกราฟ (Chart.js) จาก CDN ไม่สำเร็จ - ตารางยังใช้งานได้ตามปกติ');
    return;
  }
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart($(id).getContext('2d'), config);
}

function renderCharts(s) {
  const labels = s.dates;

  drawChart('chartPrice', {
    type: 'line',
    data: { labels, datasets: [{
      data: s.close, borderColor: '#38bdf8', backgroundColor: 'rgba(56,189,248,0.12)',
      borderWidth: 2, pointRadius: 0, fill: true, tension: 0.25,
    }] },
    options: baseOptions(),
  });

  drawChart('chartReturn', {
    data: { labels, datasets: [
      { type: 'bar', label: 'Daily return %', data: s.daily_return,
        backgroundColor: s.daily_return.map(v => (v ?? 0) >= 0 ? 'rgba(52,211,153,0.75)' : 'rgba(251,113,133,0.75)'),
        borderWidth: 0, yAxisID: 'y' },
      { type: 'line', label: 'SD 14 วัน %', data: s.rolling_sd,
        borderColor: '#fbbf24', borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: 'y1' },
    ] },
    options: baseOptions({
      plugins: { legend: { display: true, labels: { color: TICK, boxWidth: 10 } } },
      scales: {
        x: { grid: { color: GRID }, ticks: { color: TICK, maxTicksLimit: 8, font: { size: 10 } } },
        y: { position: 'left', grid: { color: GRID }, ticks: { color: TICK, font: { size: 10 } } },
        y1: { position: 'right', grid: { display: false }, ticks: { color: '#fbbf24', font: { size: 10 } } },
      },
    }),
  });

  // OI มีย้อนหลังแค่ ~30 วัน จึงตัดแกน x ให้เหลือเฉพาะช่วงที่มีข้อมูลจริง
  const oiStart = s.open_interest.findIndex(v => v !== null);
  const hasOI = oiStart >= 0;
  const oiLabels = hasOI ? labels.slice(oiStart) : labels;
  const oiData = hasOI ? s.open_interest.slice(oiStart) : s.open_interest;
  drawChart('chartOI', {
    type: 'line',
    data: { labels: oiLabels, datasets: [{
      data: oiData, borderColor: '#a78bfa', backgroundColor: 'rgba(167,139,250,0.15)',
      borderWidth: 2, pointRadius: 0, fill: true, tension: 0.25, spanGaps: false,
    }] },
    options: baseOptions({
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: c => 'OI ' + fmtUsd(c.parsed.y) } },
        title: hasOI ? { display: false } : {
          display: true, color: '#94a3b8', text: 'ไม่มีข้อมูล open interest สำหรับเหรียญนี้',
        },
      },
      scales: {
        x: { grid: { color: GRID }, ticks: { color: TICK, maxTicksLimit: 8, font: { size: 10 } } },
        y: { grid: { color: GRID }, ticks: { color: TICK, font: { size: 10 }, callback: v => fmtUsd(v) } },
      },
    }),
  });

  const rvWindow = state.meta.rv_window || 30;
  const ivDatasets = [{
    label: s.iv_source === 'dvol'
      ? 'Implied vol (Deribit DVOL)'
      : 'IV proxy = realized vol ' + rvWindow + ' วัน',
    data: s.implied_vol, borderColor: '#f472b6', borderWidth: 2, pointRadius: 0, tension: 0.3,
  }];
  if (s.iv_source === 'dvol') {
    ivDatasets.push({
      label: 'Realized vol ' + rvWindow + ' วัน (annualized)',
      data: s.realized_vol, borderColor: '#34d399', borderWidth: 2, borderDash: [5, 4],
      pointRadius: 0, tension: 0.3,
    });
  }
  drawChart('chartIV', {
    type: 'line',
    data: { labels, datasets: ivDatasets },
    options: baseOptions({ plugins: { legend: { display: true, labels: { color: TICK, boxWidth: 10 } } } }),
  });
}

/* ------------------------------ detail panel ------------------------------ */

let detailAbort = null;
let detailSeq = 0;

async function loadDetail(symbol, quiet) {
  state.selected = symbol;
  if (detailAbort) detailAbort.abort();
  detailAbort = new AbortController();
  const seq = ++detailSeq;

  if (!quiet) showLoading(true, 'กำลังโหลดกราฟของ ' + symbol + '...');
  try {
    const data = await fetchJSON(`/data/coin/${symbol}?days=${state.days}`, detailAbort.signal);
    if (seq !== detailSeq) return;
    const sum = data.summary, s = data.series;

    $('detail').classList.remove('hidden');
    $('detailTitle').textContent = `${sum.base} · ${sum.symbol}`;
    $('detailSub').textContent =
      `ช่วง ${PERIOD_LABEL[state.days] || state.days + ' วัน'} · ใช้ข้อมูล ${sum.days_used} วัน · ` +
      `IV จาก ${sum.iv_source === 'dvol' ? 'Deribit DVOL' : 'realized volatility (proxy)'} · ` +
      `OI ย้อนหลัง ${sum.oi_days} วัน`;

    const stats = [
      ['ราคาล่าสุด', fmtPrice(sum.last_price), null],
      ['ผลตอบแทนสะสม', fmtCell({ type: 'signed' }, sum.total_return) + ' %', sum.total_return],
      ['Daily return เฉลี่ย', fmtCell({ type: 'signed4' }, sum.avg_daily_return) + ' %', sum.avg_daily_return],
      ['SD รายวัน', fmtNum(sum.std_daily, 2) + ' %', null],
      ['ผันผวนต่อปี', fmtNum(sum.ann_vol, 1) + ' %', null],
      ['Open Interest', fmtUsd(sum.oi_usd), null],
      ['Implied vol', fmtNum(sum.iv, 1) + ' %', null],
      ['Max drawdown', fmtNum(sum.max_drawdown, 1) + ' %', sum.max_drawdown],
    ];
    $('detailStats').innerHTML = stats.map(([l, v, tone]) => `
      <div class="bg-slate-950/60 border border-slate-800 rounded-xl px-3 py-2">
        <div class="text-[11px] text-slate-500">${l}</div>
        <div class="mt-0.5 text-sm font-semibold mono ${tone === null || tone === undefined ? 'text-slate-100' : tone > 0 ? 'text-emerald-400' : 'text-rose-400'}">${v}</div>
      </div>`).join('');

    renderCharts(s);
    renderTable();
    if (!quiet) $('detail').scrollIntoView({ behavior: 'smooth', block: 'start' });
  } catch (err) {
    if (err.name === 'AbortError' || seq !== detailSeq) return;
    showError('โหลดกราฟไม่สำเร็จ: ' + err.message, () => loadDetail(symbol, quiet));
  } finally {
    if (seq === detailSeq && !quiet) showLoading(false);
  }
}

/* ------------------------------ events ------------------------------ */

function updateOrderBtn() {
  $('orderBtn').innerHTML = state.sortDir === 'desc' ? 'มาก &rarr; น้อย' : 'น้อย &rarr; มาก';
}

const PRESETS = {
  return: { sort: 'total_return', dir: 'desc', filters: [] },
  stable: { sort: 'sharpe', dir: 'desc', filters: [{ field: 'total_return', op: 'gte', value: 0 }] },
  oi:     { sort: 'oi_usd', dir: 'desc', filters: [] },
  iv:     { sort: 'iv', dir: 'desc', filters: [] },
  clear:  { sort: 'total_return', dir: 'desc', filters: [], reset: true },
};

function wireEvents() {
  $('refreshBtn').onclick = () => reloadMetrics(true);

  $('searchInput').oninput = e => { state.query = e.target.value.trim(); renderTable(); };

  $('sortSelect').onchange = e => { state.sortKey = e.target.value; renderHead(); renderTable(); };

  $('orderBtn').onclick = () => {
    state.sortDir = state.sortDir === 'desc' ? 'asc' : 'desc';
    updateOrderBtn(); renderHead(); renderTable();
  };

  $('limitSelect').onchange = e => { state.limit = Number(e.target.value); renderTable(); };

  $('fullOnly').onchange = e => { state.fullOnly = e.target.checked; renderTiles(); renderTable(); };

  $('addFilterBtn').onclick = () => {
    const raw = $('filterValue').value;
    if (raw === '') return;
    state.filters.push({
      field: $('filterField').value,
      op: $('filterOp').value,
      value: Number(raw),
    });
    $('filterValue').value = '';
    renderChips(); renderTable();
  };
  $('filterValue').onkeydown = e => { if (e.key === 'Enter') $('addFilterBtn').click(); };

  document.querySelectorAll('.preset').forEach(btn => {
    btn.onclick = () => {
      const p = PRESETS[btn.dataset.preset];
      if (!p) return;
      state.sortKey = p.sort;
      state.sortDir = p.dir;
      state.filters = p.filters.map(f => Object.assign({}, f));
      if (p.reset) { state.query = ''; $('searchInput').value = ''; }
      $('sortSelect').value = state.sortKey;
      updateOrderBtn(); renderChips(); renderHead(); renderTable();
    };
  });

  $('closeDetail').onclick = () => {
    $('detail').classList.add('hidden');
    state.selected = null;
    renderTable();
  };
}

buildControls();
updateOrderBtn();
wireEvents();
reloadMetrics();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print("Crypto Metrics Dashboard -> http://127.0.0.1:%d" % port)
    app.run(host=os.environ.get("HOST", "127.0.0.1"), port=port, debug=False, threaded=True)
