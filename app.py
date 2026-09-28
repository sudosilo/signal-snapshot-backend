"""
Signal Snapshot backend, Orange phone code.
Part of the Mohave Technology Group continuity set, paired with the ticker tracker.

What this does: the tracker posts the price bars for whatever ticker is open on screen.
This service stores those bars, runs TA-Lib pattern and indicator math on them, and when
a rule fires it writes a snapshot of everything known at that moment to an append only
Redis Stream. Later a grader labels each snapshot win, loss, or timeout using the triple
barrier method, and a report shows how each rule has performed.

Observation only. Nothing here places an order or talks to a brokerage.

Handoff notes: the five parts are the feed (POST /bars), the detector (detect), the
snapshot logger (log_snapshot), the grader (grade_pending), and the report (GET /report).
The scorer from phase three is not built yet. Each part passes plain dicts to the next,
so any one can be replaced without touching the others.
"""

import json
import math
import os
import time
from datetime import datetime, timezone

import numpy as np
import redis
import talib
from talib import abstract
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

REDIS_URL = os.environ.get("REDIS_URL", "")

# grading rules, adjustable from Railway variables without a code change
TARGET_PCT = float(os.environ.get("TARGET_PCT", "3"))
STOP_PCT = float(os.environ.get("STOP_PCT", "2"))
COST_PCT = float(os.environ.get("COST_PCT", "0.1"))

# the time limit is counted in bars, and a bar means something different on each chart
# range, so each range gets its own limit that works out to roughly the same idea:
# about a week on daily bars and hourly bars, one trading session on 5 minute bars.
LIMIT_BARS = {"DAILY": 5, "1W": 35, "1D": 78}

MIN_BARS = 30          # fewer than this and the indicators have nothing to warm up on
WINDOW_BARS = 250      # how much history the detector looks at
SNAPSHOT_BARS = 60     # how many bars get saved into each snapshot for the chart
MAX_STORED_BARS = 1000 # per ticker per range, oldest dropped past this

STREAM_SIGNALS = "ss:signals"
STREAM_GRADES = "ss:grades"

_client = None


def db():
    global _client
    if not REDIS_URL:
        raise HTTPException(status_code=503, detail="REDIS_URL is not set on this service")
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses=True)
    return _client


app = FastAPI(title="signal snapshot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------- feed adapter

class Bar(BaseModel):
    date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float
    volume: float | None = None


class BarsIn(BaseModel):
    ticker: str
    interval: str
    bars: list[Bar]
    context: dict = {}


def clean(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def bars_key(ticker, interval):
    return f"ss:bars:{ticker}:{interval}"


def store_bars(ticker, interval, bars):
    """Merge incoming bars into the stored series, keyed by bar date so a repeat post of
    the same bar overwrites rather than duplicates. Returns the full sorted series."""
    r = db()
    key = bars_key(ticker, interval)
    if bars:
        r.hset(key, mapping={
            b.date: json.dumps({"date": b.date, "open": clean(b.open), "high": clean(b.high),
                                "low": clean(b.low), "close": clean(b.close), "volume": clean(b.volume)})
            for b in bars if clean(b.close) is not None
        })
    series = sorted((json.loads(v) for v in r.hvals(key)), key=lambda b: b["date"])
    if len(series) > MAX_STORED_BARS:
        drop = series[: len(series) - MAX_STORED_BARS]
        r.hdel(key, *[b["date"] for b in drop])
        series = series[-MAX_STORED_BARS:]
    return series


# ---------------------------------------------------------------- detector

PATTERN_FUNCS = talib.get_function_groups()["Pattern Recognition"]
PATTERN_NAMES = {}
for _fn in PATTERN_FUNCS:
    try:
        PATTERN_NAMES[_fn] = abstract.Function(_fn).info.get("display_name", _fn)
    except Exception:
        PATTERN_NAMES[_fn] = _fn


def _crossed_up(prev, now, level):
    return prev is not None and now is not None and prev < level <= now


def _crossed_down(prev, now, level):
    return prev is not None and now is not None and prev > level >= now


def _num(x):
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else round(float(x), 4)


def detect(series):
    """Runs the rules against the most recent complete bar. The very last bar is treated
    as still forming, since on a live chart it keeps changing until its period closes,
    so the signal bar is the one before it. That keeps a pattern from firing on a candle
    that later turns into something else."""
    window = series[-WINDOW_BARS:]
    if len(window) < MIN_BARS + 1:
        return None, []

    close = np.array([b["close"] for b in window], dtype=float)
    i = len(window) - 2
    signal_bar = window[i]

    rsi = talib.RSI(close, timeperiod=14)
    roc = talib.ROC(close, timeperiod=5)
    macd, macd_sig, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)

    values = {
        "close": _num(close[i]),
        "rsi14": _num(rsi[i]),
        "roc5": _num(roc[i]),
        "macd": _num(macd[i]),
        "macd_signal": _num(macd_sig[i]),
    }
    signals = []

    def fire(rule, direction, extra=None):
        v = dict(values)
        if extra:
            v.update(extra)
        signals.append({"rule": rule, "direction": direction, "values": v})

    if _crossed_up(_num(rsi[i - 1]), _num(rsi[i]), 30):
        fire("RSI leaves oversold", "bullish")
    if _crossed_down(_num(rsi[i - 1]), _num(rsi[i]), 70):
        fire("RSI leaves overbought", "bearish")

    if _crossed_up(_num(roc[i - 1]), _num(roc[i]), 5):
        fire("5 bar momentum breakout up", "bullish")
    if _crossed_down(_num(roc[i - 1]), _num(roc[i]), -5):
        fire("5 bar momentum breakout down", "bearish")

    d_prev = None if math.isnan(macd[i - 1]) or math.isnan(macd_sig[i - 1]) else macd[i - 1] - macd_sig[i - 1]
    d_now = None if math.isnan(macd[i]) or math.isnan(macd_sig[i]) else macd[i] - macd_sig[i]
    if _crossed_up(d_prev, d_now, 0):
        fire("MACD crosses above signal", "bullish")
    if _crossed_down(d_prev, d_now, 0):
        fire("MACD crosses below signal", "bearish")

    # candlestick patterns need real open, high, and low values. bars that arrived with
    # only a close price, from an older cache or a source that doesn't supply them, just
    # skip this part rather than feeding the patterns fake numbers.
    has_ohlc = all(b.get("open") is not None and b.get("high") is not None and b.get("low") is not None
                   for b in window)
    if has_ohlc:
        o = np.array([b["open"] for b in window], dtype=float)
        h = np.array([b["high"] for b in window], dtype=float)
        lo = np.array([b["low"] for b in window], dtype=float)
        for fn in PATTERN_FUNCS:
            out = getattr(talib, fn)(o, h, lo, close)
            val = int(out[i])
            if val != 0:
                fire(PATTERN_NAMES[fn], "bullish" if val > 0 else "bearish", {"pattern_value": val})

    return signal_bar, signals


# ---------------------------------------------------------------- snapshot logger

def log_snapshot(ticker, interval, signal_bar, signal, series, context):
    """Writes one append only record per signal. Only information up to and including
    the signal bar goes in, the chart window is cut at the signal bar, so nothing from
    after the moment of the signal can leak into the record."""
    r = db()
    seen_key = f"ss:seen:{ticker}:{interval}:{signal_bar['date']}:{signal['rule']}"
    if not r.set(seen_key, "1", nx=True, ex=60 * 60 * 24 * 60):
        return None
    cut = [b for b in series if b["date"] <= signal_bar["date"]][-SNAPSHOT_BARS:]
    record = {
        "ticker": ticker,
        "interval": interval,
        "bar_time": signal_bar["date"],
        "direction": signal["direction"],
        "rule": signal["rule"],
        "values": json.dumps(signal["values"]),
        "context": json.dumps(context or {}),
        "bars": json.dumps(cut),
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return r.xadd(STREAM_SIGNALS, record, maxlen=20000, approximate=True)


# ---------------------------------------------------------------- grader

def barrier_outcome(direction, entry, future, limit_bars):
    """Triple barrier: whichever of the profit target, the stop, or the time limit is
    touched first decides the label. When one bar touches both the target and the stop
    there is no way to know which came first inside that bar, so it counts as a loss,
    the cautious reading. Trading cost comes off every result."""
    if entry is None or entry <= 0:
        return None
    long = direction == "bullish"
    up = entry * (1 + TARGET_PCT / 100) if long else entry * (1 - TARGET_PCT / 100)
    down = entry * (1 - STOP_PCT / 100) if long else entry * (1 + STOP_PCT / 100)
    for n, b in enumerate(future[:limit_bars], start=1):
        hi = b.get("high") if b.get("high") is not None else b["close"]
        lo = b.get("low") if b.get("low") is not None else b["close"]
        hit_target = hi >= up if long else lo <= up
        hit_stop = lo <= down if long else hi >= down
        if hit_target and hit_stop:
            return {"label": "loss", "exit_bar": b["date"], "bars_held": n, "return_pct": -STOP_PCT - COST_PCT, "note": "target and stop inside one bar"}
        if hit_stop:
            return {"label": "loss", "exit_bar": b["date"], "bars_held": n, "return_pct": -STOP_PCT - COST_PCT}
        if hit_target:
            return {"label": "win", "exit_bar": b["date"], "bars_held": n, "return_pct": TARGET_PCT - COST_PCT}
    if len(future) >= limit_bars:
        last = future[limit_bars - 1]
        move = (last["close"] - entry) / entry * 100
        ret = (move if long else -move) - COST_PCT
        return {"label": "timeout", "exit_bar": last["date"], "bars_held": limit_bars, "return_pct": round(ret, 3)}
    return None


def graded_ids(r):
    return {g["signal_id"] for _, g in r.xrange(STREAM_GRADES)}


def grade_pending():
    """Looks at every ungraded signal and grades the ones whose outcome is now known.
    Grades are written as their own linked records, the original snapshot is never edited."""
    r = db()
    done = graded_ids(r)
    series_cache = {}
    written = 0
    for sid, s in r.xrange(STREAM_SIGNALS):
        if sid in done:
            continue
        key = (s["ticker"], s["interval"])
        if key not in series_cache:
            series_cache[key] = sorted((json.loads(v) for v in r.hvals(bars_key(*key))), key=lambda b: b["date"])
        series = series_cache[key]
        entry = json.loads(s["values"]).get("close")
        # the newest stored bar is still forming and its high, low, and close keep moving
        # until its period ends, so it never counts toward an outcome. grades are permanent,
        # and grading off a half built candle would lock in a result that could change.
        future = [b for b in series[:-1] if b["date"] > s["bar_time"]]
        out = barrier_outcome(s["direction"], entry, future, LIMIT_BARS.get(s["interval"], 5))
        if out is None:
            continue
        r.xadd(STREAM_GRADES, {"signal_id": sid, "rule": s["rule"], "ticker": s["ticker"],
                               "interval": s["interval"], "direction": s["direction"],
                               "label": out["label"], "return_pct": str(out["return_pct"]),
                               "exit_bar": out["exit_bar"], "bars_held": str(out["bars_held"]),
                               "note": out.get("note", ""),
                               "graded_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
        written += 1
    return written


# ---------------------------------------------------------------- routes

@app.get("/health")
def health():
    ok = False
    if REDIS_URL:
        try:
            ok = bool(db().ping())
        except Exception:
            ok = False
    return {"status": "ok", "redisConfigured": bool(REDIS_URL), "redisOk": ok,
            "patterns": len(PATTERN_FUNCS), "talib": talib.__version__}


@app.post("/bars")
def receive_bars(payload: BarsIn):
    ticker = payload.ticker.strip().upper()
    interval = payload.interval.strip().upper()
    if not ticker or interval not in LIMIT_BARS:
        raise HTTPException(status_code=400, detail="ticker and an interval of DAILY, 1W, or 1D are required")
    series = store_bars(ticker, interval, payload.bars)
    signal_bar, signals = detect(series)
    logged = []
    for sig in signals:
        sid = log_snapshot(ticker, interval, signal_bar, sig, series, payload.context)
        if sid:
            logged.append({"id": sid, "rule": sig["rule"], "direction": sig["direction"]})
    graded = grade_pending()
    return {"stored": len(series), "signal_bar": signal_bar["date"] if signal_bar else None,
            "fired": logged, "graded": graded}


@app.get("/signals")
def list_signals(limit: int = 50, ticker: str = ""):
    r = db()
    grades = {g["signal_id"]: g for _, g in r.xrange(STREAM_GRADES)}
    want = ticker.strip().upper()
    out = []
    for sid, s in r.xrevrange(STREAM_SIGNALS, count=2000):
        if want and s["ticker"] != want:
            continue
        g = grades.get(sid)
        out.append({
            "id": sid, "ticker": s["ticker"], "interval": s["interval"], "bar_time": s["bar_time"],
            "direction": s["direction"], "rule": s["rule"], "logged_at": s["logged_at"],
            "values": json.loads(s["values"]), "context": json.loads(s["context"]),
            "bars": json.loads(s["bars"]),
            "grade": None if not g else {"label": g["label"], "return_pct": float(g["return_pct"]),
                                         "exit_bar": g["exit_bar"], "bars_held": int(g["bars_held"]),
                                         "note": g.get("note", "")},
        })
        if len(out) >= max(1, min(limit, 500)):
            break
    return {"signals": out}


@app.post("/grade")
def run_grader():
    return {"graded": grade_pending()}


@app.get("/report")
def report():
    r = db()
    rules = {}
    for _, g in r.xrange(STREAM_GRADES):
        k = g["rule"]
        row = rules.setdefault(k, {"rule": k, "graded": 0, "wins": 0, "losses": 0, "timeouts": 0,
                                   "gains": [], "drops": []})
        row["graded"] += 1
        ret = float(g["return_pct"])
        if g["label"] == "win":
            row["wins"] += 1
        elif g["label"] == "loss":
            row["losses"] += 1
        else:
            row["timeouts"] += 1
        (row["gains"] if ret > 0 else row["drops"]).append(ret)
    total_signals = r.xlen(STREAM_SIGNALS)
    out = []
    for row in rules.values():
        out.append({
            "rule": row["rule"], "graded": row["graded"], "wins": row["wins"], "losses": row["losses"],
            "timeouts": row["timeouts"],
            "win_rate": round(row["wins"] / row["graded"] * 100, 1) if row["graded"] else 0,
            "avg_gain": round(sum(row["gains"]) / len(row["gains"]), 2) if row["gains"] else 0,
            "avg_loss": round(sum(row["drops"]) / len(row["drops"]), 2) if row["drops"] else 0,
        })
    out.sort(key=lambda x: (-x["graded"], x["rule"]))
    return {"rules": out, "total_signals": total_signals,
            "settings": {"target_pct": TARGET_PCT, "stop_pct": STOP_PCT, "cost_pct": COST_PCT, "limit_bars": LIMIT_BARS}}
