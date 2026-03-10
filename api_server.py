import os
import time
import json
import hashlib
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
import yfinance as yf
import feedparser
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# CONFIGURATION
# ============================================================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
CACHE_TTL = {
    "gold": 60, "crypto": 45, "fx": 120, "news": 300,
    "search": 600, "weather": 600, "health": 30,
}

# ============================================================
# CACHE
# ============================================================
_cache = {}
def cache_get(key):
    if key in _cache:
        entry = _cache[key]
        if time.time() - entry["ts"] < entry["ttl"]:
            return entry["data"], True
    return None, False

def cache_set(key, data, ttl):
    _cache[key] = {"data": data, "ts": time.time(), "ttl": ttl}

def cache_get_stale(key):
    if key in _cache:
        return _cache[key]["data"]
    return None

# ============================================================
# RESPONSE WRAPPER
# ============================================================
def make_response(data, provider, stale=False, cached=False, trust="live", cross_check=None, error=None):
    resp = {
        "data": data, "provider": provider,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "stale": stale, "cached": cached, "trust": trust,
    }
    if cross_check: resp["cross_check"] = cross_check
    if error: resp["error"] = error
    return resp

# ============================================================
# YFINANCE HELPERS
# ============================================================
def yf_get_price(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        price = info.get("lastPrice") or info.get("last_price")
        if price: return float(price), "yahoo"
    except: pass
    return None, None

def yf_get_history(symbol, period="5d", interval="1h"):
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)
        if hist is not None and len(hist) > 0:
            records = []
            for idx, row in hist.iterrows():
                records.append({
                    "time": idx.isoformat(),
                    "open": round(float(row["Open"]), 2),
                    "high": round(float(row["High"]), 2),
                    "low": round(float(row["Low"]), 2),
                    "close": round(float(row["Close"]), 2),
                    "volume": int(row["Volume"]) if row["Volume"] else 0
                })
            return records
    except: pass
    return None

# ============================================================
# FX CROSS-CHECK
# ============================================================
def get_fx_yahoo(pair="USDINR=X"):
    try:
        price, _ = yf_get_price(pair)
        return price
    except: return None

def get_fx_exchangerate(base="USD", target="INR"):
    try:
        with httpx.Client(timeout=5) as client:
            resp = client.get(f"https://open.er-api.com/v6/latest/{base}")
            if resp.status_code != 200 or not resp.text.strip():
                return None
            data = resp.json()
            if data.get("result") == "success":
                return data["rates"].get(target)
    except: return None

def get_fx_crosschecked(pair="USDINR", base="USD", target="INR"):
    yahoo_rate = get_fx_yahoo(f"{pair}=X")
    er_rate = get_fx_exchangerate(base, target)
    if yahoo_rate and er_rate:
        diff = abs(yahoo_rate - er_rate)
        status = "agree" if diff < 0.5 else "disagree"
        return yahoo_rate, "yahoo", {"exchangerate_api": er_rate, "diff": round(diff, 4), "status": status}
    elif yahoo_rate:
        return yahoo_rate, "yahoo", {"exchangerate_api": None, "status": "single_source"}
    elif er_rate:
        return er_rate, "exchangerate_api", {"yahoo": None, "status": "fallback"}
    return None, None, {"status": "all_failed"}

# ============================================================
# GOLD (with safe OI fetch)
# ============================================================
def fetch_gold_data():
    result = {}
    errors = []
    try:
        gold_price, _ = yf_get_price("GC=F")
        gold_hist = yf_get_history("GC=F", period="5d", interval="1h")
        result["futures"] = {"price": gold_price, "symbol": "GC=F", "history": gold_hist}
    except Exception as e: errors.append(f"gold_futures: {e}")
    try:
        gvz, _ = yf_get_price("^GVZ")
        result["gvz"] = gvz
    except Exception as e: errors.append(f"gvz: {e}")
    # FIXED: Safe OI fetch - gc.info can return empty/invalid JSON
    try:
        gc = yf.Ticker("GC=F")
        try:
            info = gc.info if hasattr(gc, 'info') else {}
            if isinstance(info, dict):
                result["open_interest"] = info.get("openInterest")
            else:
                result["open_interest"] = None
                errors.append("oi: info returned non-dict")
        except (json.JSONDecodeError, ValueError, Exception) as e2:
            result["open_interest"] = None
            errors.append(f"oi: {e2}")
    except Exception as e: errors.append(f"oi: {e}")
    fx_rate, fx_provider, fx_cross = get_fx_crosschecked()
    result["usd_inr"] = {"rate": fx_rate, "provider": fx_provider, "cross_check": fx_cross}
    if result.get("futures", {}).get("price") and fx_rate:
        result["mcx_gold_approx"] = round(result["futures"]["price"] * fx_rate / 31.1035 * 10, 0)
    try:
        gld_price, _ = yf_get_price("GLD")
        if gld_price:
            result["spot_gold"] = {"price": round(gld_price / 0.09127, 2), "method": "GLD/0.09127", "gld_price": gld_price}
    except Exception as e: errors.append(f"spot: {e}")
    try:
        hist_30d = yf_get_history("GC=F", period="1mo", interval="1d")
        if hist_30d and len(hist_30d) > 2:
            h, l, c = hist_30d[-1]["high"], hist_30d[-1]["low"], hist_30d[-1]["close"]
            pivot = (h + l + c) / 3
            result["support_resistance"] = {
                "pivot": round(pivot, 2),
                "r1": round(2 * pivot - l, 2), "r2": round(pivot + (h - l), 2),
                "s1": round(2 * pivot - h, 2), "s2": round(pivot - (h - l), 2),
                "method": "pivot_points", "based_on": hist_30d[-1]["time"]
            }
    except Exception as e: errors.append(f"sr: {e}")
    if errors: result["_errors"] = errors
    return result

# ============================================================
# CRYPTO
# ============================================================
def fetch_crypto(coin_id="bitcoin"):
    try:
        params = {"localization": "false", "tickers": "false", "community_data": "false", "developer_data": "false"}
        with httpx.Client(timeout=5) as client:
            resp = client.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}", params=params)
            if resp.status_code != 200 or not resp.text.strip():
                return {"error": f"CoinGecko returned status {resp.status_code}"}
            data = resp.json()
            m = data.get("market_data", {})
            return {
                "name": data.get("name"), "symbol": data.get("symbol"),
                "price_usd": m.get("current_price", {}).get("usd"),
                "price_inr": m.get("current_price", {}).get("inr"),
                "change_24h": m.get("price_change_percentage_24h"),
                "market_cap_usd": m.get("market_cap", {}).get("usd"),
                "volume_24h": m.get("total_volume", {}).get("usd"),
                "high_24h": m.get("high_24h", {}).get("usd"),
                "low_24h": m.get("low_24h", {}).get("usd"),
                "image": data.get("image", {}).get("small"),
            }
    except Exception as e: return {"error": str(e)}

# ============================================================
# NEWS
# ============================================================
def fetch_news(query="gold prices"):
    try:
        feed = feedparser.parse(f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en")
        return [{"title": e.get("title",""), "link": e.get("link",""), "published": e.get("published",""),
                "source": e.get("source",{}).get("title","") if hasattr(e,"source") else ""} for e in feed.entries[:10]]
    except Exception as e: return {"error": str(e)}

# ============================================================
# WEATHER (Open-Meteo, free, no key)
# ============================================================
CITY_COORDS = {
    "kolkata": (22.57, 88.36), "delhi": (28.61, 77.23), "mumbai": (19.08, 72.88),
    "bangalore": (12.97, 77.59), "chennai": (13.08, 80.27), "hyderabad": (17.39, 78.49),
    "new york": (40.71, -74.01), "london": (51.51, -0.13), "tokyo": (35.68, 139.69),
    "paris": (48.86, 2.35), "dubai": (25.20, 55.27), "singapore": (1.35, 103.82),
}
def fetch_weather(city="Kolkata"):
    try:
        coords = CITY_COORDS.get(city.lower(), (22.57, 88.36))
        params = {"latitude": coords[0], "longitude": coords[1], "current_weather": "true",
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
                "timezone": "auto", "forecast_days": 7}
        with httpx.Client(timeout=5) as client:
            resp = client.get("https://api.open-meteo.com/v1/forecast", params=params)
            if resp.status_code != 200 or not resp.text.strip():
                return {"error": f"Weather API returned status {resp.status_code}"}
            return resp.json()
    except Exception as e: return {"error": str(e)}

# ============================================================
# GEMINI WITH GROUNDING
# ============================================================
CARD_SYSTEM_PROMPT = """You are VSPE, a Visual Search Presentation Engine.
Return ALL answers as a JSON object with this exact structure:
{
    "cards": [
        {
            "type": "stat|chart|image|table|checklist|action|fact|video|disclaimer",
            "title": "Short title (max 6 words)",
            "content": "The data or text (keep under 30 words per card)",
            "value": "For stat cards: the main number/value",
            "unit": "For stat cards: the unit (optional)",
            "source": "Where this info comes from",
            "items": ["For table/checklist: array of items"],
            "columns": ["For table: column headers"],
            "rows": [["For table: row data"]],
            "url": "For video/action/image: relevant URL (optional)"
        }
    ],
    "query_type": "knowledge|comparison|howto|person|place|event|opinion"
}
Rules:
- MINIMUM text. Maximum visual cards.
- Every fact gets its own card.
- Numbers always go in stat cards with large values.
- Comparisons always go in table cards.
- Steps always go in checklist cards.
- Always include a source for each card.
- Return ONLY valid JSON. No markdown. No explanation outside JSON.
- Support Hindi and Hinglish queries naturally.
"""

def search_gemini(query):
    if not GEMINI_API_KEY:
        return {"error": "Gemini API key not configured. Add GEMINI_API_KEY in Render environment variables."}
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": query}]}],
            "systemInstruction": {"parts": [{"text": CARD_SYSTEM_PROMPT}]},
            "tools": [{"googleSearch": {}}],
                        "generationConfig": {"temperature": 0.3}
        }
        with httpx.Client(timeout=30) as client:
            resp = client.post(url, json=payload)
                        "generationConfig": {"temperature": 0.3}
                return {"error": f"Gemini returned status {resp.status_code}"}
            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return {"error": "No response from Gemini", "raw": data}
            text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text.startswith("```"):
                text = text.split("\n", 1)[-1]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
            grounding = candidates[0].get("groundingMetadata", {})
            sources = [{"title": c.get("web",{}).get("title",""), "url": c.get("web",{}).get("uri","")} 
                    for c in grounding.get("groundingChunks", []) if c.get("web")]
            try:
                cards = json.loads(text)
            except json.JSONDecodeError:
                cards = {"cards": [{"type": "fact", "title": "Answer", "content": text[:500], "source": "Gemini"}]}
            return {"result": cards, "sources": sources, "grounded": len(sources) > 0}
    except Exception as e:
        return {"error": str(e)}

# ============================================================
# QUERY CLASSIFIER
# ============================================================
FINANCIAL_KW = {"gold","silver","price","stock","share","nifty","sensex","dow","nasdaq","s&p",
                "crude","oil","futures","mcx","comex","etf","mutual fund","gvz","vix","platinum","palladium"}
CRYPTO_KW = {"bitcoin","btc","ethereum","eth","crypto","dogecoin","solana","xrp","bnb","altcoin","doge","shib","cardano","ada"}
FX_KW = {"usd","inr","eur","gbp","forex","exchange rate","currency","dollar","rupee","yen","pound"}
NEWS_KW = {"news","latest","headlines","breaking"}
WEATHER_KW = {"weather","temperature","rain","forecast","humidity","wind","climate"}
TICKER_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ.=-^")

def classify_query(query):
    q = query.lower().strip()
    words = query.strip().split()
    if len(words) == 1 and len(words[0]) <= 5 and all(c in TICKER_CHARS for c in words[0]):
        return "financial"
    for kw in CRYPTO_KW:
        if kw in q: return "crypto"
    for kw in WEATHER_KW:
        if kw in q: return "weather"
    for kw in FX_KW:
        if kw in q: return "fx"
    for kw in FINANCIAL_KW:
        if kw in q: return "financial"
    for kw in NEWS_KW:
        if kw in q: return "news"
    return "general"

# ============================================================
# HEALTH CHECK
# ============================================================
def check_all_health():
    results = {}
    start = time.time()
    try:
        p, _ = yf_get_price("GC=F")
        results["yfinance"] = {"status": "live" if p else "degraded", "latency_ms": round((time.time()-start)*1000), "sample": p}
    except Exception as e:
        results["yfinance"] = {"status": "down", "error": str(e), "latency_ms": round((time.time()-start)*1000)}
    start = time.time()
    try:
        r = get_fx_exchangerate()
        results["exchangerate_api"] = {"status": "live" if r else "degraded", "latency_ms": round((time.time()-start)*1000), "sample": r}
    except Exception as e:
        results["exchangerate_api"] = {"status": "down", "error": str(e), "latency_ms": round((time.time()-start)*1000)}
    start = time.time()
    try:
        with httpx.Client(timeout=5) as client:
            resp = client.get("https://api.coingecko.com/api/v3/ping")
            results["coingecko"] = {"status": "live" if resp.status_code == 200 else "degraded", "latency_ms": round((time.time()-start)*1000)}
    except Exception as e:
        results["coingecko"] = {"status": "down", "error": str(e), "latency_ms": round((time.time()-start)*1000)}
    start = time.time()
    if GEMINI_API_KEY:
        try:
            with httpx.Client(timeout=5) as client:
                resp = client.get(f"https://generativelanguage.googleapis.com/v1beta/models?key={GEMINI_API_KEY}")
                results["gemini"] = {"status": "live" if resp.status_code == 200 else "degraded", "latency_ms": round((time.time()-start)*1000)}
        except Exception as e:
            results["gemini"] = {"status": "down", "error": str(e), "latency_ms": round((time.time()-start)*1000)}
    else:
        results["gemini"] = {"status": "no_key", "latency_ms": 0}
    start = time.time()
    try:
        feed = feedparser.parse("https://news.google.com/rss?hl=en-IN&gl=IN&ceid=IN:en")
        results["google_news"] = {"status": "live" if feed.entries else "degraded", "latency_ms": round((time.time()-start)*1000), "articles": len(feed.entries)}
    except Exception as e:
        results["google_news"] = {"status": "down", "error": str(e), "latency_ms": round((time.time()-start)*1000)}
    start = time.time()
    try:
        w = fetch_weather("Kolkata")
        results["open_meteo"] = {"status": "live" if "current_weather" in w else "degraded", "latency_ms": round((time.time()-start)*1000)}
    except Exception as e:
        results["open_meteo"] = {"status": "down", "error": str(e), "latency_ms": round((time.time()-start)*1000)}
    return results

# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(title="VSPE v11", version="11.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/health")
def health():
    return check_all_health()

@app.get("/health", response_class=HTMLResponse)
def health_page():
    try:
        with open("health.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Health dashboard not found</h1><p>API health: <a href='/api/health'>/api/health</a></p>")

@app.get("/api/gold")
def gold_dashboard():
    cached, hit = cache_get("gold")
    if hit:
        return make_response(cached, "yahoo", cached=True, trust="live")
    data = fetch_gold_data()
    if data and data.get("futures", {}).get("price"):
        cache_set("gold", data, CACHE_TTL["gold"])
        cross = data.get("usd_inr", {}).get("cross_check", {})
        trust = "live" if cross.get("status") != "disagree" else "cross_check_mismatch"
        return make_response(data, "yahoo", trust=trust, cross_check=cross)
    stale = cache_get_stale("gold")
    if stale:
        return make_response(stale, "yahoo", stale=True, cached=True, trust="stale")
    return make_response(None, "yahoo", trust="error", error="All gold data sources failed")

@app.get("/api/gold/sr-levels")
def sr_levels():
    gold_data = fetch_gold_data()
    if gold_data and "support_resistance" in gold_data:
        return make_response(gold_data["support_resistance"], "yahoo", trust="live")
    return make_response(None, "yahoo", trust="error", error="Could not compute S/R levels")

@app.get("/api/fx/{pair}")
def fx_rate(pair: str = "USDINR"):
    cached, hit = cache_get(f"fx_{pair}")
    if hit: return make_response(cached, "cached", cached=True, trust="live")
    pair_upper = pair.upper()
    base, target = pair_upper[:3], pair_upper[3:]
    rate, provider, cross = get_fx_crosschecked(pair_upper, base, target)
    if rate:
        data = {"rate": rate, "pair": f"{base}/{target}"}
        cache_set(f"fx_{pair}", data, CACHE_TTL["fx"])
        trust = "live" if cross.get("status") == "agree" else "single_source"
        return make_response(data, provider, trust=trust, cross_check=cross)
    stale = cache_get_stale(f"fx_{pair}")
    if stale: return make_response(stale, "cached", stale=True, cached=True, trust="stale")
    return make_response(None, None, trust="error", error="All FX sources failed")

@app.get("/api/crypto/{coin_id}")
def crypto(coin_id: str = "bitcoin"):
    cached, hit = cache_get(f"crypto_{coin_id}")
    if hit: return make_response(cached, "coingecko", cached=True, trust="live")
    data = fetch_crypto(coin_id)
    if data and not data.get("error"):
        cache_set(f"crypto_{coin_id}", data, CACHE_TTL["crypto"])
        return make_response(data, "coingecko", trust="live")
    stale = cache_get_stale(f"crypto_{coin_id}")
    if stale: return make_response(stale, "coingecko", stale=True, cached=True, trust="stale")
    return make_response(data, "coingecko", trust="error", error=data.get("error"))

@app.get("/api/news")
def news(q: str = "gold prices"):
    cached, hit = cache_get(f"news_{q}")
    if hit: return make_response(cached, "google_news_rss", cached=True, trust="live")
    data = fetch_news(q)
    if isinstance(data, list) and len(data) > 0:
        cache_set(f"news_{q}", data, CACHE_TTL["news"])
        return make_response(data, "google_news_rss", trust="live")
    stale = cache_get_stale(f"news_{q}")
    if stale: return make_response(stale, "google_news_rss", stale=True, cached=True, trust="stale")
    return make_response(data, "google_news_rss", trust="error", error="No news found")

@app.get("/api/weather")
def weather(city: str = "Kolkata"):
    cached, hit = cache_get(f"weather_{city}")
    if hit: return make_response(cached, "open_meteo", cached=True, trust="live")
    data = fetch_weather(city)
    if data and not data.get("error"):
        cache_set(f"weather_{city}", data, CACHE_TTL["weather"])
        return make_response(data, "open_meteo", trust="live")
    return make_response(data, "open_meteo", trust="error", error=data.get("error"))

@app.get("/api/search")
def search(q: str = ""):
    if not q.strip():
        return make_response(None, None, error="Please enter a query")
    q = q[:500]
    category = classify_query(q)
    if category == "financial":
        words = q.strip().split()
        if len(words) == 1 and words[0].upper() == words[0] and len(words[0]) <= 5:
            symbol = words[0]
            price, _ = yf_get_price(symbol)
            hist = yf_get_history(symbol)
            data = {"symbol": symbol, "price": price, "history": hist, "category": "financial"}
            return make_response(data, "yahoo", trust="live" if price else "error")
        data = fetch_gold_data()
        data["category"] = "financial"
        cache_set("gold", data, CACHE_TTL["gold"])
        return make_response(data, "yahoo", trust="live")
    elif category == "crypto":
        coin_map = {"btc":"bitcoin","eth":"ethereum","bitcoin":"bitcoin","ethereum":"ethereum",
                    "doge":"dogecoin","dogecoin":"dogecoin","sol":"solana","solana":"solana",
                    "xrp":"ripple","bnb":"binancecoin","shib":"shiba-inu","cardano":"cardano","ada":"cardano"}
        coin_id = "bitcoin"
        for kw, cid in coin_map.items():
            if kw in q.lower(): coin_id = cid; break
        data = fetch_crypto(coin_id)
        data["category"] = "crypto"
        return make_response(data, "coingecko", trust="live" if not data.get("error") else "error")
    elif category == "fx":
        rate, provider, cross = get_fx_crosschecked()
        data = {"rate": rate, "pair": "USD/INR", "category": "fx"}
        return make_response(data, provider, trust="live", cross_check=cross)
    elif category == "weather":
        city = q.lower()
        for kw in WEATHER_KW:
            city = city.replace(kw, "")
        city = city.strip() or "Kolkata"
        data = fetch_weather(city)
        data["category"] = "weather"
        return make_response(data, "open_meteo", trust="live" if not data.get("error") else "error")
    elif category == "news":
        news_q = q.lower()
        for kw in NEWS_KW:
            news_q = news_q.replace(kw, "")
        news_q = news_q.strip() or "India"
        data = fetch_news(news_q)
        return make_response({"articles": data, "category": "news"}, "google_news_rss", trust="live")
    else:
        cache_key = f"search_{hashlib.md5(q.encode()).hexdigest()}"
        cached, hit = cache_get(cache_key)
        if hit: return make_response(cached, "gemini", cached=True, trust="verified")
        data = search_gemini(q)
        if data and not data.get("error"):
            n_sources = len(data.get("sources", []))
            trust = "verified" if n_sources >= 2 else "single_source" if n_sources == 1 else "unverified"
            data["category"] = "general"
            cache_set(cache_key, data, CACHE_TTL["search"])
            return make_response(data, "gemini", trust=trust)
        return make_response(data, "gemini", trust="error", error=data.get("error"))

@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    try:
        with open("index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>VSPE v11</h1><p>Frontend not found. API running at <a href='/api/health'>/api/health</a></p>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
