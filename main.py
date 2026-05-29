import math, json, hashlib, time, io, csv, os, urllib.request, urllib.parse, sqlite3
from datetime import datetime, timezone
from collections import deque, Counter

FRED_KEY = os.environ.get("FRED_KEY", "617a9e4e8fa1bda0b9e0585ef518fc0c")
AV_KEY   = os.environ.get("AV_KEY",   "QQB74XYGSGW0MFLN")
TG_TOKEN = os.environ.get("TG_TOKEN", "8555960020:AAG0Znn3QWVH_zeelJdBRoiVgB2Sem8Aqzs")
TG_CHAT  = os.environ.get("TG_CHAT",  "1804751540")
DB_PATH  = os.environ.get("DB_PATH",  "/data/dll_live.db")
LOG_PATH = os.environ.get("LOG_PATH", "/data/dll_live.log")

MARKET_THRESH_NATURAL      = 0.7031
MARKET_THRESH_ORCHESTRATED = 0.6743
MARKET_P50                 = 0.6862

AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
]

def fetch(url, retries=5, base=1.0):
    import random
    for attempt in range(retries):
        try:
            req=urllib.request.Request(url,
                headers={"User-Agent":random.choice(AGENTS),
                         "Accept":"text/html,application/json,*/*"})
            with urllib.request.urlopen(req,timeout=20) as r: return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (429,503,502): time.sleep(base*(2**attempt))
            else: raise
        except: time.sleep(base*(2**attempt))
    raise Exception(f"Fetch eșuat: {url}")

def tg(msg):
    try:
        url=(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
             f"?chat_id={TG_CHAT}&text={urllib.parse.quote(str(msg))}")
        fetch(url)
    except Exception as e:
        print(f"TG error: {e}")

def log(msg):
    ts=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line=f"[{ts}] {msg}"; print(line)
    try:
        os.makedirs(os.path.dirname(LOG_PATH),exist_ok=True)
        with open(LOG_PATH,"a") as f: f.write(line+"\n")
    except: pass

def db_init(path):
    os.makedirs(os.path.dirname(path),exist_ok=True)
    con=sqlite3.connect(path)
    con.cursor().executescript("""
        CREATE TABLE IF NOT EXISTS candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT, source TEXT, ts TEXT,
            open REAL, high REAL, low REAL, close REAL,
            vol REAL, inserted TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pst ON candles(pair,source,ts);
        CREATE TABLE IF NOT EXISTS macro_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, vix REAL, us10y REAL, us2y REAL,
            yield_curve REAL, fedfunds REAL, cpi REAL,
            oil REAL, m2 REAL, hy_spread REAL,
            unemployment REAL, regime TEXT, inserted TEXT
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, pair TEXT, regime TEXT,
            market_score REAL, shannon REAL, pe REAL,
            autocorr REAL, hurst REAL,
            macro_regime TEXT, macro_adj REAL,
            final_regime TEXT, integrity INTEGER,
            market_flags TEXT, anomaly_log TEXT,
            blocked INTEGER, input_hash TEXT
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, pair TEXT, regime TEXT, reason TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_open TEXT, ts_close TEXT,
            pair TEXT, direction TEXT,
            price_open REAL, price_close REAL,
            pct_move REAL, cost_pct REAL,
            edge REAL, platform TEXT,
            signal_regime TEXT, macro_regime TEXT,
            status TEXT
        );
    """)
    con.commit()
    return con

def db_insert(con, pair, source, candles):
    now=datetime.now(timezone.utc).isoformat()
    rows=[(pair,source,c["t"],c.get("o",c["v"]),c.get("h",c["v"]),
           c.get("l",c["v"]),c["v"],c.get("vol",0),now)
          for c in candles]
    cur=con.cursor()
    cur.executemany("""INSERT OR IGNORE INTO candles
        (pair,source,ts,open,high,low,close,vol,inserted)
        VALUES (?,?,?,?,?,?,?,?,?)""",rows)
    con.commit()
    return cur.rowcount

def db_load(con, pair, source, limit=120):
    cur=con.cursor()
    cur.execute("""SELECT ts,open,high,low,close,vol FROM candles
                   WHERE pair=? AND source=?
                   ORDER BY ts DESC LIMIT ?""",(pair,source,limit))
    rows=cur.fetchall()
    return [{"t":r[0],"o":r[1],"h":r[2],"l":r[3],
             "v":r[4],"vol":r[5],"spread":round(r[2]-r[3],8)}
            for r in reversed(rows)]

def db_save_macro(con, macro):
    now=datetime.now(timezone.utc).isoformat()
    con.cursor().execute("""INSERT INTO macro_context
        (ts,vix,us10y,us2y,yield_curve,fedfunds,cpi,
         oil,m2,hy_spread,unemployment,regime,inserted)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (now,macro.get("VIX"),macro.get("US10Y"),macro.get("US2Y"),
         macro.get("YieldCurve"),macro.get("FedFunds"),macro.get("CPI"),
         macro.get("Oil_WTI"),macro.get("M2"),macro.get("HY_Spread"),
         macro.get("Unemployment"),macro.get("regime"),now))
    con.commit()

def db_open_trade(con, pair, direction, price, regime, macro_regime):
    con.cursor().execute("""INSERT INTO paper_trades
        (ts_open,pair,direction,price_open,cost_pct,
         signal_regime,macro_regime,platform,status)
        VALUES (?,?,?,?,?,?,?,'paper','OPEN')""",
        (datetime.now(timezone.utc).isoformat(),
         pair,direction,price,0.0028,regime,macro_regime))
    con.commit()
    return con.execute("SELECT last_insert_rowid()").fetchone()[0]

def db_close_trade(con, tid, price_close):
    cur=con.cursor()
    cur.execute("SELECT price_open,cost_pct,direction FROM paper_trades WHERE id=?",
                (tid,))
    row=cur.fetchone()
    if not row: return None
    price_open,cost_pct,direction=row
    pct=((price_close-price_open)/price_open*100
         if direction=="LONG"
         else (price_open-price_close)/price_open*100)
    edge=round(pct-cost_pct*100,4)
    cur.execute("""UPDATE paper_trades SET
        ts_close=?,price_close=?,pct_move=?,edge=?,status=?
        WHERE id=?""",
        (datetime.now(timezone.utc).isoformat(),
         price_close,round(pct,4),edge,
         "WIN" if edge>0 else "LOSS",tid))
    con.commit()
    return edge

def permutation_entropy(w, m=4):
    n=len(w)
    if n<m+1: return 1.0
    patterns=[tuple(sorted(range(m),key=lambda x:w[i:i+m][x]))
              for i in range(n-m+1)]
    counts=Counter(patterns); total=sum(counts.values())
    probs=[c/total for c in counts.values()]
    pe=-sum(p*math.log(p) for p in probs if p>0)
    max_pe=math.log(math.factorial(m))
    return round(pe/max_pe if max_pe>0 else 0.0,4)

def pe_score(series, ws=60):
    vals=[p["v"] for p in series]; results={}
    for m in (3,4,5):
        scores=[permutation_entropy(vals[i:i+ws],m=m)
                for i in range(len(vals)-ws+1)]
        if scores: results[m]=round(sum(scores)/len(scores),4)
    if not results: return 1.0
    return round(results.get(3,1)*0.25+results.get(4,1)*0.50+
                 results.get(5,1)*0.25,4)

def shannon_score(series, ws=60, bins=20):
    vals=[p["v"] for p in series]; scores=[]
    for i in range(len(vals)-ws+1):
        d=[vals[i+j+1]-vals[i+j] for j in range(ws-1)]
        mn,mx=min(d),max(d)
        if mx==mn: scores.append(0.0); continue
        bs=(mx-mn)/bins
        c=Counter(min(int((x-mn)/bs),bins-1) for x in d)
        tot=sum(c.values()); p=[v/tot for v in c.values()]
        scores.append(-sum(x*math.log2(x) for x in p if x>0)/math.log2(bins))
    return round(sum(scores)/len(scores),4) if scores else 0.0

def autocorr_score(series, ws=60):
    vals=[p["v"] for p in series]; A=[]
    for i in range(len(vals)-ws+1):
        w=vals[i:i+ws]; mean=sum(w)/len(w)
        num=sum((w[j]-mean)*(w[j+1]-mean) for j in
