import math, json, hashlib, time, io, csv, os, urllib.request, urllib.parse, sqlite3
from datetime import datetime, timezone
from collections import deque, Counter

FRED_KEY = os.environ.get("FRED_KEY", "617a9e4e8fa1bda0b9e0585ef518fc0c")
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
    raise Exception(f"Fetch failed: {url}")

def tg(msg):
    try:
        url=(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
             f"?chat_id={TG_CHAT}&text={urllib.parse.quote(str(msg)[:4000])}")
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
        CREATE TABLE IF NOT EXISTS triangulation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, pair TEXT, sources TEXT, prices TEXT,
            max_div_pct REAL, status TEXT
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, pair TEXT, regime TEXT,
            market_score REAL, shannon REAL, pe REAL,
            autocorr REAL, hurst REAL,
            macro_regime TEXT, final_regime TEXT,
            integrity INTEGER, tri_status TEXT,
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

def db_save_tri(con, pair, sources, prices, max_div, status):
    con.cursor().execute("""INSERT INTO triangulation_log
        (ts,pair,sources,prices,max_div_pct,status)
        VALUES (?,?,?,?,?,?)""",
        (datetime.now(timezone.utc).isoformat(),
         pair,json.dumps(sources),json.dumps(prices),max_div,status))
    con.commit()

def db_open_trade(con, pair, direction, price, regime, macro_regime):
    con.cursor().execute("""INSERT INTO paper_trades
        (ts_open,pair,direction,price_open,cost_pct,
         signal_regime,macro_regime,platform,status)
        VALUES (?,?,?,?,?,?,?,"paper","OPEN")""",
        (datetime.now(timezone.utc).isoformat(),
         pair,direction,price,0.0028,regime,macro_regime))
    con.commit()
    return con.execute("SELECT last_insert_rowid()").fetchone()[0]

def db_close_trade(con, tid, price_close):
    cur=con.cursor()
    cur.execute("SELECT price_open,cost_pct,direction FROM paper_trades WHERE id=?",(tid,))
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
        num=sum((w[j]-mean)*(w[j+1]-mean) for j in range(len(w)-1))
        den=sum((x-mean)**2 for x in w)
        A.append(abs(num/den) if den else 0)
    return round(sum(A)/len(A),4) if A else 0.5

def hurst_exp(series):
    vals=[p["v"] for p in series]; n=len(vals)
    if n<40: return 0.5
    rs_vals=[]
    for lag in range(2,min(20,n//2)):
        segs=[vals[i:i+lag] for i in range(0,n-lag,lag)]
        rs_seg=[]
        for seg in segs:
            mean=sum(seg)/len(seg)
            devs=[sum(seg[:i+1])-mean*(i+1) for i in range(len(seg))]
            r=max(devs)-min(devs)
            s=(sum((x-mean)**2 for x in seg)/len(seg))**0.5
            if s>0: rs_seg.append(r/s)
        if rs_seg: rs_vals.append((lag,sum(rs_seg)/len(rs_seg)))
    if len(rs_vals)<2: return 0.5
    x=[math.log(r[0]) for r in rs_vals]
    y=[math.log(r[1]) for r in rs_vals]
    xm=sum(x)/len(x); ym=sum(y)/len(y)
    num=sum((x[i]-xm)*(y[i]-ym) for i in range(len(x)))
    den=sum((x[i]-xm)**2 for i in range(len(x)))
    return round(num/den if den else 0.5,4)

def market_composite(series, ws=60):
    sh=shannon_score(series,ws)
    pe=pe_score(series,ws)
    ac=autocorr_score(series,ws)
    h=hurst_exp(series)
    return round(0.40*sh+0.40*pe+0.20*(1-ac),4),sh,pe,ac,h

def get_macro_latest(con):
    macro={}
    for name in ["VIX","US10Y","US2Y","YieldCurve","FedFunds",
                 "CPI","Oil_WTI","M2","HY_Spread","Unemployment"]:
        cur=con.cursor()
        cur.execute("""SELECT close FROM candles WHERE pair=? AND source="fred_macro"
                       ORDER BY ts DESC LIMIT 1""",(name,))
        row=cur.fetchone()
        if row: macro[name]=row[0]
    return macro

def classify_macro(macro):
    vix=macro.get("VIX",20); yc=macro.get("YieldCurve",0)
    hy=macro.get("HY_Spread",400); ff=macro.get("FedFunds",2)
    s=0
    if vix>40: s-=3
    elif vix>30: s-=2
    elif vix>20: s-=1
    elif vix<15: s+=1
    if yc<-0.5: s-=2
    elif yc<0: s-=1
    elif yc>1.0: s+=1
    if hy>700: s-=2
    elif hy>500: s-=1
    elif hy<300: s+=1
    if ff>4: s-=1
    if s<=-4: r="RISK_OFF_EXTREME"
    elif s<=-2: r="RISK_OFF"
    elif s<=1: r="NEUTRAL"
    elif s<=3: r="RISK_ON"
    else: r="RISK_ON_EXTREME"
    return r,s

def macro_adj(score, regime):
    adj={"RISK_OFF_EXTREME":+0.04,"RISK_OFF":+0.02,"NEUTRAL":0.0,
         "RISK_ON":-0.02,"RISK_ON_EXTREME":-0.04}
    return round(score+adj.get(regime,0),4)

def get_regime(score):
    if score>MARKET_THRESH_NATURAL: return "NATURAL"
    elif score<MARKET_THRESH_ORCHESTRATED: return "ORCHESTRATED"
    else: return "UNCERTAIN"

def dll_v14(primary, macro_regime="NEUTRAL"):
    score,sh,pe,ac,h=market_composite(primary)
    adj=macro_adj(score,macro_regime)
    regime=get_regime(adj)
    flags=[]
    vols=[p["vol"] for p in primary]
    if any(v>0 for v in vols):
        mean_v=sum(vols)/len(vols)
        std_v=(sum((x-mean_v)**2 for x in vols)/len(vols))**0.5
        cv_v=std_v/mean_v if mean_v else 0
        if mean_v>3000 and cv_v<0.05:
            flags.append(f"WASH:{round(mean_v,0)}"); regime="ORCHESTRATED"
    h_label="TRENDING" if h>0.6 else "RANDOM" if h>0.4 else "MEAN_REV"
    return {"timestamp":datetime.now(timezone.utc).isoformat(),
            "input_hash":hashlib.sha256(
                json.dumps([p["v"] for p in primary[:10]]).encode()
            ).hexdigest()[:16],
            "regime":regime,"market_score":score,"adj_score":adj,
            "shannon":sh,"pe":pe,"autocorr":ac,"hurst":h,
            "hurst_label":h_label,"macro_regime":macro_regime,
            "integrity_flag":True,"market_flags":flags or [],
            "anomaly_log":(f"sh={sh} pe={pe} ac={ac} H={h}[{h_label}] "
                          f"mkt={score} adj={adj} macro={macro_regime}")}

def triangulate_all(prices, tol=0.5):
    if len(prices)<2: return "SINGLE",{},[],False
    vals=list(prices.values()); mean_p=sum(vals)/len(vals)
    divs={s:round(abs(p-mean_p)/mean_p*100,4) for s,p in prices.items()}
    conflicts=[]
    srcs=list(prices.keys())
    for i in range(len(srcs)):
        for j in range(i+1,len(srcs)):
            s1,s2=srcs[i],srcs[j]
            d=abs(prices[s1]-prices[s2])/max(prices[s1],prices[s2])*100
            if d>tol: conflicts.append(f"{s1}v{s2}:{round(d,3)}%")
    fail=max(divs.values())>tol if divs else False
    return ("FAIL" if fail else "OK"),divs,conflicts,fail

class PairGovernor:
    def __init__(self,pair,thr=3):
        self.pair=pair; self.thr=thr
        self.history=deque(maxlen=50)
        self.blocked=False; self.reason=""; self.consec=0
        self.open_trade_id=None
    def evaluate(self,snap):
        self.history.append(snap)
        self.consec=self.consec+1 if snap["regime"]=="ORCHESTRATED" else 0
        if self.consec>=self.thr and not self.blocked:
            self.blocked=True; self.reason=f"{self.pair}:{self.consec}xORC"
        return {**snap,"consec":self.consec,"blocked":self.blocked,
                "action":"AWAITING" if self.blocked else "MONITOR"}
    def override(self):
        self.blocked=False; self.consec=0; self.reason=""

def load_fred(sid,key=FRED_KEY):
    url=(f"https://api.stlouisfed.org/fred/series/observations"
         f"?series_id={sid}&file_type=json&api_key={key}")
    data=json.loads(fetch(url)); out=[]
    for obs in data.get("observations",[]):
        try:
            v=float(obs["value"])
            out.append({"t":obs["date"],"v":v,"o":v,"h":v,"l":v,"vol":0})
        except: continue
    return out

def load_kraken(pair,interval=60,limit=150):
    url=(f"https://api.kraken.com/0/public/OHLC"
         f"?pair={pair}&interval={interval}&count={limit}")
    data=json.loads(fetch(url))
    if data.get("error") and data["error"]: raise Exception(data["error"])
    result=data["result"]; key=[k for k in result if k!="last"][0]
    return [{"t":datetime.fromtimestamp(c[0],tz=timezone.utc).isoformat(),
             "o":float(c[1]),"h":float(c[2]),"l":float(c[3]),
             "v":float(c[4]),"vol":float(c[6])} for c in result[key]]

def load_cg(coin_id):
    url=f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
    return json.loads(fetch(url)).get(coin_id,{}).get("usd")

def load_bybit(symbol):
    url=f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol}"
    items=json.loads(fetch(url)).get("result",{}).get("list",[])
    return float(items[0]["lastPrice"]) if items else None

def load_coincap(coin_id):
    p=json.loads(fetch(f"https://api.coincap.io/v2/assets/{coin_id}")).get("data",{}).get("priceUsd")
    return float(p) if p else None

def load_er(base,target):
    rates=json.loads(fetch(f"https://open.er-api.com/v6/latest/{base}")).get("rates",{})
    return rates.get(target)

def load_fg():
    data=json.loads(fetch("https://api.alternative.me/fng/?limit=1"))
    return int(data["data"][0]["value"]),data["data"][0]["value_classification"]

def _est_move(series):
    vals=[p["v"] for p in series[-20:]]; mean=sum(vals)/len(vals)
    std=(sum((x-mean)**2 for x in vals)/len(vals))**0.5
    return round(std/mean*100*5**0.5,4)

def _cat(regime,blocked,pair,nat,orc,unc,blk):
    if regime=="NATURAL": nat.append(pair)
    elif regime=="ORCHESTRATED": orc.append(pair)
    else: unc.append(pair)
    if blocked: blk.append(pair)

CRYPTO={
    "XBTUSD":{"kraken":"XBTUSD","cg":"bitcoin","bybit":"BTCUSDT","cc":"bitcoin"},
    "ETHUSD":{"kraken":"ETHUSD","cg":"ethereum","bybit":"ETHUSDT","cc":"ethereum"},
    "SOLUSD":{"kraken":"SOLUSD","cg":"solana","bybit":"SOLUSDT","cc":"solana"},
    "ADAUSD":{"kraken":"ADAUSD","cg":"cardano","bybit":"ADAUSDT","cc":"cardano"},
    "XRPUSD":{"kraken":"XRPUSD","cg":"ripple","bybit":"XRPUSDT","cc":"xrp"},
    "LINKUSD":{"kraken":"LINKUSD","cg":"chainlink","bybit":"LINKUSDT","cc":"chainlink"},
    "LTCUSD":{"kraken":"LTCUSD","cg":"litecoin","bybit":"LTCUSDT","cc":"litecoin"},
    "ATOMUSD":{"kraken":"ATOMUSD","cg":"cosmos","bybit":"ATOMUSDT","cc":"cosmos"},
    "AVAXUSD":{"kraken":"AVAXUSD","cg":"avalanche-2","bybit":"AVAXUSDT","cc":"avalanche"},
    "UNIUSD":{"kraken":"UNIUSD","cg":"uniswap","bybit":"UNIUSDT","cc":"uniswap"},
}
FOREX={
    "EURUSD":{"fred":"DEXUSEU","base":"EUR","target":"USD"},
    "USDJPY":{"fred":"DEXJPUS","base":"USD","target":"JPY"},
    "GBPUSD":{"fred":"DEXUSUK","base":"GBP","target":"USD"},
    "AUDUSD":{"fred":"DEXUSAL","base":"AUD","target":"USD"},
    "USDCAD":{"fred":"DEXCAUS","base":"USD","target":"CAD"},
    "USDCHF":{"fred":"DEXSZUS","base":"USD","target":"CHF"},
}
MACRO={
    "VIX":"VIXCLS","US10Y":"DGS10","US2Y":"DGS2",
    "YieldCurve":"T10Y2Y","FedFunds":"FEDFUNDS",
    "CPI":"CPIAUCSL","Oil_WTI":"DCOILWTICO",
    "M2":"M2SL","HY_Spread":"BAMLH0A0HYM2","Unemployment":"UNRATE"
}

def live_loop(con,interval_min=60,fetch_every_sec=3600,window=120,max_cycles=None):
    governors={p:PairGovernor(p) for p in list(CRYPTO)+list(FOREX)}
    cycle=0; mcycle=0
    log("START v14"); tg("DLL v14 pornit — 4 surse crypto, 2 surse forex")
    while True:
        cycle+=1
        if max_cycles and cycle>max_cycles: log("STOP"); break
        log(f"Ciclu #{cycle}"); t0=time.time(); mcycle+=1
        if mcycle==1 or mcycle%24==0:
            for n,s in MACRO.items():
                try: db_insert(con,n,"fred_macro",load_fred(s)); time.sleep(0.4)
                except Exception as e: log(f"macro {n}:{e}")
            for p,m in FOREX.items():
                try: db_insert(con,p,"fred",load_fred(m["fred"])); time.sleep(0.4)
                except Exception as e: log(f"forex {p}:{e}")
        mv=get_macro_latest(con); mr,ms=classify_macro(mv)
        log(f"MACRO:{mr}(s={ms}) VIX={mv.get('VIX','?')} YC={mv.get('YieldCurve','?')}")
        try: fg,fgl=load_fg()
        except: fg=50; fgl="Unknown"
        nat=[]; orc=[]; unc=[]; blk=[]
        for pair,m in CRYPTO.items():
            gov=governors[pair]; prices={}
            try:
                c=load_kraken(m["kraken"],interval_min,window+30)
                db_insert(con,pair,"kraken",c)
                if c: prices["kraken"]=c[-1]["v"]
                time.sleep(0.2)
                try:
                    p=load_cg(m["cg"])
                    if p: prices["coingecko"]=p
                    time.sleep(1.0)
                except: pass
                try:
                    p=load_bybit(m["bybit"])
                    if p: prices["bybit"]=p
                    time.sleep(0.2)
                except: pass
                try:
                    p=load_coincap(m["cc"])
                    if p: prices["coincap"]=p
                    time.sleep(0.2)
                except: pass
                ts,divs,conflicts,tf=triangulate_all(prices,0.5)
                md=max(divs.values()) if divs else 0
                db_save_tri(con,pair,list(prices.keys()),
                            {s:round(p,4) for s,p in prices.items()},md,ts)
                series=db_load(con,pair,"kraken",window)
                if len(series)<window: continue
                snap=dll_v14(series,mr)
                if tf:
                    snap["integrity_flag"]=False
                    snap["market_flags"].append(f"TRI_FAIL:{md:.3f}%")
                    snap["regime"]="ORCHESTRATED"
                dec=gov.evaluate(snap)
                em=_est_move(series); edge=round(em-0.28,4)
                if dec["regime"]=="ORCHESTRATED" and edge>0 and not gov.blocked:
                    if not gov.open_trade_id:
                        tid=db_open_trade(con,pair,"SHORT",prices.get("kraken",0),dec["regime"],mr)
                        gov.open_trade_id=tid
                        log(f"SHORT {pair} @ {prices.get('kraken',0):.2f} edge={edge}%")
                elif dec["regime"]=="NATURAL" and gov.open_trade_id:
                    e=db_close_trade(con,gov.open_trade_id,prices.get("kraken",0))
                    r="WIN" if e and e>0 else "LOSS"
                    log(f"CLOSE {pair} edge={e}% {r}")
                    tg(f"{r} {pair} Edge:{e}% Macro:{mr}")
                    gov.open_trade_id=None
                if dec["regime"]=="ORCHESTRATED":
                    tg(f"ORC {pair} score={snap["market_score"]} PE={snap["pe"]} src={len(prices)} div={md:.3f}% Macro:{mr} F&G:{fg}")
                icon="X" if dec["blocked"] else ("R" if dec["regime"]=="ORCHESTRATED" else ("G" if dec["regime"]=="NATURAL" else "Y"))
                log(f"[{icon}] {pair} {dec["regime"]} mkt={snap["market_score"]} pe={snap["pe"]} H={snap["hurst"]} src={len(prices)} div={md:.3f}%")
                _cat(dec["regime"],gov.blocked,pair,nat,orc,unc,blk)
            except Exception as e:
                log(f"ERR {pair}:{e}"); time.sleep(2)
        for pair,m in FOREX.items():
            gov=governors[pair]; prices={}
            try:
                series=db_load(con,pair,"fred",window)
                try:
                    p=load_er(m["base"],m["target"])
                    if p: prices["exchangerate"]=p
                    if series: prices["fred"]=series[-1]["v"]
                except: pass
                ts,divs,conflicts,tf=triangulate_all(prices,0.3)
                md=max(divs.values()) if divs else 0
                if series: db_save_tri(con,pair,list(prices.keys()),{s:round(p,6) for s,p in prices.items()},md,ts)
                if len(series)<window: continue
                snap=dll_v14(series,mr); dec=gov.evaluate(snap)
                if dec["regime"]=="ORCHESTRATED":
                    tg(f"ORC FOREX {pair} score={snap["market_score"]} div={md:.4f}%")
                log(f"[{'R' if dec['regime']=='ORCHESTRATED' else 'G' if dec['regime']=='NATURAL' else 'Y'}] {pair} {dec['regime']} mkt={snap['market_score']} div={md:.4f}%")
                _cat(dec["regime"],gov.blocked,pair,nat,orc,unc,blk)
            except Exception as e:
                log(f"ERR {pair}:{e}")
        log(f"SUMMARY NAT={len(nat)} ORC={len(orc)} UNC={len(unc)} BLOC={len(blk)}")
        tg(f"DLL v14 Ciclu#{cycle}\nMacro:{mr}\nNAT:{','.join(nat) or '-'}\nORC:{','.join(orc) or '-'}\nF&G:{fg}({fgl})")
        sleep_t=max(0,fetch_every_sec-(time.time()-t0))
        log(f"Next {sleep_t:.0f}s"); time.sleep(sleep_t)

os.makedirs("/data",exist_ok=True)
con=db_init(DB_PATH)
print("DLL v14 START")
tg("DLL v14 OK")
live_loop(con,interval_min=60,fetch_every_sec=3600,window=120,max_cycles=None)
