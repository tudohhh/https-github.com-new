import sqlite3, math, json, time, os, urllib.request, urllib.parse, hashlib
from datetime import datetime, timezone
from collections import deque, Counter

FRED_KEY = os.environ.get("FRED_KEY", "617a9e4e8fa1bda0b9e0585ef518fc0c")
TG_TOKEN = os.environ.get("TG_TOKEN", "8555960020:AAG0Znn3QWVH_zeelJdBRoiVgB2Sem8Aqzs")
TG_CHAT  = os.environ.get("TG_CHAT",  "1804751540")
DB_PATH  = os.environ.get("DB_PATH",  "/data/dll_live.db")
LOG_PATH = os.environ.get("LOG_PATH", "/data/dll_live.log")
THRESH_NATURAL=0.7031; THRESH_ORCHESTRATED=0.6743; MARKET_P50=0.6862; COST_PCT=0.0028
AGENTS=["Mozilla/5.0 (Windows NT 10.0)","Mozilla/5.0 (Macintosh)","Mozilla/5.0 (X11)"]


def fetch(url,retries=5,base=1.0):
    import random
    for attempt in range(retries):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":random.choice(AGENTS),"Accept":"application/json,*/*"})
            with urllib.request.urlopen(req,timeout=20) as r: return r.read()
        except urllib.error.HTTPError as e:
            if e.code in(429,503,502): time.sleep(base*(2**attempt))
            else: raise
        except: time.sleep(base*(2**attempt))
    raise Exception(f"Fetch failed:{url}")

def tg(msg):
    try:
        url=f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage?chat_id={TG_CHAT}&text={urllib.parse.quote(str(msg)[:4000])}"
        fetch(url)
    except: pass

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
        CREATE TABLE IF NOT EXISTS candles(id INTEGER PRIMARY KEY AUTOINCREMENT,pair TEXT,source TEXT,ts TEXT,open REAL,high REAL,low REAL,close REAL,vol REAL,inserted TEXT);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pst ON candles(pair,source,ts);
        CREATE TABLE IF NOT EXISTS macro_context(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,vix REAL,us10y REAL,us2y REAL,yield_curve REAL,fedfunds REAL,cpi REAL,oil REAL,m2 REAL,hy_spread REAL,unemployment REAL,regime TEXT,inserted TEXT);
        CREATE TABLE IF NOT EXISTS triangulation_log(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,pair TEXT,sources TEXT,prices TEXT,max_div_pct REAL,status TEXT);
        CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,pair TEXT,regime TEXT,market_score REAL,shannon REAL,pe REAL,autocorr REAL,hurst REAL,direction TEXT,direction_reason TEXT,macro_regime TEXT,final_regime TEXT,integrity INTEGER,tri_status TEXT,market_flags TEXT,anomaly_log TEXT,blocked INTEGER,input_hash TEXT);
        CREATE TABLE IF NOT EXISTS paper_trades(id INTEGER PRIMARY KEY AUTOINCREMENT,ts_open TEXT,ts_close TEXT,pair TEXT,direction TEXT,price_open REAL,price_close REAL,pct_move REAL,cost_pct REAL,edge REAL,platform TEXT,signal_regime TEXT,macro_regime TEXT,direction_reason TEXT,status TEXT);
        CREATE TABLE IF NOT EXISTS transfer_entropy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,source_pair TEXT,target_pair TEXT,te_value REAL,lag INTEGER,significant INTEGER);
    """)
    con.commit(); return con

def db_insert(con,pair,source,candles):
    now=datetime.now(timezone.utc).isoformat()
    rows=[(pair,source,c["t"],c.get("o",c["v"]),c.get("h",c["v"]),c.get("l",c["v"]),c["v"],c.get("vol",0),now) for c in candles]
    cur=con.cursor(); cur.executemany("INSERT OR IGNORE INTO candles(pair,source,ts,open,high,low,close,vol,inserted) VALUES(?,?,?,?,?,?,?,?,?)",rows); con.commit(); return cur.rowcount

def db_load(con,pair,source,limit=120):
    cur=con.cursor(); cur.execute("SELECT ts,open,high,low,close,vol FROM candles WHERE pair=? AND source=? ORDER BY ts DESC LIMIT ?",(pair,source,limit)); rows=cur.fetchall()
    return [{"t":r[0],"o":r[1],"h":r[2],"l":r[3],"v":r[4],"vol":r[5],"spread":round(r[2]-r[3],8)} for r in reversed(rows)]

def db_save_tri(con,pair,sources,prices,max_div,status):
    con.cursor().execute("INSERT INTO triangulation_log(ts,pair,sources,prices,max_div_pct,status) VALUES(?,?,?,?,?,?)",(datetime.now(timezone.utc).isoformat(),pair,json.dumps(sources),json.dumps(prices),max_div,status)); con.commit()

def db_open_trade(con,pair,direction,price,regime,macro_regime,reason):
    con.cursor().execute("INSERT INTO paper_trades(ts_open,pair,direction,price_open,cost_pct,signal_regime,macro_regime,direction_reason,platform,status) VALUES(?,?,?,?,?,?,?,?,'paper','OPEN')",(datetime.now(timezone.utc).isoformat(),pair,direction,price,COST_PCT,regime,macro_regime,reason)); con.commit()
    return con.execute("SELECT last_insert_rowid()").fetchone()[0]

def db_close_trade(con,tid,price_close):
    cur=con.cursor(); cur.execute("SELECT price_open,cost_pct,direction FROM paper_trades WHERE id=?",(tid,)); row=cur.fetchone()
    if not row: return None
    price_open,cost_pct,direction=row
    pct=((price_close-price_open)/price_open*100 if direction=="LONG" else (price_open-price_close)/price_open*100)
    edge=round(pct-cost_pct*100,4)
    cur.execute("UPDATE paper_trades SET ts_close=?,price_close=?,pct_move=?,edge=?,status=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),price_close,round(pct,4),edge,"WIN" if edge>0 else "LOSS",tid)); con.commit(); return edge

def db_save_te(con,src,tgt,te_val,lag,significant):
    con.cursor().execute("INSERT INTO transfer_entropy_log(ts,source_pair,target_pair,te_value,lag,significant) VALUES(?,?,?,?,?,?)",(datetime.now(timezone.utc).isoformat(),src,tgt,te_val,lag,int(significant))); con.commit()


def permutation_entropy(w,m=4):
    n=len(w)
    if n<m+1: return 1.0
    patterns=[tuple(sorted(range(m),key=lambda x:w[i:i+m][x])) for i in range(n-m+1)]
    counts=Counter(patterns); total=sum(counts.values()); probs=[c/total for c in counts.values()]
    pe=-sum(p*math.log(p) for p in probs if p>0); max_pe=math.log(math.factorial(m))
    return round(pe/max_pe if max_pe>0 else 0.0,4)

def pe_score(vals,ws=60):
    results={}
    for m in(3,4,5):
        scores=[permutation_entropy(vals[i:i+ws],m=m) for i in range(len(vals)-ws+1)]
        if scores: results[m]=round(sum(scores)/len(scores),4)
    if not results: return 1.0
    return round(results.get(3,1)*0.25+results.get(4,1)*0.50+results.get(5,1)*0.25,4)

def shannon_score(vals,ws=60,bins=20):
    scores=[]
    for i in range(len(vals)-ws+1):
        d=[vals[i+j+1]-vals[i+j] for j in range(ws-1)]; mn,mx=min(d),max(d)
        if mx==mn: scores.append(0.0); continue
        bs=(mx-mn)/bins; c=Counter(min(int((x-mn)/bs),bins-1) for x in d)
        tot=sum(c.values()); p=[v/tot for v in c.values()]
        scores.append(-sum(x*math.log2(x) for x in p if x>0)/math.log2(bins))
    return round(sum(scores)/len(scores),4) if scores else 0.0

def autocorr_score(vals,ws=60):
    A=[]
    for i in range(len(vals)-ws+1):
        w=vals[i:i+ws]; mean=sum(w)/len(w)
        num=sum((w[j]-mean)*(w[j+1]-mean) for j in range(len(w)-1))
        den=sum((x-mean)**2 for x in w); A.append(abs(num/den) if den else 0)
    return round(sum(A)/len(A),4) if A else 0.5

def hurst_exp(vals):
    n=len(vals)
    if n<40: return 0.5
    rs_vals=[]
    for lag in range(2,min(20,n//2)):
        segs=[vals[i:i+lag] for i in range(0,n-lag,lag)]; rs_seg=[]
        for seg in segs:
            mean=sum(seg)/len(seg); devs=[sum(seg[:i+1])-mean*(i+1) for i in range(len(seg))]
            r=max(devs)-min(devs); s=(sum((x-mean)**2 for x in seg)/len(seg))**0.5
            if s>0: rs_seg.append(r/s)
        if rs_seg: rs_vals.append((lag,sum(rs_seg)/len(rs_seg)))
    if len(rs_vals)<2: return 0.5
    x=[math.log(r[0]) for r in rs_vals]; y=[math.log(r[1]) for r in rs_vals]
    xm=sum(x)/len(x); ym=sum(y)/len(y)
    num=sum((x[i]-xm)*(y[i]-ym) for i in range(len(x))); den=sum((x[i]-xm)**2 for i in range(len(x)))
    return round(num/den if den else 0.5,4)

def moving_average(vals,n=20):
    if len(vals)<n: return vals[-1]
    return sum(vals[-n:])/n

def ma_slope(vals,n=20,lookback=5):
    if len(vals)<n+lookback: return 0.0
    ma_now=sum(vals[-n:])/n; ma_old=sum(vals[-(n+lookback):-lookback])/n
    return round((ma_now-ma_old)/ma_old*100,4)

def market_composite(series,ws=60):
    vals=[p["v"] for p in series]
    sh=shannon_score(vals,ws); pe=pe_score(vals,ws); ac=autocorr_score(vals,ws); h=hurst_exp(vals)
    return round(0.40*sh+0.40*pe+0.20*(1-ac),4),sh,pe,ac,h


def discretize(vals,bins=5):
    mn,mx=min(vals),max(vals)
    if mx==mn: return [0]*len(vals)
    bs=(mx-mn)/bins
    return [min(int((v-mn)/bs),bins-1) for v in vals]

def transfer_entropy(source_vals,target_vals,lag=3,bins=5):
    n=len(source_vals)
    if n<lag+10 or len(target_vals)<lag+10: return 0.0
    xs=discretize(source_vals,bins); xt=discretize(target_vals,bins)
    joint_xy=Counter(); joint_xyz=Counter(); marg_x=Counter(); joint_xz=Counter()
    for i in range(lag,n-1):
        xt_now=xt[i]; xt_next=xt[i+1]; xs_lag=xs[i-lag]
        joint_xy[(xt_now,xt_next)]+=1; joint_xyz[(xt_now,xt_next,xs_lag)]+=1
        marg_x[xt_now]+=1; joint_xz[(xt_now,xs_lag)]+=1
    total=sum(joint_xyz.values())
    if total==0: return 0.0
    te=0.0
    for (x,y,z),cnt in joint_xyz.items():
        p_xyz=cnt/total; p_xy=joint_xy.get((x,y),0)/total
        p_xz=joint_xz.get((x,z),0)/total; p_x=marg_x.get(x,0)/total
        if p_xyz>0 and p_xy>0 and p_xz>0 and p_x>0:
            te+=p_xyz*math.log2(p_xyz*p_x/(p_xy*p_xz))
    return round(te,6)

def determine_direction(series,macro_vals,fg_val,hurst,asset_type="crypto"):
    vals=[p["v"] for p in series]; ma20=moving_average(vals,20)
    price_now=vals[-1]; slope=ma_slope(vals,20,5); h_trending=hurst>0.75
    if asset_type=="crypto":
        if price_now>ma20*1.01 and slope>0.05 and (fg_val<25 or fg_val>70) and h_trending:
            return "LONG","price>MA20+slope_up+FG_extreme+trending"
        elif price_now<ma20*0.99 and slope<-0.05 and (fg_val<25 or fg_val>70) and h_trending:
            return "SHORT","price<MA20+slope_down+FG_extreme+trending"
        else:
            return "SKIP","context_ambiguous"
    elif asset_type=="forex":
        yc=macro_vals.get("YieldCurve",0); fedfunds=macro_vals.get("FedFunds",2); vix=macro_vals.get("VIX",20)
        if vix>30 or yc<-0.3: return "SKIP","systemic_risk"
        usd_strong=(fedfunds>4 and yc>0.2); usd_weak=(fedfunds<2 or yc<0)
        pair_name=series[0].get("src","") if series else ""
        if usd_strong:
            return ("LONG","usd_strong") if "USD"==pair_name[:3] else ("SHORT","usd_strong")
        elif usd_weak:
            return ("SHORT","usd_weak") if "USD"==pair_name[:3] else ("LONG","usd_weak")
        else: return "SKIP","usd_neutral"
    return "SKIP","no_context"


def get_macro_latest(con):
    macro={}
    for name in["VIX","US10Y","US2Y","YieldCurve","FedFunds","CPI","Oil_WTI","M2","HY_Spread","Unemployment"]:
        cur=con.cursor(); cur.execute("SELECT close FROM candles WHERE pair=? AND source='fred_macro' ORDER BY ts DESC LIMIT 1",(name,)); row=cur.fetchone()
        if row: macro[name]=row[0]
    return macro

def classify_macro(macro):
    vix=macro.get("VIX",20); yc=macro.get("YieldCurve",0); hy=macro.get("HY_Spread",400); ff=macro.get("FedFunds",2)
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

def macro_adj(score,regime):
    adj={"RISK_OFF_EXTREME":+0.04,"RISK_OFF":+0.02,"NEUTRAL":0.0,"RISK_ON":-0.02,"RISK_ON_EXTREME":-0.04}
    return round(score+adj.get(regime,0),4)

def get_regime(score):
    if score>THRESH_NATURAL: return "NATURAL"
    elif score<THRESH_ORCHESTRATED: return "ORCHESTRATED"
    else: return "UNCERTAIN"

def dll_v16(primary,macro_regime="NEUTRAL",macro_vals=None,fg_val=50,asset_type="crypto"):
    macro_vals=macro_vals or {}
    score,sh,pe,ac,h=market_composite(primary)
    adj=macro_adj(score,macro_regime); regime=get_regime(adj)
    direction,dir_reason="SKIP","no_signal"
    if regime=="ORCHESTRATED":
        direction,dir_reason=determine_direction(primary,macro_vals,fg_val,h,asset_type)
    flags=[]; vols=[p["vol"] for p in primary]
    if any(v>0 for v in vols):
        mean_v=sum(vols)/len(vols); std_v=(sum((x-mean_v)**2 for x in vols)/len(vols))**0.5; cv_v=std_v/mean_v if mean_v else 0
        if mean_v>3000 and cv_v<0.05: flags.append(f"WASH:{round(mean_v,0)}"); regime="ORCHESTRATED"
    h_label="TRENDING" if h>0.6 else "RANDOM" if h>0.4 else "MEAN_REV"
    vals=[p["v"] for p in primary]; ma20=moving_average(vals,20); slope=ma_slope(vals,20,5)
    return {"timestamp":datetime.now(timezone.utc).isoformat(),
            "input_hash":hashlib.sha256(json.dumps([p["v"] for p in primary[:10]]).encode()).hexdigest()[:16],
            "regime":regime,"market_score":score,"adj_score":adj,"shannon":sh,"pe":pe,"autocorr":ac,"hurst":h,
            "hurst_label":h_label,"macro_regime":macro_regime,"direction":direction,"direction_reason":dir_reason,
            "ma20":round(ma20,4),"slope":slope,"integrity_flag":True,"market_flags":flags or [],
            "anomaly_log":f"sh={sh} pe={pe} ac={ac} H={h}[{h_label}] mkt={score} adj={adj} dir={direction}({dir_reason})"}

def triangulate_all(prices,tol=0.5):
    if len(prices)<2: return "SINGLE",{},[],False
    vals=list(prices.values()); mean_p=sum(vals)/len(vals)
    divs={s:round(abs(p-mean_p)/mean_p*100,4) for s,p in prices.items()}
    conflicts=[]; srcs=list(prices.keys())
    for i in range(len(srcs)):
        for j in range(i+1,len(srcs)):
            s1,s2=srcs[i],srcs[j]; d=abs(prices[s1]-prices[s2])/max(prices[s1],prices[s2])*100
            if d>tol: conflicts.append(f"{s1}v{s2}:{round(d,3)}%")
    fail=max(divs.values())>tol if divs else False
    return("FAIL" if fail else "OK"),divs,conflicts,fail


class PairGovernor:
    def __init__(self,pair,thr=3):
        self.pair=pair; self.thr=thr; self.history=deque(maxlen=100)
        self.blocked=False; self.reason=""; self.consec=0
        self.open_trade_id=None; self.trades_closed=0
        self.wins=0; self.losses=0; self.total_edge=0.0; self.skipped=0
    def evaluate(self,snap):
        self.history.append(snap); self.consec=self.consec+1 if snap["regime"]=="ORCHESTRATED" else 0
        if self.consec>=self.thr and not self.blocked: self.blocked=True; self.reason=f"{self.pair}:{self.consec}xORC"
        return {**snap,"consec":self.consec,"blocked":self.blocked,"action":"AWAITING" if self.blocked else "MONITOR"}
    def record_trade(self,edge):
        self.trades_closed+=1; self.total_edge+=edge
        if edge>0: self.wins+=1
        else: self.losses+=1
    def stats(self):
        t=self.trades_closed
        return {"trades":t,"win_rate":round(self.wins/t*100,1) if t else 0,
                "avg_edge":round(self.total_edge/t,4) if t else 0,"skipped":self.skipped}
    def override(self): self.blocked=False; self.consec=0; self.reason=""


def load_fred(sid,key=FRED_KEY):
    url=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&file_type=json&api_key={key}"
    data=json.loads(fetch(url)); out=[]
    for obs in data.get("observations",[]):
        try: v=float(obs["value"]); out.append({"t":obs["date"],"v":v,"o":v,"h":v,"l":v,"vol":0})
        except: continue
    return out

def load_kraken(pair,interval=60,limit=150):
    url=f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval={interval}&count={limit}"
    data=json.loads(fetch(url))
    if data.get("error") and data["error"]: raise Exception(data["error"])
    result=data["result"]; key=[k for k in result if k!="last"][0]
    return [{"t":datetime.fromtimestamp(c[0],tz=timezone.utc).isoformat(),"o":float(c[1]),"h":float(c[2]),"l":float(c[3]),"v":float(c[4]),"vol":float(c[6])} for c in result[key]]

def load_cg(coin_id):
    return json.loads(fetch(f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd")).get(coin_id,{}).get("usd")

def load_bybit(symbol):
    items=json.loads(fetch(f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol}")).get("result",{}).get("list",[])
    return float(items[0]["lastPrice"]) if items else None

def load_coincap(coin_id):
    p=json.loads(fetch(f"https://api.coincap.io/v2/assets/{coin_id}")).get("data",{}).get("priceUsd")
    return float(p) if p else None

def load_er(base,target):
    return json.loads(fetch(f"https://open.er-api.com/v6/latest/{base}")).get("rates",{}).get(target)

def load_frankfurter_hist(base,target):
    url=f"https://api.frankfurter.app/1999-01-04..?from={base}&to={target}"
    data=json.loads(fetch(url)); out=[]
    for date,rates in sorted(data.get("rates",{}).items()):
        v=rates.get(target)
        if v: out.append({"t":date,"v":float(v),"o":float(v),"h":float(v),"l":float(v),"vol":0})
    return out

def load_frankfurter_live(base,target):
    return json.loads(fetch(f"https://api.frankfurter.app/latest?from={base}&to={target}")).get("rates",{}).get(target)

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
    "VIX":"VIXCLS","US10Y":"DGS10","US2Y":"DGS2","YieldCurve":"T10Y2Y",
    "FedFunds":"FEDFUNDS","CPI":"CPIAUCSL","Oil_WTI":"DCOILWTICO",
    "M2":"M2SL","HY_Spread":"BAMLH0A0HYM2","Unemployment":"UNRATE"
}


def live_loop_v16(con,interval_min=60,fetch_every_sec=3600,window=120,max_cycles=None):
    governors={p:PairGovernor(p) for p in list(CRYPTO)+list(FOREX)}
    cycle=0; mcycle=0; series_cache={}
    log("START v16 — Directie Dinamica + Transfer Entropy")
    tg("DLL v16 pornit\nLONG/SHORT/SKIP dinamic\nTransfer Entropy BTC->altcoins")
    while True:
        cycle+=1
        if max_cycles and cycle>max_cycles: log("STOP"); break
        log(f"Ciclu #{cycle}"); t0=time.time(); mcycle+=1
        if mcycle==1 or mcycle%24==0:
            log("Refresh macro+forex...")
            for n,s in MACRO.items():
                try: db_insert(con,n,"fred_macro",load_fred(s)); time.sleep(0.4)
                except Exception as e: log(f"macro {n}:{e}")
            for p,m in FOREX.items():
                try:
                    db_insert(con,p,"fred",load_fred(m["fred"])); time.sleep(0.4)
                    fkf=load_frankfurter_hist(m["base"],m["target"])
                    db_insert(con,p,"frankfurter",fkf); log(f"BCE {p}:{len(fkf)} zile"); time.sleep(0.3)
                except Exception as e: log(f"forex {p}:{e}")
        macro_vals=get_macro_latest(con); macro_regime,macro_score=classify_macro(macro_vals)
        log(f"MACRO:{macro_regime}(s={macro_score}) VIX={macro_vals.get('VIX','?')} YC={macro_vals.get('YieldCurve','?')}")
        try: fg_val,fg_label=load_fg()
        except: fg_val=50; fg_label="Unknown"
        log(f"F&G:{fg_val} {fg_label}")
        nat=[]; orc=[]; unc=[]; blk=[]; skipped_total=0; long_total=0; short_total=0
        for pair,m in CRYPTO.items():
            gov=governors[pair]; prices={}
            try:
                c=load_kraken(m["kraken"],interval_min,window+30)
                db_insert(con,pair,"kraken",c)
                if c: prices["kraken"]=c[-1]["v"]
                time.sleep(0.2)
                try: p=load_cg(m["cg"]); prices["coingecko"]=p if p else prices.get("coingecko"); time.sleep(1.0)
                except: pass
                try: p=load_bybit(m["bybit"]); prices["bybit"]=p if p else prices.get("bybit"); time.sleep(0.2)
                except: pass
                try: p=load_coincap(m["cc"]); prices["coincap"]=p if p else prices.get("coincap"); time.sleep(0.2)
                except: pass
                prices={k:v for k,v in prices.items() if v is not None}
                ts,divs,conflicts,tf=triangulate_all(prices,0.5)
                md=max(divs.values()) if divs else 0
                db_save_tri(con,pair,list(prices.keys()),{s:round(p,4) for s,p in prices.items()},md,ts)
                series=db_load(con,pair,"kraken",window)
                if len(series)<window: continue
                series_cache[pair]=series
                snap=dll_v16(series,macro_regime,macro_vals,fg_val,"crypto")
                if tf: snap["integrity_flag"]=False; snap["market_flags"].append(f"TRI_FAIL:{md:.3f}%"); snap["regime"]="ORCHESTRATED"
                dec=gov.evaluate(snap); direction=snap["direction"]; dir_reason=snap["direction_reason"]
                if dec["regime"]=="ORCHESTRATED":
                    if direction=="SKIP": gov.skipped+=1; skipped_total+=1; log(f"  SKIP {pair} {dir_reason}")
                    elif not gov.blocked and not gov.open_trade_id:
                        tid=db_open_trade(con,pair,direction,prices.get("kraken",0),dec["regime"],macro_regime,dir_reason)
                        gov.open_trade_id=tid
                        if direction=="LONG": long_total+=1
                        else: short_total+=1
                        log(f"  {direction} {pair} @ {prices.get('kraken',0):.4f} {dir_reason}")
                elif dec["regime"]=="NATURAL" and gov.open_trade_id:
                    e=db_close_trade(con,gov.open_trade_id,prices.get("kraken",0))
                    gov.record_trade(e); result="WIN" if e and e>0 else "LOSS"
                    log(f"  CLOSE {pair} edge={e}% {result}")
                    tg(f"{result} {pair}\nEdge:{e}%\nMacro:{macro_regime}"); gov.open_trade_id=None
                if dec["regime"]=="ORCHESTRATED":
                    tg(f"ORC {pair}\nscore={snap['market_score']} PE={snap['pe']}\ndir={direction}({dir_reason})\ndiv={md:.3f}% Macro:{macro_regime} F&G:{fg_val}")
                icon="X" if dec["blocked"] else("R" if dec["regime"]=="ORCHESTRATED" else("G" if dec["regime"]=="NATURAL" else "Y"))
                log(f"[{icon}] {pair} {dec['regime']} mkt={snap['market_score']} H={snap['hurst']} dir={direction} slope={snap['slope']:+.3f}%")
                _cat(dec["regime"],gov.blocked,pair,nat,orc,unc,blk)
            except Exception as e: log(f"ERR {pair}:{e}"); time.sleep(2)
        if "XBTUSD" in series_cache and len(series_cache)>1:
            log("  [TE] Transfer Entropy BTC->altcoins...")
            btc_vals=[p["v"] for p in series_cache["XBTUSD"]]; te_results=[]
            for tgt_pair,tgt_series in series_cache.items():
                if tgt_pair=="XBTUSD": continue
                tgt_vals=[p["v"] for p in tgt_series]
                te=transfer_entropy(btc_vals,tgt_vals,lag=3)
                significant=te>0.05; db_save_te(con,"XBTUSD",tgt_pair,te,3,significant)
                te_results.append(f"{tgt_pair}:{te:.4f}{'*' if significant else ''}")
            log(f"  TE: {' | '.join(te_results)}")
        for pair,m in FOREX.items():
            gov=governors[pair]; prices={}
            try:
                fred_s=db_load(con,pair,"fred",window); fkf_s=db_load(con,pair,"frankfurter",window)
                if fred_s: prices["fred"]=fred_s[-1]["v"]
                if fkf_s: prices["bce"]=fkf_s[-1]["v"]
                try: p=load_er(m["base"],m["target"]); prices["exchangerate"]=p if p else prices.get("exchangerate"); time.sleep(0.3)
                except: pass
                try: p=load_frankfurter_live(m["base"],m["target"]); prices["bce_live"]=p if p else prices.get("bce_live"); time.sleep(0.3)
                except: pass
                prices={k:v for k,v in prices.items() if v is not None}
                ts,divs,conflicts,tf=triangulate_all(prices,0.3)
                md=max(divs.values()) if divs else 0
                if prices: db_save_tri(con,pair,list(prices.keys()),{s:round(p,6) for s,p in prices.items()},md,ts)
                series=fred_s if len(fred_s)>=window else fkf_s
                if not series or len(series)<window: continue
                for pt in series: pt["src"]=pair
                snap=dll_v16(series,macro_regime,macro_vals,fg_val,"forex")
                if tf: snap["integrity_flag"]=False; snap["market_flags"].append(f"TRI_FAIL:{md:.4f}%"); snap["regime"]="ORCHESTRATED"
                dec=gov.evaluate(snap); direction=snap["direction"]
                if dec["regime"]=="ORCHESTRATED" and direction!="SKIP" and not gov.blocked and not gov.open_trade_id:
                    fred_price=prices.get("fred",prices.get("exchangerate",0))
                    tid=db_open_trade(con,pair,direction,fred_price,dec["regime"],macro_regime,snap["direction_reason"])
                    gov.open_trade_id=tid; log(f"  {direction} FOREX {pair} @ {fred_price:.6f}")
                elif dec["regime"]=="NATURAL" and gov.open_trade_id:
                    fred_price=prices.get("fred",prices.get("exchangerate",0))
                    e=db_close_trade(con,gov.open_trade_id,fred_price)
                    gov.record_trade(e); result="WIN" if e and e>0 else "LOSS"
                    log(f"  CLOSE FOREX {pair} edge={e}% {result}")
                    tg(f"{result} FOREX {pair}\nEdge:{e}%"); gov.open_trade_id=None
                icon="R" if dec["regime"]=="ORCHESTRATED" else "G" if dec["regime"]=="NATURAL" else "Y"
                log(f"[{icon}] {pair} {dec['regime']} mkt={snap['market_score']} dir={direction} div={md:.4f}%")
                _cat(dec["regime"],gov.blocked,pair,nat,orc,unc,blk)
            except Exception as e: log(f"ERR {pair}:{e}")
        log(f"SUMMARY NAT={len(nat)} ORC={len(orc)} UNC={len(unc)} BLOC={len(blk)}")
        log(f"TRADES: LONG={long_total} SHORT={short_total} SKIP={skipped_total}")
        stats_msg=""
        for pair,gov in governors.items():
            if gov.trades_closed>0:
                s=gov.stats(); stats_msg+=f"\n{pair}:{s['trades']}t WR={s['win_rate']}% edge={s['avg_edge']:+.4f}%"
        tg(f"DLL v16 C#{cycle}\nMacro:{macro_regime}\nNAT:{','.join(nat) or '-'}\nORC:{','.join(orc) or '-'}\nLONG={long_total} SHORT={short_total} SKIP={skipped_total}\nF&G:{fg_val}{stats_msg}")
        sleep_t=max(0,fetch_every_sec-(time.time()-t0)); log(f"Next {sleep_t:.0f}s"); time.sleep(sleep_t)
    return governors


os.makedirs("/data",exist_ok=True)
con=db_init(DB_PATH)
print("DLL v16 START")
print("Directie dinamica: LONG/SHORT/SKIP bazat pe MA20+Hurst+FG+macro")
print("Transfer Entropy: BTC -> toate altcoins la fiecare ciclu")
tg("DLL v16 OK")
live_loop_v16(con,interval_min=60,fetch_every_sec=3600,window=120,max_cycles=None)
