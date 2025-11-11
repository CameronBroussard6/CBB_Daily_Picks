#!/usr/bin/env python3
import os, io, sys, time, re, random
from datetime import datetime, timezone
import requests
import pandas as pd

# ---------------- CONFIG (env overridable) ----------------
HOME_COURT_POINTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESHOLD    = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTPUT_DIR        = os.getenv("OUTPUT_DIR", "site")
TORVIK_YEAR       = os.getenv("TORVIK_YEAR", "2025")

TORVIK_URL_BASE   = "https://barttorvik.com/trank.php"
TR_ODDS_URL       = "https://www.teamrankings.com/ncb/odds/"

# retry tuning (still live-only, just tries again if Torvik sends HTML)
TORVIK_MAX_RETRIES = int(os.getenv("TORVIK_MAX_RETRIES", "6"))
TORVIK_SLEEP_BASE  = float(os.getenv("TORVIK_SLEEP_BASE", "2.0"))  # seconds

# ---------------- Utilities ----------------
def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.lower().strip()
    s = re.sub(r"&", "and", s)
    s = s.replace("’", "'").replace("‘", "'").replace("´","'")
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"\s+", " ", s)
    repl = {
        "st "         : "saint ",
        "st. "        : "saint ",
        "cal st "     : "cal state ",
        "texas a&m cc": "texas a&m corpus christi",
        "texas a&m-corpus christi": "texas a&m corpus christi",
        "long island university": "liu",
        "central connecticut state": "central connecticut",
        "saint josephs": "saint joseph's",
        "william and mary": "william & mary",
        "mount st marys": "mount st. mary's",
        "ucsb": "uc santa barbara",
    }
    for k,v in repl.items():
        if s.startswith(k):
            s = s.replace(k, v, 1)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def pick(df: pd.DataFrame, names):
    for n in names:
        if n in df.columns:
            return df[n]
    raise KeyError(f"Missing expected column among ({', '.join(names)})")

# ---------------- Data Loads (LIVE ONLY) ----------------
_UAS = [
    # rotate a few modern desktop UAs to dodge basic rate limits
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]

def _torvik_params():
    # little cache-buster to avoid CDN reuse when rate limit page is cached
    return {
        "year": TORVIK_YEAR,
        "csv": "1",
        "top": "0",            # include all teams (avoids default cut)
        "_": str(int(time.time()*1000) + random.randint(0, 99999)),
    }

def load_torvik():
    session = requests.Session()
    last_err = None
    for attempt in range(1, TORVIK_MAX_RETRIES + 1):
        ua = random.choice(_UAS)
        params = _torvik_params()
        headers = {
            "User-Agent": ua,
            "Accept": "text/csv, text/plain, */*;q=0.8",
            "Referer": "https://barttorvik.com/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        url = TORVIK_URL_BASE
        log(f"[INFO] Loading Torvik CSV (try {attempt}/{TORVIK_MAX_RETRIES}) from {url} params={params}")
        try:
            r = session.get(url, params=params, headers=headers, timeout=25)
            r.raise_for_status()
            text = r.text
            # if starts with HTML tag, it's the splash/rate-limit page
            if text.lstrip().startswith("<"):
                last_err = RuntimeError("Torvik returned HTML instead of CSV (rate limited or format change).")
                raise last_err
            df = pd.read_csv(io.StringIO(text))
            team = pick(df, ["Team","team","School","school"])
            adjo = pick(df, ["AdjO","AdjOE","AdjO."])
            adjd = pick(df, ["AdjD","AdjDE","AdjD."])
            out = pd.DataFrame({
                "team_raw": team.astype(str),
                "AdjO": pd.to_numeric(adjo, errors="coerce"),
                "AdjD": pd.to_numeric(adjd, errors="coerce"),
            }).dropna(subset=["AdjO","AdjD"])
            out["team_key"] = out["team_raw"].map(normalize_name)
            log(f"[INFO] Loaded Torvik rows: {len(out)}")
            return out
        except Exception as e:
            last_err = e
            sleep_s = TORVIK_SLEEP_BASE * (1.5 ** (attempt - 1))
            log(f"[WARN] Torvik fetch failed (attempt {attempt}): {e}. Sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
    # if we get here, all retries failed (still live-only: we fail the job)
    raise RuntimeError(f"Unable to load ratings from Torvik after {TORVIK_MAX_RETRIES} attempts: {last_err}")

def parse_tr_spread_cell(cell: str):
    if not isinstance(cell, str):
        return (None, None)
    s = cell.strip()
    if not s or s.lower() == "pick":
        return (None, 0.0)
    m = re.match(r"^(.*)\s([+-]?\d+(?:\.\d+)?)$", s)
    if not m:
        return (None, None)
    fav = m.group(1).strip()
    val = float(m.group(2))
    return (fav, val)

def load_odds_from_teamrankings():
    log(f"[INFO] Loading market spreads from {TR_ODDS_URL}")
    headers = {"User-Agent": random.choice(_UAS)}
    r = requests.get(TR_ODDS_URL, headers=headers, timeout=25)
    r.raise_for_status()
    # Use lxml if available, else fallback to bs4-html5lib path via pandas internally
    tables = pd.read_html(io.StringIO(r.text), flavor=None)
    odds = None
    for t in tables:
        cols = [c.lower() for c in t.columns.astype(str)]
        if any("matchup" in c for c in cols) and any("spread" in c for c in cols):
            odds = t
            break
    if odds is None:
        raise RuntimeError("Could not find odds table on TeamRankings page.")
    odds.columns = [str(c).strip() for c in odds.columns]
    matchup_col = [c for c in odds.columns if "Matchup" in c][0]
    spread_col  = [c for c in odds.columns if c.lower().startswith("spread")][0]

    def split_matchup(s: str):
        s = str(s)
        if " at " in s.lower():
            parts = re.split(r"\s+at\s+", s, flags=re.I)
        elif " @ " in s:
            parts = s.split(" @ ")
        else:
            parts = s.split()
            if len(parts) >= 2:
                return parts[0], " ".join(parts[1:])
            return s, ""
        away = parts[0].strip()
        home = parts[1].strip() if len(parts) > 1 else ""
        return away, home

    recs = []
    for _, row in odds.iterrows():
        away_raw, home_raw = split_matchup(row.get(matchup_col, ""))
        if not away_raw or not home_raw:
            continue
        fav, val = parse_tr_spread_cell(str(row.get(spread_col, "")).strip())
        home_spread = None
        if fav is None and val is not None:
            home_spread = 0.0
        elif fav is not None and (val is not None):
            fav_key = normalize_name(fav)
            home_key = normalize_name(home_raw)
            away_key = normalize_name(away_raw)
            if fav_key == home_key:
                home_spread = float(val)
            elif fav_key == away_key:
                home_spread = -float(val)  # away favored -> home is +val
            else:
                home_spread = float(val)
        if home_spread is None:
            continue
        recs.append({
            "home_raw": home_raw,
            "away_raw": away_raw,
            "home_spread": float(home_spread),
            "home_key": normalize_name(home_raw),
            "away_key": normalize_name(away_raw),
        })
    out = pd.DataFrame.from_records(recs)
    log(f"[INFO] Parsed markets: {len(out)} games")
    return out

# ---------------- Model ----------------
def model_margin(h_AdjO, h_AdjD, a_AdjO, a_AdjD, hcp=HOME_COURT_POINTS):
    return (h_AdjO - a_AdjD) - (a_AdjO - h_AdjD) + hcp

# ---------------- Publish ----------------
def write_index(ok: bool, n_games: int, n_edges: int):
    ensure_dir(OUTPUT_DIR)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M %Z")
    if ok:
        body = f"""
        <h1>NCAAB Daily Edges</h1>
        <p>Latest run artifacts below. (Auto-published)</p>
        <p><b>{n_games}</b> games with lines. <b>{n_edges}</b> edges ≥ {EDGE_THRESHOLD:.1f} pts.</p>
        <ul>
          <li><a href="edges_full.csv">edges_full.csv</a></li>
          <li><a href="edges_top.csv">edges_top.csv</a></li>
          <li><a href="build_log.txt">build_log.txt</a></li>
        </ul>
        <p>Updated: {ts}</p>
        """
    else:
        body = f"""
        <h1>NCAAB Daily Edges</h1>
        <p>Latest run artifacts below. (Auto-published)</p>
        <p><b>No CSV outputs found</b></p>
        <p>See <a href="build_log.txt">build_log.txt</a> for details.</p>
        <p>Updated: {ts}</p>
        """
    html = "<!doctype html><meta charset='utf-8'><body style='font-family:Georgia,serif;font-size:18px;line-height:1.3'>" + body + "</body>"
    with open(os.path.join(OUTPUT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

# ---------------- Main ----------------
def main():
    ensure_dir(OUTPUT_DIR)
    log_path = os.path.join(OUTPUT_DIR, "build_log.txt")

    class Tee:
        def __init__(self, path):
            self.f = open(path, "w", encoding="utf-8")
        def write(self, s): self.f.write(s); self.f.flush()
        def close(self): self.f.close()

    tee = Tee(log_path)

    try:
        p = lambda *a, **k: (print(*a, **k), tee.write(" ".join(str(x) for x in a) + "\n"))
        p(f"[INFO] Date={datetime.now(timezone.utc).strftime('%Y-%m-%d')} HCA={HOME_COURT_POINTS} Edge={EDGE_THRESHOLD}")

        ratings = load_torvik()
        odds    = load_odds_from_teamrankings()

        home = ratings.add_prefix("h_")
        away = ratings.add_prefix("a_")
        merged = odds.merge(home, left_on="home_key", right_on="h_team_key") \
                     .merge(away, left_on="away_key", right_on="a_team_key")

        merged["model_home_margin"] = merged.apply(
            lambda r: model_margin(r["h_AdjO"], r["h_AdjD"], r["a_AdjO"], r["a_AdjD"]), axis=1
        )
        merged["market_home_margin"] = -merged["home_spread"].astype(float)
        merged["edge_pts"] = merged["model_home_margin"] - merged["market_home_margin"]

        out = merged[[
            "home_raw","away_raw","home_spread",
            "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
            "model_home_margin","market_home_margin","edge_pts"
        ]].rename(columns={"home_raw":"home","away_raw":"away"}).copy()

        out["ticket"] = out.apply(lambda r: f"{r['home']} {r['home_spread']:+.1f}".replace("+0.0","PK").replace("-0.0","PK"), axis=1)
        out.sort_values("edge_pts", key=lambda s: s.abs(), ascending=False, inplace=True)

        ensure_dir(OUTPUT_DIR)
        out.to_csv(os.path.join(OUTPUT_DIR, "edges_full.csv"), index=False)
        top = out[out["edge_pts"].abs() >= EDGE_THRESHOLD]
        top.to_csv(os.path.join(OUTPUT_DIR, "edges_top.csv"), index=False)

        p(f"[INFO] Wrote {len(out)} games; {len(top)} edges >= {EDGE_THRESHOLD}")
        write_index(True, len(out), len(top))
    except Exception as e:
        err = f"[ERROR] {type(e).__name__}: {e}"
        print(err); tee.write(err+"\n")
        write_index(False, 0, 0)
        sys.exit(1)
    finally:
        tee.close()

if __name__ == "__main__":
    main()
