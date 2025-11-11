# run.py (hardened Torvik loader; robust HTML/CSV detection)
import os, io, re, sys, datetime as dt
from typing import List
import pandas as pd
import requests
from pandas.errors import ParserError

EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "2.0"))
HOME_COURT_POINTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "site")
LOG_PATH = os.path.join(OUTPUT_DIR, "build_log.txt")
TODAY = dt.date.today()

def _log(msg: str):
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

# -------------------- normalization helpers --------------------
def _norm(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("&amp;", "and").replace("&", "and")
    s = s.replace("st.", "saint").replace("st ", "saint ")
    s = s.replace("a&m", "am").replace(" a & m", " am")
    s = s.replace("cal st", "cal state")
    s = s.replace("stephen f. austin", "stephen f austin")
    s = s.replace("miami (fl)", "miami fl").replace("miami (oh)", "miami oh")
    s = re.sub(r"\b(university|college|city|univ)\b", "", s)
    return re.sub(r"[^a-z0-9]+", "", s)

def _safe_read_csv_text(text: str) -> pd.DataFrame:
    # Detect HTML masquerading as CSV
    if text.lstrip().startswith("<!DOCTYPE") or text.lstrip().startswith("<html"):
        raise ParserError("HTML returned instead of CSV")
    buf = io.StringIO(text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff"))
    try:
        return pd.read_csv(buf)
    except ParserError:
        buf.seek(0)
        return pd.read_csv(buf, engine="python", sep=",", on_bad_lines="skip")

def _pick_column(df: pd.DataFrame, candidates: List[str], substrats: List[str]=None,
                 allow_fallback_first_text=False, name_for_error=""):
    def norm(s): return re.sub(r"[^a-z0-9]+", "", str(s).lower())
    norm_map = {norm(c): c for c in df.columns}
    for cand in candidates:
        if norm(cand) in norm_map:
            return norm_map[norm(cand)]
    if substrats:
        for c in df.columns:
            if any(sub in norm(c) for sub in substrats):
                return c
    if allow_fallback_first_text:
        for c in df.columns:
            if df[c].dtype == object:
                return c
    raise KeyError(f"Missing expected column for {name_for_error or candidates} (first cols: {list(df.columns)[:5]})")

# -------------------- data sources --------------------
def load_torvik(run_date: dt.date) -> pd.DataFrame:
    year = run_date.year
    headers = {"User-Agent": "Mozilla/5.0 (GitHubActions CBB Edges)"}
    csv_url  = f"https://barttorvik.com/trank.php?year={year}&csv=1"
    html_url = f"https://barttorvik.com/trank.php?year={year}"

    # Try CSV with UA; if HTML sniffed, fall back to HTML tables
    try:
        r = requests.get(csv_url, timeout=30, headers=headers)
        r.raise_for_status()
        df = _safe_read_csv_text(r.text)
        _log(f"[INFO] Loaded Torvik CSV rows: {len(df)}; cols={list(df.columns)[:5]}")
    except Exception as e_csv:
        _log(f"[WARN] CSV fetch/parse failed ({e_csv}); trying HTML table")
        r = requests.get(html_url, timeout=30, headers=headers)
        r.raise_for_status()
        tables = pd.read_html(r.text)
        # choose the widest table
        df = max(tables, key=lambda t: t.shape[0] * t.shape[1])
        _log(f"[INFO] Loaded Torvik HTML table rows: {len(df)}; cols={list(df.columns)[:5]}")

    # header variants seen across seasons
    team_col = _pick_column(
        df,
        candidates=["Team", "School", "Team Name", "Program", "Name"],
        substrats=["team", "school", "program", "name"],
        allow_fallback_first_text=True,
        name_for_error="team"
    )
    adjo_col = _pick_column(
        df,
        candidates=["AdjO", "AdjOE", "Adj O", "Adj Off", "Offense", "Off Eff", "Offensive Efficiency"],
        substrats=["adjo", "adjoe", "adj o", "offeff", "offense"],
        name_for_error="AdjO"
    )
    adjd_col = _pick_column(
        df,
        candidates=["AdjD", "AdjDE", "Adj D", "Adj Def", "Defense", "Def Eff", "Defensive Efficiency"],
        substrats=["adjd", "adjde", "adj d", "defeff", "defense"],
        name_for_error="AdjD"
    )

    out = df[[team_col, adjo_col, adjd_col]].copy()
    out.columns = ["team", "AdjO", "AdjD"]
    out["team"] = out["team"].astype(str).str.strip()
    out["AdjO"] = pd.to_numeric(out["AdjO"], errors="coerce")
    out["AdjD"] = pd.to_numeric(out["AdjD"], errors="coerce")
    out = out.dropna(subset=["AdjO", "AdjD"]).reset_index(drop=True)
    out["key"] = out["team"].map(_norm)
    return out

def fetch_bovada_spreads() -> pd.DataFrame:
    url = "https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/ncaa-basketball?marketFilterId=def&eventsLimit=10000"
    r = requests.get(url, timeout=30, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()
    rows = []
    for blk in data:
        for ev in blk.get("events", []):
            comps = ev.get("competitors", [])
            if len(comps) != 2:
                continue
            home = next((c["name"] for c in comps if c.get("home")), None)
            away = next((c["name"] for c in comps if not c.get("home")), None)
            if not home or not away:
                continue
            line = None
            for g in ev.get("displayGroups", []):
                for m in g.get("markets", []):
                    if "Point Spread" in m.get("description", ""):
                        for o in m.get("outcomes", []):
                            if o.get("type") == "H":
                                try:
                                    line = float(o["price"]["handicap"])
                                except Exception:
                                    pass
                        break
                if line is not None:
                    break
            if line is None:
                continue
            rows.append({"home": home, "away": away, "home_spread": float(line)})
    df = pd.DataFrame(rows)
    if df.empty:
        _log("[WARN] No spreads parsed from Bovada")
    else:
        _log(f"[INFO] Parsed {len(df)} spread rows from Bovada")
    df["hkey"] = df["home"].map(_norm)
    df["akey"] = df["away"].map(_norm)
    return df

# -------------------- merge / model --------------------
def _build_matchups(spreads: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    left = spreads.merge(ratings.add_prefix("h_"), left_on="hkey", right_on="h_key", how="left")
    left = left.merge(ratings.add_prefix("a_"), left_on="akey", right_on="a_key", how="left")

    cols_present = [c for c in [
        "home","away","home_spread","h_team","h_AdjO","h_AdjD","a_team","a_AdjO","a_AdjD"
    ] if c in left.columns]
    left = left[cols_present].copy()

    left["model_home_margin"] = (
        (left["h_AdjO"] - left["a_AdjD"]) - (left["a_AdjO"] - left["h_AdjD"]) + HOME_COURT_POINTS
    )
    left["market_home_margin"] = left["home_spread"]
    left["edge_pts"] = left["model_home_margin"] - left["market_home_margin"]
    left["ticket"] = left.apply(lambda r: f"{r['home']} {r['home_spread']:+g}" if pd.notna(r.get("home_spread")) else "", axis=1)

    final_cols = ["home","away","home_spread","h_AdjO","h_AdjD","a_AdjO","a_AdjD",
                  "model_home_margin","market_home_margin","edge_pts","ticket"]
    left = left.reindex(columns=final_cols)
    return left.sort_values("edge_pts", ascending=False).reset_index(drop=True)

# -------------------- output --------------------
def _write_csv(df: pd.DataFrame, name: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, name)
    df.to_csv(path, index=False)
    _log(f"[INFO] wrote {path} ({len(df)} rows)")

def _write_site_index(have_csv: bool):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    idx = os.path.join(OUTPUT_DIR, "index.html")
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    link = '<li><a href="edges.csv">edges.csv</a></li>' if have_csv else "<p>No CSV outputs found</p>"
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>NCAAB Daily Edges</title></head>
<body style="font-family: Georgia, serif; font-size:18px">
<h1>NCAAB Daily Edges</h1>
<p>Latest run artifacts below. (Auto-published)</p>
<ul>{link}<li><a href="build_log.txt">build_log.txt</a></li></ul>
<p style="margin-top:24px;font-size:14px;color:#666">Updated: {ts}</p>
</body></html>"""
    with open(idx, "w", encoding="utf-8") as f:
        f.write(html)
    _log("[INFO] wrote site/index.html")

# -------------------- main --------------------
def main(run_date: dt.date):
    _log(f"[INFO] Date={run_date} HCA={HOME_COURT_POINTS:.1f} Edge={EDGE_THRESHOLD:.1f}")
    ratings = load_torvik(run_date)
    spreads = fetch_bovada_spreads()
    if spreads.empty:
        _log("[ERROR] No spreads fetched; publishing index only")
        _write_site_index(False); return
    joined = _build_matchups(spreads, ratings)
    _write_csv(joined, "edges.csv")
    strong = joined[joined["edge_pts"].abs() >= EDGE_THRESHOLD].reset_index(drop=True)
    _write_csv(strong, "edges_strong.csv")
    _write_site_index(True)

if __name__ == "__main__":
    try:
        if os.path.exists(LOG_PATH):
            os.remove(LOG_PATH)
    except Exception:
        pass
    try:
        main(TODAY)
    except Exception as e:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        _log(f"[FATAL] {repr(e)}")
        try:
            _write_site_index(False)
        finally:
            sys.exit(1)
