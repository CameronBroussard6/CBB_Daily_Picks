# run.py
# NCAAB Daily Edges – scrape ratings + spreads, join, and publish
# Python 3.11
import os, io, re, sys, json, math, datetime as dt
from typing import List, Dict, Tuple
import pandas as pd
import requests
from pandas.errors import ParserError

# ------------------------------- CONFIG -------------------------------
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "2.0"))
HOME_COURT_POINTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "site")
LOG_PATH = os.path.join(OUTPUT_DIR, "build_log.txt")

TODAY = dt.date.today()

# ------------------------------- UTIL --------------------------------
def _log(msg: str):
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def _norm(s: str) -> str:
    # normalize team names for joining across sources
    s = (s or "").lower()
    # common cleanups
    s = s.replace("&amp;", "and").replace("&", "and")
    s = re.sub(r"\b(university|st\.|st|state|college|city|univ|u)\b", lambda m: {
        "university":"", "st.":"saint", "st":"saint", "state":"st", "college":"", "city":"", "univ":"", "u":""}[m.group(0)], s)
    s = s.replace("cal st", "cal state")
    s = s.replace("a&m", "am").replace("a & m", "am")
    s = s.replace("miami (fl)", "miami fl").replace("miami (oh)", "miami oh")
    s = s.replace("stephen f. austin", "stephen f austin")
    s = s.replace("uab", "alabama birmingham")
    s = s.replace("uc ", "california ").replace("ucla", "ucla").replace("usc", "southern california")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def _safe_read_csv(text: str) -> pd.DataFrame:
    text = text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")
    buf = io.StringIO(text)
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
            nc = norm(c)
            if any(sub in nc for sub in substrats):
                return c
    if allow_fallback_first_text:
        for c in df.columns:
            if df[c].dtype == object and not re.search(r"(rank|rk|seed)", c, flags=re.I):
                return c
    raise KeyError(f"Missing expected column for {name_for_error or candidates} (first cols: {list(df.columns)[:10]})")

# --------------------------- DATA LOADERS -----------------------------
def load_torvik(run_date: dt.date) -> pd.DataFrame:
    """Load Torvik efficiency ratings with robust header detection."""
    year = run_date.year
    url_csv = f"https://barttorvik.com/trank.php?year={year}&csv=1"
    try:
        r = requests.get(url_csv, timeout=30)
        r.raise_for_status()
        df = _safe_read_csv(r.text)
        _log(f"[INFO] Loaded Torvik rows: {len(df)}")
    except Exception as e_csv:
        _log(f"[WARN] Torvik CSV failed: {e_csv}")
        try:
            url_html = f"https://barttorvik.com/trank.php?year={year}"
            r = requests.get(url_html, timeout=30)
            r.raise_for_status()
            tables = pd.read_html(r.text)
            df = max(tables, key=lambda t: t.shape[0] * t.shape[1])
            _log(f"[INFO] Loaded Torvik via HTML: {len(df)}")
        except Exception as e_html:
            _log(f"[WARN] Torvik HTML fallback failed: {e_html}")
            bkp = "data/torvik_backup.csv"
            if os.path.exists(bkp):
                _log("[INFO] Using local backup: data/torvik_backup.csv")
                df = pd.read_csv(bkp)
            else:
                raise RuntimeError("Unable to load ratings from Torvik (CSV/HTML/backup all failed)")

    team_col = _pick_column(
        df,
        candidates=["team", "school", "team name", "program", "teamname", "name"],
        substrats=["team", "school", "program", "name"],
        allow_fallback_first_text=True,
        name_for_error="team"
    )
    adjo_col = _pick_column(
        df,
        candidates=["AdjO", "AdjOE", "Adj O", "Adj Off", "Offense", "Off Eff"],
        substrats=["adjo", "adjoe", "adj o", "off", "offense", "offeff"],
        name_for_error="AdjO"
    )
    adjd_col = _pick_column(
        df,
        candidates=["AdjD", "AdjDE", "Adj D", "Adj Def", "Defense", "Def Eff"],
        substrats=["adjd", "adjde", "adj d", "def", "defense", "defeff"],
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
    """
    Scrape spreads from Bovada JSON endpoint. Returns:
    columns: home, away, home_spread (float)
    Notes: some small-school or non-lined games won’t appear.
    """
    url = "https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/ncaa-basketball?marketFilterId=def&eventsLimit=10000"
    r = requests.get(url, timeout=30)
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

            # find Point Spread market
            line = None
            for m in ev.get("displayGroups", []):
                for mk in m.get("markets", []):
                    if "Point Spread" in mk.get("description", ""):
                        # pick home outcome
                        for o in mk.get("outcomes", []):
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

            rows.append({
                "home": home,
                "away": away,
                "home_spread": float(line)  # Bovada uses negative for fav (e.g., -7.5)
            })

    df = pd.DataFrame(rows)
    if df.empty:
        _log("[WARN] No spreads parsed from Bovada")
    else:
        _log(f"[INFO] Parsed {len(df)} spread rows from Bovada")
    # build join keys
    df["hkey"] = df["home"].map(_norm)
    df["akey"] = df["away"].map(_norm)
    return df

# --------------------------- MODEL / MERGE ----------------------------
def _build_matchups(spreads: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    left = spreads.merge(ratings.add_prefix("h_"), left_on="hkey", right_on="h_key", how="left")
    left = left.merge(ratings.add_prefix("a_"), left_on="akey", right_on="a_key", how="left")

    # Clean columns
    keep = ["home", "away", "home_spread",
            "h_team", "h_AdjO", "h_AdjD",
            "a_team", "a_AdjO", "a_AdjD"]
    left = left[[c for c in keep if c in left.columns]].copy()

    # Compute model home margin from adj efficiencies (+ HCA)
    # Simple proxy: (h_AdjO - a_AdjD) - (a_AdjO - h_AdjD) + HCA
    left["model_home_margin"] = (
        (left["h_AdjO"] - left["a_AdjD"]) - (left["a_AdjO"] - left["h_AdjD"]) + HOME_COURT_POINTS
    )

    # Market margin from home perspective: Bovada home_spread already uses negative for favorite.
    # So market_home_margin == home_spread.
    left["market_home_margin"] = left["home_spread"]

    # Edge in points
    left["edge_pts"] = left["model_home_margin"] - left["market_home_margin"]

    # Ticket display "HOME +/-X"
    def _ticket(row):
        try:
            return f"{row['home']} {row['home_spread']:+g}"
        except Exception:
            return ""
    left["ticket"] = left.apply(_ticket, axis=1)

    # Final column order (as you requested)
    cols = [
        "home", "away", "home_spread",
        "h_AdjO", "h_AdjD", "a_AdjO", "a_AdjD",
        "model_home_margin", "market_home_margin", "edge_pts", "ticket"
    ]
    left = left[cols]

    # sort by absolute edge desc
    left = left.sort_values("edge_pts", ascending=False).reset_index(drop=True)
    return left

# ------------------------------ OUTPUT --------------------------------
def _write_csv(df: pd.DataFrame, name: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, name)
    df.to_csv(path, index=False)
    _log(f"[INFO] wrote {path} ({len(df)} rows)")

def _write_site_index(have_csv: bool):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    idx = os.path.join(OUTPUT_DIR, "index.html")
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    if have_csv:
        table_link = '<li><a href="edges.csv">edges.csv</a></li>'
    else:
        table_link = "<p>No CSV outputs found</p>"
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>NCAAB Daily Edges</title></head>
<body style="font-family: Georgia, serif; font-size: 18px;">
<h1>NCAAB Daily Edges</h1>
<p>Latest run artifacts below. (Auto-published)</p>
<ul>{table_link}<li><a href="build_log.txt">build_log.txt</a></li></ul>
<p style="margin-top:24px;font-size:14px;color:#666;">Updated: {ts}</p>
</body></html>"""
    with open(idx, "w", encoding="utf-8") as f:
        f.write(html)
    _log("[INFO] wrote site/index.html")

# ------------------------------- MAIN ---------------------------------
def main(run_date: dt.date):
    _log(f"[INFO] Date={run_date} HCA={HOME_COURT_POINTS:.1f} Edge={EDGE_THRESHOLD:.1f}")

    ratings = load_torvik(run_date)
    spreads = fetch_bovada_spreads()

    if spreads.empty:
        _log("[ERROR] No spreads fetched; writing index only.")
        _write_site_index(False)
        return

    joined = _build_matchups(spreads, ratings)

    # keep as-is; you asked NOT to include away_spread/h_Team/a_Team/recommend
    _write_csv(joined, "edges.csv")

    # filtered edges >= threshold for convenience
    strong = joined[joined["edge_pts"].abs() >= EDGE_THRESHOLD].reset_index(drop=True)
    _write_csv(strong, "edges_strong.csv")

    _write_site_index(True)

if __name__ == "__main__":
    try:
        # start clean log each run
        if os.path.exists(LOG_PATH):
            os.remove(LOG_PATH)
    except Exception:
        pass

    try:
        main(TODAY)
    except Exception as e:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        _log(f"[FATAL] {repr(e)}")
        # Ensure at least the index & log go up so Pages step never fails silently
        try:
            _write_site_index(False)
        finally:
            sys.exit(1)
