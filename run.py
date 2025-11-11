#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build & Publish NCAAB Edges (resilient)
- Tries Torvik CSV; falls back to Torvik HTML; then local backup CSV if present.
- If ratings/odds unavailable, still writes index + build_log and exits 0 (so Pages deploys).
- Outputs:
    site/index.html
    site/edges_full.csv           (when we have ratings+odds)
    site/edges_top.csv            (|edge| >= EDGE_THRESHOLD)
    site/build_log.txt
Env:
  HOME_COURT_POINTS (default 0.6)
  EDGE_THRESHOLD    (default 2.0)
  OUTPUT_DIR        (default "site")
  TORVIK_URL        (optional) default points at Torvik CSV
  ODDS_CSV          (optional) local odds fallback
  TORVIK_BACKUP     (optional) local backup ratings CSV (default data/torvik_backup.csv)
"""

from __future__ import annotations
import io, os, re, sys, math, traceback
from datetime import datetime, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ------------ Config ------------
HCA_PTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "site").strip() or "site"

TORVIK_URL = os.getenv("TORVIK_URL", "https://barttorvik.com/trank.php?year=2025&csv=1")
ODDS_CSV = os.getenv("ODDS_CSV", "data/odds.csv")
TORVIK_BACKUP = os.getenv("TORVIK_BACKUP", "data/torvik_backup.csv")

# ------------ Logging ------------
def ensure_dir(p): os.makedirs(p, exist_ok=True)
ensure_dir(OUTPUT_DIR)

def log_write(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(os.path.join(OUTPUT_DIR, "build_log.txt"), "a", encoding="utf-8") as f:
        f.write(line + "\n")

def write_index(has_csv: bool, n_games: int, n_edges: int, note: str = ""):
    ensure_dir(OUTPUT_DIR)
    path = os.path.join(OUTPUT_DIR, "index.html")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not has_csv:
        body = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>NCAAB Daily Edges</title></head>
<body style="font-family: Georgia, serif; padding:16px">
<h1>NCAAB Daily Edges</h1>
<p>Latest run artifacts below. (Auto-published)</p>
<h2>No CSV outputs found</h2>
<p>{note}</p>
<p>See <a href="build_log.txt">build_log.txt</a> for details.</p>
<p style="color:#555">Updated: {now}</p>
</body></html>"""
    else:
        body = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>NCAAB Daily Edges</title></head>
<body style="font-family: Georgia, serif; padding:16px">
<h1>NCAAB Daily Edges</h1>
<p>Latest run artifacts below. (Auto-published)</p>
<ul>
  <li><a href="edges_full.csv">edges_full.csv</a> (games: {n_games})</li>
  <li><a href="edges_top.csv">edges_top.csv</a> (|edge| ≥ {EDGE_THRESHOLD}: {n_edges})</li>
  <li><a href="build_log.txt">build_log.txt</a></li>
</ul>
<p style="color:#555">Updated: {now}</p>
</body></html>"""
    with open(path, "w", encoding="utf-8") as f: f.write(body)
    log_write(f"[INFO] Wrote {path}")

# ------------ Helpers ------------
NONALNUM = re.compile(r"[^A-Z0-9]+")
ALIASES = {
    "OLEMISS": "MISSISSIPPI",
    "CENTRALCONNECTICUTSTATE": "CENTRALCONNECTICUT",
    "SAINTJOHNS": "STJOHNS",
    "SAINTJOSEPHS": "STJOSEPHS",
    "ILLCHICAGO": "UIC",
    "LOYOLOMARYMOUNT": "LOYOLAMARYMOUNT",
    "TEXASAAMCORPUSCHRISTI": "TEXASAAMCORPUSCHRISTI",
}
def norm_team(s: str) -> str:
    if pd.isna(s): return ""
    s = str(s).replace("St.", "State").replace("&", "and").replace("'", "")
    return NONALNUM.sub("", s.upper())
def canonical(name: str) -> str:
    n = norm_team(name)
    return ALIASES.get(n, n)

def pick_col(df: pd.DataFrame, *cands) -> str:
    for c in cands:
        if c in df.columns: return c
    raise KeyError(f"Missing expected column among {cands} (head cols: {list(df.columns[:6])})")

# ------------ Ratings ------------
def coerce_ratings(df: pd.DataFrame) -> pd.DataFrame:
    team_col = None
    for c in ["Team","team","school","School","TEAM"]:
        if c in df.columns: team_col = c; break
    if not team_col:
        # heuristic
        for c in df.columns:
            s = df[c].astype(str)
            if s.str.contains("State|College|University|A&M|St\\.|Saint", case=False, regex=True).any():
                team_col = c; break
    if not team_col:
        raise KeyError(f"Could not find team column in ratings: {df.columns.tolist()[:8]}")

    off = pick_col(df, "AdjO","AdjOE","Adj Off","AdjOff","Adj. Offense")
    de  = pick_col(df, "AdjD","AdjDE","Adj Def","AdjDef","Adj. Defense")

    out = df[[team_col, off, de]].copy()
    out.columns = ["Team","AdjO","AdjD"]
    out["AdjO"] = pd.to_numeric(out["AdjO"], errors="coerce")
    out["AdjD"] = pd.to_numeric(out["AdjD"], errors="coerce")
    out["TEAM_KEY"] = out["Team"].map(canonical)
    out = out.dropna(subset=["AdjO","AdjD","TEAM_KEY"]).reset_index(drop=True)
    return out

def load_torvik() -> pd.DataFrame:
    log_write(f"[INFO] Date={datetime.utcnow().date()} HCA={HCA_PTS:.1f} Edge={EDGE_THRESHOLD:.1f}")
    log_write(f"[INFO] Loading Torvik from {TORVIK_URL}")
    text = None
    try:
        r = requests.get(TORVIK_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        text = r.text
    except Exception as e:
        log_write(f"[WARN] Torvik request failed: {e}")

    if text:
        first = text.splitlines()[0] if text.splitlines() else ""
        looks_csv = ("," in first) and ("<html" not in text.lower())
        if looks_csv:
            try:
                df = pd.read_csv(io.StringIO(text))
                log_write(f"[INFO] Loaded Torvik rows (CSV): {len(df)}")
                return coerce_ratings(df)
            except Exception as e:
                log_write(f"[WARN] CSV parse failed; trying HTML: {e}")

        # HTML fallback (lxml/html5lib if available)
        try:
            flavors = []
            try: import lxml  # noqa
            except Exception: pass
            else: flavors.append("lxml")
            try: import html5lib  # noqa
            except Exception: pass
            else: flavors.append("html5lib")

            tables = pd.read_html(io.StringIO(text), flavor=flavors or None)
            if tables:
                dfh = max(tables, key=lambda t: t.shape[0])
                log_write(f"[INFO] Loaded Torvik rows (HTML): {len(dfh)}")
                return coerce_ratings(dfh)
            else:
                log_write("[ERROR] No tables found in Torvik HTML.")
        except Exception as e:
            log_write(f"[ERROR] HTML parse failed: {e}")

    # Local backup (optional)
    if TORVIK_BACKUP and os.path.exists(TORVIK_BACKUP):
        try:
            dfb = pd.read_csv(TORVIK_BACKUP)
            log_write(f"[INFO] Loaded Torvik backup: {TORVIK_BACKUP} ({len(dfb)} rows)")
            return coerce_ratings(dfb)
        except Exception as e:
            log_write(f"[WARN] Could not parse backup {TORVIK_BACKUP}: {e}")

    log_write("[ERROR] Ratings unavailable after CSV/HTML/backup.")
    return pd.DataFrame(columns=["Team","AdjO","AdjD","TEAM_KEY"])

# ------------ Odds ------------
def coerce_odds(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    home = cols.get("home") or cols.get("home_team") or cols.get("h_team")
    away = cols.get("away") or cols.get("away_team") or cols.get("a_team")
    hspr = cols.get("home_spread") or cols.get("spread") or cols.get("h_spread")
    if not (home and away and hspr):
        # loose guess
        def find_like(ks):
            for c in df.columns:
                lc = c.lower()
                if any(k in lc for k in ks): return c
            return None
        home = home or find_like(["home"])
        away = away or find_like(["away"])
        hspr = hspr or find_like(["spread","line"])
    if not (home and away and hspr):
        return pd.DataFrame(columns=["home","away","home_spread","HOME_KEY","AWAY_KEY"])

    out = df[[home,away,hspr]].copy()
    out.columns = ["home","away","home_spread"]
    out["home"] = out["home"].astype(str).str.strip()
    out["away"] = out["away"].astype(str).str.strip()
    out["home_spread"] = pd.to_numeric(out["home_spread"], errors="coerce")
    out = out.dropna(subset=["home","away","home_spread"]).reset_index(drop=True)
    out["HOME_KEY"] = out["home"].map(canonical)
    out["AWAY_KEY"] = out["away"].map(canonical)
    return out

def load_odds() -> pd.DataFrame:
    # 1) local CSV if present
    if ODDS_CSV and os.path.exists(ODDS_CSV):
        try:
            df = pd.read_csv(ODDS_CSV)
            log_write(f"[INFO] Loaded odds from local CSV: {ODDS_CSV} ({len(df)} rows)")
            return coerce_odds(df)
        except Exception as e:
            log_write(f"[WARN] Local odds parse failed: {e}")

    # 2) best-effort scrape (often JS, likely to be empty—this is optional)
    try:
        url = "https://www.vegasinsider.com/college-basketball/odds/spreads/"
        r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
        if r.ok:
            soup = BeautifulSoup(r.text, "html.parser")
            tables = soup.find_all("table")
            if tables:
                df = pd.read_html(str(tables[0]))[0]
                log_write("[INFO] Parsed a spreads table from backup site (best-effort).")
                return coerce_odds(df)
    except Exception as e:
        log_write(f"[WARN] Backup odds scrape failed: {e}")

    log_write("[WARN] No odds available.")
    return pd.DataFrame(columns=["home","away","home_spread","HOME_KEY","AWAY_KEY"])

# ------------ Model ------------
def join_and_score(ratings: pd.DataFrame, odds: pd.DataFrame) -> pd.DataFrame:
    r_home = ratings.add_prefix("h_").rename(columns={"h_TEAM_KEY":"HOME_KEY"})
    r_away = ratings.add_prefix("a_").rename(columns={"a_TEAM_KEY":"AWAY_KEY"})
    df = odds.merge(r_home, on="HOME_KEY", how="left").merge(r_away, on="AWAY_KEY", how="left")

    df["model_home_margin"] = (df["h_AdjO"] - df["a_AdjD"]) - (df["a_AdjO"] - df["h_AdjD"]) + HCA_PTS
    df["market_home_margin"] = -df["home_spread"]
    df["edge_pts"] = df["model_home_margin"] - df["market_home_margin"]
    df["ticket"] = df.apply(lambda r: f"{r['home']} {r['home_spread']:+.1f}" if pd.notna(r["home_spread"]) else "", axis=1)

    keep = ["home","away","home_spread","h_AdjO","h_AdjD","a_AdjO","a_AdjD",
            "model_home_margin","market_home_margin","edge_pts","ticket"]
    for k in keep:
        if k not in df.columns: df[k] = pd.NA
    df = df.sort_values("edge_pts", ascending=False, na_position="last").reset_index(drop=True)
    return df[keep]

# ------------ Main ------------
def main():
    try:
        ratings = load_torvik()
        odds = load_odds()

        if ratings.empty or odds.empty:
            note = []
            if ratings.empty: note.append("Ratings unavailable.")
            if odds.empty:    note.append("Odds unavailable.")
            note = " ".join(note) if note else "Upstream data unavailable."
            write_index(False, 0, 0, note=note)
            # EXIT SUCCESS so Pages step still runs
            return

        df = join_and_score(ratings, odds)
        ensure_dir(OUTPUT_DIR)
        full = os.path.join(OUTPUT_DIR, "edges_full.csv")
        top  = os.path.join(OUTPUT_DIR, "edges_top.csv")
        df.to_csv(full, index=False)
        df[df["edge_pts"].abs() >= EDGE_THRESHOLD].to_csv(top, index=False)
        n_games = len(df)
        n_edges = int((df["edge_pts"].abs() >= EDGE_THRESHOLD).sum())
        log_write(f"[INFO] Wrote {full} ({n_games} rows)")
        log_write(f"[INFO] Wrote {top} ({n_edges} rows with |edge| >= {EDGE_THRESHOLD})")
        write_index(True, n_games, n_edges)

    except Exception as e:
        log_write(f"[FATAL] {e}")
        log_write(traceback.format_exc())
        write_index(False, 0, 0, note="Unexpected error. See build_log.txt.")
        # EXIT SUCCESS to allow GitHub Pages to deploy the error page
        return

if __name__ == "__main__":
    main()
