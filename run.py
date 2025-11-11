#!/usr/bin/env python3
import os, io, csv, sys, datetime as dt
import pandas as pd
import numpy as np
import requests
from pandas.errors import ParserError

# ---------- config ----------
HOME_COURT_POINTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "site")
TODAY = dt.date.today()

# ---------- fs helpers ----------
def _mkdir(p): os.makedirs(p, exist_ok=True)
def _log(msg):
    _mkdir(OUTPUT_DIR)
    with open(os.path.join(OUTPUT_DIR, "build_log.txt"), "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")
    print(msg)

# ---------- torvik handling ----------
def _safe_read_csv(text: str) -> pd.DataFrame:
    text = text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")
    buf = io.StringIO(text)
    try:
        return pd.read_csv(buf)
    except ParserError:
        buf.seek(0)
        return pd.read_csv(buf, engine="python", sep=",", on_bad_lines="skip", quoting=csv.QUOTE_MINIMAL)

def load_torvik(run_date: dt.date) -> pd.DataFrame:
    year = run_date.year
    url_csv = f"https://barttorvik.com/trank.php?year={year}&csv=1"
    try:
        r = requests.get(url_csv, timeout=30)
        r.raise_for_status()
        df = _safe_read_csv(r.text)
    except Exception as e_csv:
        _log(f"[WARN] Torvik CSV failed: {e_csv}")
        try:
            url_html = f"https://barttorvik.com/trank.php?year={year}"
            r = requests.get(url_html, timeout=30)
            r.raise_for_status()
            tables = pd.read_html(r.text)
            df = max(tables, key=lambda t: t.shape[0] * t.shape[1])
        except Exception as e_html:
            _log(f"[WARN] Torvik HTML fallback failed: {e_html}")
            bkp = "data/torvik_backup.csv"
            if os.path.exists(bkp):
                _log("[INFO] Using local backup: data/torvik_backup.csv")
                df = pd.read_csv(bkp)
            else:
                raise RuntimeError("Unable to load ratings from Torvik (CSV/HTML/backup all failed)")

    cols = {c.lower().strip(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n in cols: return cols[n]
        raise KeyError(f"Missing expected column among {names}")

    team_col = pick("team", "school", "Team")
    adjo_col = pick("adjo", "adj_o", "AdjO")
    adjd_col = pick("adjd", "adj_d", "AdjD")

    out = df[[team_col, adjo_col, adjd_col]].copy()
    out.columns = ["team", "AdjO", "AdjD"]
    out["team"] = out["team"].astype(str).str.strip()
    out["AdjO"] = pd.to_numeric(out["AdjO"], errors="coerce")
    out["AdjD"] = pd.to_numeric(out["AdjD"], errors="coerce")
    out = out.dropna(subset=["AdjO", "AdjD"]).reset_index(drop=True)
    return out

# ---------- odds acquisition ----------
def _normalize_and_dedupe(raw_odds: pd.DataFrame) -> pd.DataFrame:
    df = raw_odds.copy()
    for c in ["home", "away", "home_spread"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    if "home_spread" in df.columns:
        df["home_spread"] = df["home_spread"].replace({"PK": 0, "pk": 0, "None": np.nan, "nan": np.nan})
        df["home_spread"] = pd.to_numeric(df["home_spread"], errors="coerce")
    if {"home","away"}.issubset(df.columns):
        df = (df.sort_values(by=["home","away"])
                .drop_duplicates(subset=["home","away"], keep="first")
                .reset_index(drop=True))
    return df

def try_import_scraper():
    """
    Try:
      - odds.get_market_odds(date)
      - scraper.odds.get_market_odds(date)
    """
    try:
        from odds import get_market_odds as _gom
        return _gom
    except Exception:
        pass
    try:
        from scraper.odds import get_market_odds as _gom
        return _gom
    except Exception:
        pass
    return None

def load_market_odds(run_date: dt.date) -> pd.DataFrame | None:
    fn = try_import_scraper()
    if fn:
        _log("[INFO] Using project odds scraper module.")
        try:
            return fn(run_date)
        except Exception as e:
            _log(f"[WARN] Project odds scraper error: {e}")

    # CSV fallback
    csv_path = "data/odds.csv"
    if os.path.exists(csv_path):
        _log("[INFO] Using data/odds.csv fallback.")
        try:
            return pd.read_csv(csv_path)
        except Exception as e:
            _log(f"[WARN] Could not read data/odds.csv: {e}")
    return None

# ---------- model ----------
def model_join(odds_df: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    left = odds_df.merge(ratings, left_on="home", right_on="team", how="left")
    left = left.merge(ratings, left_on="away", right_on="team", how="left", suffixes=("_h","_a"))
    left["model_home_margin"] = (
        (left["AdjO_h"] - left["AdjD_a"]) - (left["AdjO_a"] - left["AdjD_h"]) + HOME_COURT_POINTS
    )
    left["market_home_margin"] = -left["home_spread"]
    left["edge_pts"] = left["model_home_margin"] - left["market_home_margin"]

    def ticket_row(row):
        try:
            num = row["home_spread"]
            if pd.isna(num): return ""
            if row["edge_pts"] >= 0:
                return f"{row['home']} {num:+.1f}".replace("+-","-")
            else:
                return f"{row['away']} {(-num):+.1f}".replace("+-","-")
        except Exception:
            return ""

    left["ticket"] = left.apply(ticket_row, axis=1)
    want = [
        "home","away","home_spread",
        "AdjO_h","AdjD_h","AdjO_a","AdjD_a",
        "model_home_margin","market_home_margin","edge_pts","ticket"
    ]
    return left[want].rename(columns={
        "AdjO_h":"h_AdjO","AdjD_h":"h_AdjD","AdjO_a":"a_AdjO","AdjD_a":"a_AdjD"
    })

# ---------- outputs ----------
def write_index():
    html = f"""<html><head><title>NCAAB Daily Edges</title></head><body>
<h1>NCAAB Daily Edges</h1>
<p>Latest run artifacts below. (Auto-published)</p>
<ul>
  <li><a href="edges.csv">edges.csv</a></li>
  <li><a href="games.csv">games.csv</a></li>
  <li><a href="build_log.txt">build_log.txt</a></li>
</ul>
<p>Updated: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC</p>
</body></html>"""
    with open(os.path.join(OUTPUT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

def save_outputs(games_df: pd.DataFrame, edges_df: pd.DataFrame):
    _mkdir(OUTPUT_DIR)
    games_df.to_csv(os.path.join(OUTPUT_DIR, "games.csv"), index=False)
    edges_df.to_csv(os.path.join(OUTPUT_DIR, "edges.csv"), index=False)
    write_index()

# ---------- main ----------
def main():
    _mkdir(OUTPUT_DIR)
    _log(f"[INFO] Date={TODAY} HCA={HOME_COURT_POINTS} Edge={EDGE_THRESHOLD}")

    # Ratings
    ratings = load_torvik(TODAY)
    _log(f"[INFO] Torvik ratings loaded: {len(ratings)} teams")

    # Odds (non-fatal)
    raw = load_market_odds(TODAY)
    if raw is None or raw.empty:
        _log("[ERROR] No odds data available. Writing empty outputs.")
        empty_games = pd.DataFrame(columns=["home","away","home_spread","model_home_margin","edge_pts"])
        empty_edges = pd.DataFrame(columns=[
            "home","away","home_spread","h_AdjO","h_AdjD","a_AdjO","a_AdjD",
            "model_home_margin","market_home_margin","edge_pts","ticket"
        ])
        save_outputs(empty_games, empty_edges)
        return

    odds = _normalize_and_dedupe(raw)
    _log(f"[INFO] Odds rows after clean/dedupe: {len(odds)}")

    joined = model_join(odds, ratings)
    edges = (joined.dropna(subset=["home_spread"])
                    .sort_values("edge_pts", ascending=False)
                    .reset_index(drop=True))
    games = joined[["home","away","home_spread","model_home_margin","edge_pts"]].copy()

    save_outputs(games, edges)
    _log(f"[INFO] Wrote {len(edges)} edges.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        _mkdir(OUTPUT_DIR)
        _log(f"FATAL: {repr(e)}")
        write_index()
        raise
