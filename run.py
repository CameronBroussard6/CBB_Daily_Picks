# run.py
# Orchestrates: load ratings -> scrape odds -> compute edges -> write outputs.

import os
import sys
import datetime as dt
import pandas as pd
import requests
from io import StringIO

from scrape_odds import get_spreads  # unchanged
from model import compute_edges

TODAY = dt.date.today()

HCA_POINTS = float(os.getenv("HCA_POINTS", "0.6"))
EDGE_THRESH = float(os.getenv("EDGE_THRESH", "2.0"))
OUT_DIR = os.getenv("OUT_DIR", "out")
TORVIK_YEAR = os.getenv("TORVIK_YEAR", "2025")
TORVIK_BACKUP = os.getenv("TORVIK_BACKUP", "data/torvik_backup.csv")  # optional local fallback

os.makedirs(OUT_DIR, exist_ok=True)

def _log(msg: str):
    print(f"[INFO] {msg}", flush=True)

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only columns we need and standardize casing
    cols_lower = {c.lower(): c for c in df.columns}
    team = cols_lower.get("team") or cols_lower.get("name")
    adjo = cols_lower.get("adjo") or cols_lower.get("adj_o")
    adjd = cols_lower.get("adjd") or cols_lower.get("adj_d")
    if not (team and adjo and adjd):
        raise ValueError("Ratings table missing required columns Team/AdjO/AdjD")
    df = df.rename(columns={team: "Team", adjo: "AdjO", adjd: "AdjD"})
    return df[["Team", "AdjO", "AdjD"]].copy()

def load_torvik(date: dt.date) -> pd.DataFrame:
    """
    Robust loader:
      1) CSV endpoint with UA
      2) HTML table parse
      3) local CSV backup
    """
    base = f"https://barttorvik.com/trankteamo.php?year={TORVIK_YEAR}"
    csv_url = f"{base}&csv=1"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    # 1) Try CSV
    try:
        _log("Fetching Torvik ratings CSV …")
        r = requests.get(csv_url, headers=headers, timeout=30)
        r.raise_for_status()
        text = r.text
        # Guard: sometimes HTML is returned
        if "<html" in text.lower() or "<table" in text.lower():
            raise ValueError("CSV endpoint returned HTML")
        df = pd.read_csv(StringIO(text), engine="python", on_bad_lines="skip")
        df = _normalize_cols(df)
        _log(f"Loaded Torvik rows (CSV): {len(df)}")
        return df
    except Exception as e:
        _log(f"CSV load failed ({type(e).__name__}): {e}")

    # 2) Try HTML table
    try:
        _log("Falling back to Torvik HTML table …")
        rh = requests.get(base, headers=headers, timeout=30)
        rh.raise_for_status()
        tables = pd.read_html(rh.text, flavor="lxml")
        # pick the first table that contains 'Team'
        pick = None
        for t in tables:
            if any(c.lower() in ("team", "name") for c in t.columns.astype(str).str.lower()):
                pick = t
                break
        if pick is None:
            raise ValueError("No suitable table with Team column found")
        df = _normalize_cols(pick)
        _log(f"Loaded Torvik rows (HTML): {len(df)}")
        return df
    except Exception as e:
        _log(f"HTML load failed ({type(e).__name__}): {e}")

    # 3) Local backup
    try:
        _log(f"Falling back to local backup at {TORVIK_BACKUP!r} …")
        df = pd.read_csv(TORVIK_BACKUP)
        df = _normalize_cols(df)
        _log(f"Loaded Torvik rows (backup): {len(df)}")
        return df
    except Exception as e:
        _log(f"Backup load failed ({type(e).__name__}): {e}")
        raise RuntimeError("Unable to load ratings from Torvik (CSV/HTML/backup all failed)")

def _normalize_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ("home", "away"):
        out[c] = out[c].astype(str).str.strip()
    from scrape_odds import _pair_key as _pair
    out["__key"] = out.apply(lambda r: _pair(r["home"], r["away"]), axis=1)
    out["__has"] = out["market_home_margin"].notna()
    out = (out.sort_values(["__has"], ascending=[False])
               .drop_duplicates(subset="__key", keep="first"))
    return out.drop(columns=["__key", "__has"])

def main(run_date: dt.date):
    _log(f"Date={run_date}   HCA={HCA_POINTS}   Edge={EDGE_THRESH}")

    # 1) Ratings (robust)
    ratings = load_torvik(run_date)

    # 2) Odds
    raw_odds = get_spreads(run_date)
    if raw_odds is None or raw_odds.empty:
        _log("No odds scraped; aborting with empty output.")
        print("")
        sys.exit(0)

    # 3) Clean + dedupe odds
    odds = _normalize_and_dedupe(raw_odds)
    raw_count = len(raw_odds)
    deduped = len(odds)
    with_spread = int(odds["market_home_margin"].notna().sum())

    # 4) Compute model edges
    edges, diag = compute_edges(
        ratings=ratings,
        odds=odds,
        hca_points=HCA_POINTS,
        edge_threshold=EDGE_THRESH,
    )

    cols = [
        "home","away","home_spread",
        "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
        "model_home_margin","market_home_margin",
        "edge_pts","ticket",
    ]
    edges = edges[cols]

    # 6) Write outputs
    stamp = run_date.strftime("%Y%m%d")
    out_csv = os.path.join(OUT_DIR, f"cbb_edges_{stamp}.csv")
    edges.to_csv(out_csv, index=False)

    _log(f"Odds scraped (raw): {raw_count}  | deduped: {deduped}  | with spreads: {with_spread}")
    _log(f"Lined games in output: {int(edges['market_home_margin'].notna().sum())}")
    _log(f"Edges |>= {EDGE_THRESH:.1f}|: {int((edges['edge_pts'].abs() >= EDGE_THRESH).sum())}")
    _log(f"Wrote: {out_csv}")

    preview = edges[edges["market_home_margin"].notna()].head(10)
    if not preview.empty:
        _log("Preview (top 10 lined games):")
        print(preview.to_string(index=False))

if __name__ == "__main__":
    main(TODAY)
