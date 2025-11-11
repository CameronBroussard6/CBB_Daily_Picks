# run.py
# Orchestrates: load ratings -> scrape odds -> compute edges -> write outputs.
# Produces CSV at ./out/cbb_edges_<YYYYMMDD>.csv and prints coverage stats.

import os
import sys
import datetime as dt
import pandas as pd
import requests
from io import StringIO

from scrape_odds import get_spreads  # your updated scraper from earlier
from model import compute_edges

TODAY = dt.date.today()

HCA_POINTS = float(os.getenv("HCA_POINTS", "0.6"))     # matches your logs
EDGE_THRESH = float(os.getenv("EDGE_THRESH", "2.0"))   # matches your logs

OUT_DIR = os.getenv("OUT_DIR", "out")
os.makedirs(OUT_DIR, exist_ok=True)

def _log(msg: str):
    print(f"[INFO] {msg}", flush=True)

def load_torvik(date: dt.date) -> pd.DataFrame:
    """
    Loads current team efficiencies from T-Rank.
    Fallback-friendly: trims to required columns.
    """
    # T-Rank 'Team' / 'AdjO' / 'AdjD' CSV endpoint
    # (Public page CSV; if this ever changes, swap to your cached file.)
    url = "https://barttorvik.com/trankteamo.php?year=2025&csv=1"  # adjust year as needed
    _log(f"Fetching Torvik ratings CSV …")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    # Normalize columns we need
    # Common headers on Torvik CSV: "Team", "AdjO", "AdjD" among many others
    needed = [c for c in df.columns if c.lower() in ("team","adjo","adj_o","adjd","adj_d")]
    df = df[needed].copy()
    _log(f"Loaded Torvik rows: {len(df)}")
    return df

def _normalize_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Safe string cleanup and dedupe (fixes the .strip AttributeError).
    """
    out = df.copy()
    for c in ("home", "away"):
        # Elementwise .str.* (not scalar .strip)
        out[c] = out[c].astype(str).str.strip()
    # Dedupe on unordered pair key to avoid repeats across sources
    from scrape_odds import _pair_key as _pair  # reuse same key for consistency
    out["__key"] = out.apply(lambda r: _pair(r["home"], r["away"]), axis=1)
    # Prefer rows that actually have spreads
    out["__has"] = out["market_home_margin"].notna()
    out = (out.sort_values(["__has"], ascending=[False])
               .drop_duplicates(subset="__key", keep="first"))
    return out.drop(columns=["__key","__has"])

def main(run_date: dt.date):
    _log(f"Date={run_date}   HCA={HCA_POINTS}   Edge={EDGE_THRESH}")

    # 1) Ratings
    ratings = load_torvik(run_date)

    # 2) Odds (multi-source inside)
    raw_odds = get_spreads(run_date)
    if raw_odds is None or raw_odds.empty:
        _log("No odds scraped; aborting with empty output.")
        print("")  # keep action from choking on no stdout
        sys.exit(0)

    # 3) Clean + dedupe odds (fix prior .strip bug)
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

    # 5) Keep only requested columns (already trimmed in model.py, but enforce here too)
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

    # 7) Print diagnostics (useful in Actions log)
    _log(f"Odds scraped (raw): {raw_count}  | deduped: {deduped}  | with spreads: {with_spread}")
    _log(f"Lined games in output: {int(edges['market_home_margin'].notna().sum())}")
    _log(f"Edges |>= {EDGE_THRESH:.1f}|: {int((edges['edge_pts'].abs() >= EDGE_THRESH).sum())}")
    _log(f"Wrote: {out_csv}")

    # Also echo a compact preview to stdout (first 10 lined games)
    preview = edges[edges["market_home_margin"].notna()].head(10)
    if not preview.empty:
        _log("Preview (top 10 lined games):")
        print(preview.to_string(index=False))

if __name__ == "__main__":
    main(TODAY)
