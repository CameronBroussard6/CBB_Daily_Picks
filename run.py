import os
import sys
import datetime as dt
import pandas as pd
import requests
from io import StringIO
from typing import Optional

from scrape_odds import get_spreads
from model import compute_edges

TODAY = dt.date.today()

HCA_POINTS = float(os.getenv("HCA_POINTS", "0.6"))
EDGE_THRESH = float(os.getenv("EDGE_THRESH", "2.0"))
OUT_DIR = os.getenv("OUT_DIR", "out")
TORVIK_YEAR = os.getenv("TORVIK_YEAR", "2025")
TORVIK_BACKUP = os.getenv("TORVIK_BACKUP", "data/torvik_backup.csv")  # now optional

os.makedirs(OUT_DIR, exist_ok=True)

def _log(msg: str):
    print(f"[INFO] {msg}", flush=True)

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols_lower = {str(c).lower(): c for c in df.columns}
    team = cols_lower.get("team") or cols_lower.get("name")
    adjo = cols_lower.get("adjo") or cols_lower.get("adj_o")
    adjd = cols_lower.get("adjd") or cols_lower.get("adj_d")
    if not (team and adjo and adjd):
        raise ValueError("Ratings table missing Team/AdjO/AdjD columns")
    out = df.rename(columns={team: "Team", adjo: "AdjO", adjd: "AdjD"})
    out = out[["Team", "AdjO", "AdjD"]].copy()
    # standardize team strings
    out["Team"] = out["Team"].astype(str).str.strip()
    return out

def _file_exists(path: Optional[str]) -> bool:
    return bool(path) and os.path.isfile(path)

def load_torvik() -> Optional[pd.DataFrame]:
    """
    Try Torvik CSV → HTML. If both fail and a backup file EXISTS, use it.
    If all fail, return None (fail-soft).
    """
    base = f"https://barttorvik.com/trankteamo.php?year={TORVIK_YEAR}"
    csv_url = f"{base}&csv=1"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36")
    }

    # 1) CSV
    try:
        _log("Fetching Torvik ratings (CSV)…")
        r = requests.get(csv_url, headers=headers, timeout=30)
        r.raise_for_status()
        text = r.text
        if "<html" in text.lower() or "<table" in text.lower():
            raise ValueError("CSV endpoint returned HTML")
        df = pd.read_csv(StringIO(text), engine="python", on_bad_lines="skip")
        df = _normalize_cols(df)
        _log(f"Loaded Torvik rows (CSV): {len(df)}")
        return df
    except Exception as e:
        _log(f"CSV load failed: {e}")

    # 2) HTML
    try:
        _log("Falling back to Torvik HTML table…")
        page = requests.get(base, headers=headers, timeout=30)
        page.raise_for_status()
        tables = pd.read_html(page.text)
        pick = None
        for t in tables:
            if any(str(c).lower() in ("team", "name") for c in t.columns):
                pick = t
                break
        if pick is None:
            raise ValueError("No suitable table with Team column found")
        df = _normalize_cols(pick)
        _log(f"Loaded Torvik rows (HTML): {len(df)}")
        return df
    except Exception as e:
        _log(f"HTML load failed: {e}")

    # 3) Optional local backup
    if _file_exists(TORVIK_BACKUP):
        try:
            _log(f"Using local backup: {TORVIK_BACKUP}")
            df = pd.read_csv(TORVIK_BACKUP)
            df = _normalize_cols(df)
            _log(f"Loaded Torvik rows (backup): {len(df)}")
            return df
        except Exception as e:
            _log(f"Backup load failed: {e}")
    else:
        _log("No local backup present (skipping).")

    _log("All Torvik sources failed. Continuing without ratings.")
    return None  # fail-soft

def _normalize_and_dedupe(odds: pd.DataFrame) -> pd.DataFrame:
    out = odds.copy()
    for c in ("home", "away"):
        out[c] = out[c].astype(str).str.strip()
    # keep first row with a real market line per matchup
    key = out["home"].str.lower().str.replace(r"\s+", " ", regex=True) + "@" + \
          out["away"].str.lower().str.replace(r"\s+", " ", regex=True)
    out["__key"] = key
    out["__hasline"] = out["market_home_margin"].notna()
    out = (out.sort_values(["__hasline"], ascending=[False])
              .drop_duplicates(subset="__key", keep="first")
              .drop(columns=["__key", "__hasline"]))
    return out

def main(run_date: dt.date):
    _log(f"Date={run_date}  HCA={HCA_POINTS}  Edge={EDGE_THRESH}")
    ratings = load_torvik()

    raw_odds = get_spreads(run_date)
    if raw_odds is None or raw_odds.empty:
        _log("No odds scraped; nothing to do.")
        sys.exit(0)

    odds = _normalize_and_dedupe(raw_odds)
    raw_count = len(raw_odds)
    deduped = len(odds)
    with_spread = int(odds["market_home_margin"].notna().sum())

    edges = compute_edges(
        ratings=ratings,
        odds=odds,
        hca_points=HCA_POINTS,
        edge_threshold=EDGE_THRESH,
    )

    # Column order (and remove the old recommend/h_Team/a_Team style cols)
    cols = [
        "home","away","home_spread",
        "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
        "model_home_margin","market_home_margin",
        "edge_pts","ticket",
    ]
    for c in cols:
        if c not in edges.columns:
            edges[c] = pd.NA
    edges = edges[cols]

    stamp = run_date.strftime("%Y%m%d")
    out_csv = os.path.join(OUT_DIR, f"cbb_edges_{stamp}.csv")
    edges.to_csv(out_csv, index=False)

    _log(f"Odds scraped (raw): {raw_count} | deduped games: {deduped} | with spreads: {with_spread}")
    lined = int(edges["market_home_margin"].notna().sum())
    _log(f"Lined games in output: {lined}")
    _log(f"Edges |>= {EDGE_THRESH:.1f}|: {int((edges['edge_pts'].abs() >= EDGE_THRESH).sum())}")
    _log(f"Wrote: {out_csv}")

    prev = edges[edges["market_home_margin"].notna()].head(10)
    if not prev.empty:
        _log("Preview (top 10 lined games):")
        print(prev.to_string(index=False))

if __name__ == "__main__":
    main(TODAY)
