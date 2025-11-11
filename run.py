#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build & Publish NCAAB Edges
- Loads Torvik ratings (CSV; falls back to HTML table if CSV isn't returned)
- Scrapes or reads market spreads
- Computes model vs market edge
- Publishes CSVs + a simple HTML index into ./site

Environment vars honored:
  HOME_COURT_POINTS (float, default 0.6)
  EDGE_THRESHOLD     (float, default 2.0)  -> for edges_top.csv
  OUTPUT_DIR         (str,   default "site")
  TORVIK_URL         (str,   optional)     -> override ratings source
  ODDS_CSV           (str,   optional)     -> local odds CSV fallback path
"""

from __future__ import annotations

import io
import os
import re
import sys
import json
import time
import math
import shutil
import logging
import traceback
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup  # only used for a backup odds grab (non-essential)


# =========================
# Config / Env
# =========================
HCA_PTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "site").strip() or "site"

# Reasonable default Torvik endpoint (often returns HTML when CSV blocked)
TORVIK_URL = os.getenv(
    "TORVIK_URL",
    # Year doesn't matter as much if source returns current season
    "https://barttorvik.com/trank.php?year=2025&csv=1"
)

# Optional pre-scraped odds CSV path (used if live scrape fails)
ODDS_CSV = os.getenv("ODDS_CSV", "data/odds.csv")


# =========================
# Utilities / Logging
# =========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_dir(OUTPUT_DIR)
    log = logging.getLogger("build")
    log.setLevel(logging.INFO)
    log.handlers.clear()

    fmt = logging.Formatter("[%(asctime)s UTC] [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)

    fh = logging.FileHandler(os.path.join(OUTPUT_DIR, "build_log.txt"), mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)

    return log


log = setup_logger()


# =========================
# Name normalization
# =========================
NONALNUM = re.compile(r"[^A-Z0-9]+")


def norm_team(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    # common quick replacements
    s = s.replace("St.", "State")
    s = s.replace("UC-", "UC ")
    s = s.replace(" Cal ", " California ")
    s = s.replace("&", "and")
    s = s.replace("'", "")
    up = NONALNUM.sub("", s.upper())
    return up


# Known alias mapping (add as needed)
ALIASES = {
    "OLEMISS": "MISSISSIPPI",
    "CENTRALCONNECTICUTSTATE": "CENTRALCONNECTICUT",
    "SAINTJOHNS": "STJOHNS",
    "SAINTJOSEPHS": "STJOSEPHS",
    "ILLCHICAGO": "UIC",
    "TEXASAAMCORPUSCHRISTI": "TEXASAAMCORPUSCHRISTI",
    "LOYOLOMARYMOUNT": "LOYOLAMARYMOUNT",
    "UCALIFORNIASANTABARBARA": "UCSB",
}


def canonical(name: str) -> str:
    n = norm_team(name)
    return ALIASES.get(n, n)


# =========================
# Ratings loader (Torvik)
# =========================
def _pick(df: pd.DataFrame, *candidates: str) -> str:
    """
    Return the first existing column from candidates; else raise KeyError with
    a helpful error showing the first few columns.
    """
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(
        f"Missing expected column among {candidates} "
        f"(first cols: {list(df.columns[:5])})"
    )


def _maybe_float(s):
    try:
        return float(s)
    except Exception:
        return math.nan


def load_torvik() -> pd.DataFrame:
    """
    Attempts:
      1) request TORVIK_URL expecting CSV; if it smells like HTML, parse table
      2) raise with a clean message on failure
    Returns cols: ['Team','AdjO','AdjD'] at minimum.
    """
    log.info("Loading Torvik from %s", TORVIK_URL)
    r = requests.get(TORVIK_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    text = r.text

    # Heuristic: if it looks like CSV with commas in first line, try read_csv
    looks_csv = "," in text.splitlines()[0] and "<html" not in text.lower()

    if looks_csv:
        try:
            df = pd.read_csv(io.StringIO(text))
            log.info("Loaded Torvik rows (CSV): %d", len(df))
            return _coerce_ratings(df)
        except Exception as e:
            log.warning("CSV fetch/parse failed (%s); trying HTML table", e)

    # HTML fallback
    # Use lxml if available; else html5lib
    flavors = []
    try:
        import lxml  # noqa
        flavors.append("lxml")
    except Exception:
        pass
    try:
        import html5lib  # noqa
        flavors.append("html5lib")
    except Exception:
        pass

    if not flavors:
        raise ImportError(
            "Missing optional dependency 'lxml' or 'html5lib'. "
            "Install one of them so pandas.read_html can parse the fallback table."
        )

    tables = pd.read_html(io.StringIO(text), flavor=flavors or None)
    if not tables:
        raise RuntimeError("No tables found in Torvik HTML response.")

    # Pick the largest table
    df = max(tables, key=lambda t: t.shape[0])
    log.info("Loaded Torvik rows (HTML): %d", len(df))
    return _coerce_ratings(df)


def _coerce_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize columns to: Team, AdjO, AdjD
    Accepts various column spellings from Torvik exports.
    """
    # Find team column
    team_col = None
    for c in ["Team", "team", "school", "School", "TEAM"]:
        if c in df.columns:
            team_col = c
            break
    if not team_col:
        # try infer by common contents
        for c in df.columns:
            if df[c].astype(str).str.contains("State|College|University|A&M|St\\.|Saint", case=False, regex=True).any():
                team_col = c
                break
    if not team_col:
        raise KeyError(f"Couldn't find team column in ratings. Head: {df.columns[:8].tolist()}")

    # Find offensive/defensive efficiency columns
    # Torvik sometimes uses AdjOE/AdjDE; sometimes AdjO/AdjD
    def_col_candidates = ["AdjD", "AdjDE", "Adj Def", "AdjDef", "Adj. Defense"]
    off_col_candidates = ["AdjO", "AdjOE", "Adj Off", "AdjOff", "Adj. Offense"]

    adjd = _pick(df, *[c for c in def_col_candidates if c in df.columns] + def_col_candidates)
    adjo = _pick(df, *[c for c in off_col_candidates if c in df.columns] + off_col_candidates)

    out = df[[team_col, adjo, adjd]].copy()
    out.columns = ["Team", "AdjO", "AdjD"]
    # numeric
    out["AdjO"] = pd.to_numeric(out["AdjO"], errors="coerce")
    out["AdjD"] = pd.to_numeric(out["AdjD"], errors="coerce")
    out["TEAM_KEY"] = out["Team"].map(canonical)

    # Drop NA rows
    out = out.dropna(subset=["AdjO", "AdjD", "TEAM_KEY"]).reset_index(drop=True)
    return out


# =========================
# Odds loader (spreads)
# =========================
def load_odds() -> pd.DataFrame:
    """
    Strategy:
      1) If a local CSV exists at ODDS_CSV, use it.
         Expected columns (case-insensitive variants handled):
             home, away, home_spread   (home is + if home underdog)
      2) Try a very light HTML scrape as a backup (best-effort).
      3) Return empty DataFrame if nothing works.
    """
    # 1) Local CSV
    if ODDS_CSV and os.path.exists(ODDS_CSV):
        log.info("Loading odds from local CSV: %s", ODDS_CSV)
        df = pd.read_csv(ODDS_CSV)
        return _coerce_odds(df)

    # 2) Very light backup (best-effort) – keep robust & optional
    try:
        # Example endpoint placeholder; you can wire your real odds source here.
        # If this 404s/blocks, we just return empty and still publish.
        url = "https://www.vegasinsider.com/college-basketball/odds/spreads/"  # generic page
        log.info("Attempting best-effort scrape from: %s", url)
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        if r.ok:
            soup = BeautifulSoup(r.text, "html.parser")
            # This is intentionally generic; many pages render via JS and won't work in CI.
            # So we mostly expect to fall back to empty.
            tables = soup.find_all("table")
            if tables:
                df = pd.read_html(str(tables[0]))[0]
                # Try to guess columns
                # Users will typically replace this with their own scraper anyway.
                return _coerce_odds(df)
    except Exception as e:
        log.warning("Backup odds scrape failed: %s", e)

    log.warning("No odds source available; proceeding with empty odds.")
    return pd.DataFrame(columns=["home", "away", "home_spread"])


def _coerce_odds(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    home = cols.get("home") or cols.get("home_team") or cols.get("h_team") or cols.get("homeName")
    away = cols.get("away") or cols.get("away_team") or cols.get("a_team") or cols.get("awayName")
    hspr = cols.get("home_spread") or cols.get("spread") or cols.get("h_spread")

    if not (home and away and hspr):
        # Try a very loose rename by substring
        def find_like(keywords):
            for c in df.columns:
                lc = c.lower()
                if any(k in lc for k in keywords):
                    return c
            return None

        home = home or find_like(["home"])
        away = away or find_like(["away"])
        hspr = hspr or find_like(["spread", "line"])

    if not (home and away and hspr):
        log.warning("Could not confidently map odds columns; got columns: %s", list(df.columns))
        return pd.DataFrame(columns=["home", "away", "home_spread"])

    out = df[[home, away, hspr]].copy()
    out.columns = ["home", "away", "home_spread"]
    out["home"] = out["home"].astype(str).str.strip()
    out["away"] = out["away"].astype(str).str.strip()
    out["home_spread"] = pd.to_numeric(out["home_spread"], errors="coerce")
    out = out.dropna(subset=["home", "away", "home_spread"]).reset_index(drop=True)

    # Canonical keys
    out["HOME_KEY"] = out["home"].map(canonical)
    out["AWAY_KEY"] = out["away"].map(canonical)
    return out


# =========================
# Edge model
# =========================
def join_and_score(ratings: pd.DataFrame, odds: pd.DataFrame) -> pd.DataFrame:
    # Self-join ratings for home/away
    r_home = ratings.add_prefix("h_")
    r_home = r_home.rename(columns={"h_TEAM_KEY": "HOME_KEY"})
    r_away = ratings.add_prefix("a_")
    r_away = r_away.rename(columns={"a_TEAM_KEY": "AWAY_KEY"})

    df = odds.merge(r_home, on="HOME_KEY", how="left").merge(r_away, on="AWAY_KEY", how="left")

    # Compute a basic margin model:
    #   model_home_margin ≈ (h.AdjO - a.AdjD) - (a.AdjO - h.AdjD) + HCA
    df["model_home_margin"] = (
        (df["h_AdjO"] - df["a_AdjD"]) - (df["a_AdjO"] - df["h_AdjD"]) + HCA_PTS
    )

    # Market convention in your tables: market_home_margin = -home_spread
    df["market_home_margin"] = -df["home_spread"]

    df["edge_pts"] = df["model_home_margin"] - df["market_home_margin"]

    # Ticket string (match your earlier style "BYU +35.5")
    def fmt_ticket(row):
        try:
            return f"{row['home']} {row['home_spread']:+.1f}"
        except Exception:
            return ""

    df["ticket"] = df.apply(fmt_ticket, axis=1)

    # Trim to the columns you wanted to keep
    keep = [
        "home", "away", "home_spread",
        "h_AdjO", "h_AdjD", "a_AdjO", "a_AdjD",
        "model_home_margin", "market_home_margin", "edge_pts", "ticket"
    ]
    for k in keep:
        if k not in df.columns:
            df[k] = pd.NA

    # Sort by absolute edge descending
    df = df.sort_values("edge_pts", ascending=False, na_position="last").reset_index(drop=True)
    return df[keep]


# =========================
# HTML Writer
# =========================
def write_index(has_csv: bool, n_games: int, n_edges: int):
    ensure_dir(OUTPUT_DIR)
    path = os.path.join(OUTPUT_DIR, "index.html")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if not has_csv:
        body = f"""
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>NCAAB Daily Edges</title></head>
<body style="font-family: Georgia, serif; padding: 16px;">
  <h1>NCAAB Daily Edges</h1>
  <p>Latest run artifacts below. (Auto-published)</p>
  <h2>No CSV outputs found</h2>
  <p>See <a href="build_log.txt">build_log.txt</a> for details.</p>
  <p style="color:#555;">Updated: {now}</p>
</body></html>
"""
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        log.info("Wrote %s", path)
        return

    body = f"""
<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>NCAAB Daily Edges</title></head>
<body style="font-family: Georgia, serif; padding: 16px;">
  <h1>NCAAB Daily Edges</h1>
  <p>Latest run artifacts below. (Auto-published)</p>

  <ul>
    <li><a href="edges_full.csv">edges_full.csv</a> (all matched games; {n_games} rows)</li>
    <li><a href="edges_top.csv">edges_top.csv</a> (edges ≥ {EDGE_THRESHOLD} pts; {n_edges} rows)</li>
    <li><a href="build_log.txt">build_log.txt</a></li>
  </ul>

  <p style="color:#555;">Updated: {now}</p>
</body></html>
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    log.info("Wrote %s", path)


# =========================
# Main
# =========================
def main():
    try:
        log.info("Date=%s HCA=%.1f Edge=%.1f", datetime.utcnow().date(), HCA_PTS, EDGE_THRESHOLD)

        ratings = load_torvik()
        odds = load_odds()

        if odds.empty:
            log.warning("No odds loaded. Will still publish a page.")
            # We still generate an index.html with "No CSV outputs found"
            write_index(has_csv=False, n_games=0, n_edges=0)
            return

        df = join_and_score(ratings, odds)

        ensure_dir(OUTPUT_DIR)
        full_path = os.path.join(OUTPUT_DIR, "edges_full.csv")
        top_path = os.path.join(OUTPUT_DIR, "edges_top.csv")

        df.to_csv(full_path, index=False)
        df[df["edge_pts"].abs() >= EDGE_THRESHOLD].to_csv(top_path, index=False)

        n_games = len(df)
        n_edges = int((df["edge_pts"].abs() >= EDGE_THRESHOLD).sum())

        log.info("Wrote %s (%d rows)", full_path, n_games)
        log.info("Wrote %s (%d rows with |edge| >= %.1f)", top_path, n_edges, EDGE_THRESHOLD)

        write_index(has_csv=True, n_games=n_games, n_edges=n_edges)

    except Exception as e:
        log.error("[FATAL] %s", e)
        log.error(traceback.format_exc())
        # Always publish an index (so Pages updates) even on failure
        write_index(has_csv=False, n_games=0, n_edges=0)
        # non-zero exit so Actions shows failure
        sys.exit(1)


if __name__ == "__main__":
    main()
