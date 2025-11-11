#!/usr/bin/env python3
import os, io, csv, sys, datetime as dt
import pandas as pd
import numpy as np
import requests
from pandas.errors import ParserError

# ---------- config from env ----------
HOME_COURT_POINTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "site")

TODAY = dt.date.today()

# ---------- utils ----------
def _mkdir(p):
    os.makedirs(p, exist_ok=True)

def _safe_read_csv(text: str) -> pd.DataFrame:
    """
    Torvik CSV sometimes has quirky lines. Try normal parse, then tolerant.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")
    buf = io.StringIO(text)
    try:
        return pd.read_csv(buf)
    except ParserError:
        buf.seek(0)
        # tolerant parse: python engine, skip bad lines, keep commas
        return pd.read_csv(
            buf, engine="python", sep=",", on_bad_lines="skip", quoting=csv.QUOTE_MINIMAL
        )

def load_torvik(run_date: dt.date) -> pd.DataFrame:
    """
    Returns ratings with at least: team, AdjO, AdjD.
    Tries CSV endpoint, then HTML table, then optional backup file.
    """
    year = run_date.year
    url_csv = f"https://barttorvik.com/trank.php?year={year}&csv=1"
    try:
        r = requests.get(url_csv, timeout=30)
        r.raise_for_status()
        df = _safe_read_csv(r.text)
    except Exception:
        # fallback to HTML table
        try:
            url_html = f"https://barttorvik.com/trank.php?year={year}"
            r = requests.get(url_html, timeout=30)
            r.raise_for_status()
            tables = pd.read_html(r.text)
            # pick the largest table (usually the team ratings)
            df = max(tables, key=lambda t: t.shape[0] * t.shape[1])
        except Exception:
            # optional local backup
            bkp = "data/torvik_backup.csv"
            if os.path.exists(bkp):
                df = _safe_read_csv(open(bkp, "r", encoding="utf-8").read())
            else:
                raise RuntimeError("Unable to load ratings from Torvik (CSV/HTML/backup all failed)")

    # normalize expected column names
    cols = {c.lower().strip(): c for c in df.columns}
    def pick(*names):
        for n in names:
            if n in cols: return cols[n]
        raise KeyError(f"Missing expected column among {names}")

    # team name
    team_col = pick("team", "school", "Team")
    # adjusted offense/defense (Torvik often uses AdjO, AdjD or adj_o, adj_d)
    adjo_col = pick("adjo", "adj_o", "AdjO")
    adjd_col = pick("adjd", "adj_d", "AdjD")

    out = df[[team_col, adjo_col, adjd_col]].copy()
    out.columns = ["team", "AdjO", "AdjD"]

    # strip whitespace safely
    out["team"] = out["team"].astype(str).str.strip()
    # coerce numerics
    out["AdjO"] = pd.to_numeric(out["AdjO"], errors="coerce")
    out["AdjD"] = pd.to_numeric(out["AdjD"], errors="coerce")
    out = out.dropna(subset=["AdjO", "AdjD"]).reset_index(drop=True)

    return out

def _normalize_and_dedupe(raw_odds: pd.DataFrame) -> pd.DataFrame:
    """
    Expects raw odds with home/away team & spreads. Clean names.
    """
    df = raw_odds.copy()

    # IMPORTANT: use .str.strip() and assign back (fixes your Series .strip error)
    for c in ["home", "away", "home_spread"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # fix weird 'PK' or 'None'
    if "home_spread" in df.columns:
        df["home_spread"] = (
            df["home_spread"]
            .replace({"PK": 0, "pk": 0, "None": np.nan, "nan": np.nan})
        )
        df["home_spread"] = pd.to_numeric(df["home_spread"], errors="coerce")

    # dedupe by (home, away)
    if {"home", "away"}.issubset(df.columns):
        df = (df.sort_values(by=["home", "away"])
                .drop_duplicates(subset=["home", "away"], keep="first")
                .reset_index(drop=True))
    return df

def get_market_odds(run_date: dt.date) -> pd.DataFrame:
    """
    YOUR existing odds-scrape. Keep as-is; just make sure it returns:
    columns: home, away, home_spread  (home_spread is home - points)
    """
    # TODO: replace with your current scraper import if it lives elsewhere.
    # Here is a minimal placeholder that should be overwritten by your module.
    raise NotImplementedError("Wire this to your existing odds scraper function.")

def model_join(odds_df: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Join odds to ratings and compute model margin and edge.
    """
    r = ratings.rename(columns={"team":"Team"})
    # simple fuzzy: exact join first
    left = odds_df.merge(ratings, left_on="home", right_on="team", how="left")
    left = left.merge(ratings, left_on="away", right_on="team", how="left", suffixes=("_h","_a"))

    # compute model margin (home minus away); you can plug your exact formula here
    # Here: (AdjO_h - AdjD_a) - (AdjO_a - AdjD_h) + home court
    left["model_home_margin"] = (
        (left["AdjO_h"] - left["AdjD_a"]) - (left["AdjO_a"] - left["AdjD_h"]) + HOME_COURT_POINTS
    )

    # edge vs market
    left["market_home_margin"] = -left["home_spread"]  # spread is away+? ensure sign convention
    left["edge_pts"] = left["model_home_margin"] - left["market_home_margin"]

    # ticket format like "BYU +35.5"
    def ticket_row(row):
        try:
            side = row["home"] if row["edge_pts"] >= 0 else row["away"]
            num = row["home_spread"]
            if pd.isna(num): return ""
            # If model likes home (edge>=0), we take home at minus spread; if you want plus/minus swap, adjust here.
            if row["edge_pts"] >= 0:
                return f"{row['home']} {num:+.1f}".replace("+-", "-")
            else:
                return f"{row['away']} {(-num):+.1f}".replace("+-", "-")
        except Exception:
            return ""

    left["ticket"] = left.apply(ticket_row, axis=1)

    want_cols = [
        "home","away","home_spread",
        "AdjO_h","AdjD_h","AdjO_a","AdjD_a",
        "model_home_margin","market_home_margin","edge_pts","ticket"
    ]
    # column renames to match your sample
    final = left[want_cols].rename(columns={
        "AdjO_h":"h_AdjO","AdjD_h":"h_AdjD","AdjO_a":"a_AdjO","AdjD_a":"a_AdjD"
    })

    return final

def save_outputs(games_df: pd.DataFrame, edges_df: pd.DataFrame):
    _mkdir(OUTPUT_DIR)
    games_path = os.path.join(OUTPUT_DIR, "games.csv")
    edges_path = os.path.join(OUTPUT_DIR, "edges.csv")
    games_df.to_csv(games_path, index=False)
    edges_df.to_csv(edges_path, index=False)
    print(f"[INFO] wrote {games_path}")
    print(f"[INFO] wrote {edges_path}")

# ---------- main ----------
def main():
    _mkdir(OUTPUT_DIR)

    # 1) ratings
    print(f"[INFO] Date={TODAY}  HCA={HOME_COURT_POINTS}  Edge={EDGE_THRESHOLD}")
    ratings = load_torvik(TODAY)
    print(f"[INFO] Loaded Torvik rows: {len(ratings)}")

    # 2) odds
    raw = get_market_odds(TODAY)          # <-- your scraper
    odds = _normalize_and_dedupe(raw)
    print(f"[INFO] Loaded odds rows: {len(odds)}")

    # 3) model + edges
    joined = model_join(odds, ratings)
    # filter to games with market spread present
    edges = joined.dropna(subset=["home_spread", "market_home_margin"]).copy()
    edges = edges.sort_values("edge_pts", ascending=False).reset_index(drop=True)

    # also give a lighter “games” file if you like
    games = joined[["home","away","home_spread","model_home_margin","edge_pts"]].copy()

    save_outputs(games, edges)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # always leave a non-empty site with the error log
        _mkdir(OUTPUT_DIR)
        with open(os.path.join(OUTPUT_DIR, "build_log.txt"), "a", encoding="utf-8") as f:
            f.write(f"FATAL: {repr(e)}\n")
        raise
