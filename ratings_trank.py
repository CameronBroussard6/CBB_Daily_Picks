import datetime as dt
import io
import requests
import pandas as pd
from unidecode import unidecode

CANDIDATE_ENDPOINTS = [
    "https://barttorvik.com/{year}_team_results.csv",
    "https://barttorvik.com/{year}_fffinal.csv",
]

def season_year_for_date(d: dt.date) -> int:
    # CBB season is labeled by the spring year (e.g., 2025-26 -> 2026)
    return d.year + 1 if d.month >= 7 else d.year

def _download_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def _norm(s: str) -> str:
    s = unidecode((s or "")).strip().lower()
    return " ".join(s.replace("_", " ").replace("-", " ").split())

def _first_present(cols_map, candidates):
    """Return original column name for first normalized candidate present."""
    for cand in candidates:
        if cand in cols_map:
            return cols_map[cand]
    return None

def load_trank_team_eff(date: dt.date) -> pd.DataFrame:
    year = season_year_for_date(date)
    last_err = None

    for tmpl in CANDIDATE_ENDPOINTS:
        url = tmpl.format(year=year)
        try:
            df = _download_csv(url)

            # Build a normalization map: normalized -> original
            norm_to_orig = {_norm(c): c for c in df.columns}

            # Find team column (Torvik uses 'Team' or similar)
            team_col = _first_present(
                norm_to_orig,
                ["team", "ncaa team", "name", "school"]
            )
            if not team_col:
                raise ValueError("Team column not found")

            # Find offensive & defensive efficiency
            # Common variants on Torvik exports:
            #   AdjOE / AdjDE, AdjO / AdjD, Adj Off / Adj Def, etc.
            adjo_col = _first_present(
                norm_to_orig,
                ["adjoe", "adjo", "adj off", "adj o", "adj offensive", "offensive efficiency", "off eff"]
            )
            adjd_col = _first_present(
                norm_to_orig,
                ["adjde", "adjd", "adj def", "adj d", "adj defensive", "defensive efficiency", "def eff"]
            )

            if not adjo_col or not adjd_col:
                # Try to heuristically pick columns that look like AdjO/AdjD
                # (start with 'adj' and contain 'o' or 'd')
                lowers = {c.lower(): c for c in df.columns}
                # crude scans
                maybe_o = [c for c in df.columns if c.lower().startswith("adjo") or "adj o" in c.lower()]
                maybe_d = [c for c in df.columns if c.lower().startswith("adjd") or "adj d" in c.lower()]
                if maybe_o and not adjo_col:
                    adjo_col = maybe_o[0]
                if maybe_d and not adjd_col:
                    adjd_col = maybe_d[0]

            if not adjo_col or not adjd_col:
                raise KeyError("Could not find AdjO/AdjD columns")

            out = df[[team_col, adjo_col, adjd_col]].copy()
            out.rename(columns={team_col: "Team", adjo_col: "AdjO", adjd_col: "AdjD"}, inplace=True)
            out["AdjEM"] = out["AdjO"] - out["AdjD"]
            return out.dropna(subset=["Team", "AdjO", "AdjD", "AdjEM"]).reset_index(drop=True)

        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Could not load Torvik efficiencies for {year}: {last_err}")
