import datetime as dt
import io, csv, requests, pandas as pd

# We use public CSV endpoints mentioned by Torvik (no login).
# Primary: YEAR_team_results.csv (has AdjO/AdjD/AdjEM during season)
# Fallback: YEAR_fffinal.csv (four factors; sometimes includes off/def eff labels)
CANDIDATE_ENDPOINTS = [
    "https://barttorvik.com/{year}_team_results.csv",
    "https://barttorvik.com/{year}_fffinal.csv",
]

def season_year_for_date(d: dt.date) -> int:
    # CBB season labeled by spring year (e.g., 2025-26 => 2026)
    return d.year + 1 if d.month >= 7 else d.year

def _download_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    # Some Torvik CSVs may be served text/plain; robust parse:
    return pd.read_csv(io.StringIO(r.text))

def load_trank_team_eff(date: dt.date) -> pd.DataFrame:
    year = season_year_for_date(date)
    last_err = None
    for tmpl in CANDIDATE_ENDPOINTS:
        url = tmpl.format(year=year)
        try:
            df = _download_csv(url)
            cols = {c.strip(): c for c in df.columns}
            # Try to find efficiency columns
            # Common Torvik headers: 'AdjOE','AdjDE','AdjEM' or 'AdjO','AdjD'
            cand_adjo = next((cols[k] for k in ["AdjOE","AdjO","Adj Off","Adj_O"]), None)
            cand_adjd = next((cols[k] for k in ["AdjDE","AdjD","Adj Def","Adj_D"]), None)
            if cand_adjo and cand_adjd:
                out = df.copy()
                out.rename(columns={cand_adjo:"AdjO", cand_adjd:"AdjD"}, inplace=True)
                if "AdjEM" not in out.columns:
                    out["AdjEM"] = out["AdjO"] - out["AdjD"]
                # Team column heuristics
                team_col = next((c for c in out.columns if c.lower() in ("team","ncaa_team","name")), None)
                if not team_col:
                    raise ValueError("Team column not found")
                out = out[[team_col,"AdjO","AdjD","AdjEM"]].rename(columns={team_col:"Team"})
                return out.dropna(subset=["Team","AdjO","AdjD","AdjEM"]).reset_index(drop=True)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not load Torvik efficiencies for {year}: {last_err}")
