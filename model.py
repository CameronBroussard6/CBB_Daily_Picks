# model.py
# Computes model edges from ratings + market spreads.
# Output schema (one row per lined game):
# home, away, home_spread, h_AdjO, h_AdjD, a_AdjO, a_AdjD,
# model_home_margin, market_home_margin, edge_pts, ticket

from typing import Tuple
import pandas as pd
import re

# Keep cleaner consistent with scraper
ALIASES = {
    "uc santa barbara": "cal santa barbara",
    "uc riverside": "cal riverside",
    "st. john's": "st johns",
    "saint joseph's": "saint josephs",
    "saint francis (pa)": "saint francis",
    "cal state northridge": "cal st. northridge",
    "csu northridge": "cal st. northridge",
    "central connecticut state": "central connecticut",
    "william & mary": "william and mary",
    "texas a&m-corpus christi": "texas a&m corpus chris",
    "texas a&m corpus christi": "texas a&m corpus chris",
    "mount st. mary's": "mount st. mary's",
}

def _clean(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"[‐–—−]", "-", s)  # dash variants -> "-"
    s = s.replace("&", "and")
    s = re.sub(r"[^\w\s\-']", " ", s)  # drop punctuation (keep word chars, dash, apostrophe)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return ALIASES.get(s, s)

def _pair_key(h: str, a: str) -> str:
    # Order-independent key (so neutral/flip joins don’t break)
    h1, a1 = _clean(h), _clean(a)
    return "|".join(sorted([h1, a1]))

def _prep_ratings(ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Expect a ratings DF with columns at least:
      Team, AdjO, AdjD
    Column names are case-insensitive and will be normalized.
    """
    df = ratings.copy()
    cols = {c.lower(): c for c in df.columns}
    # Flexible column grabs
    team_col = cols.get("team") or cols.get("name") or "Team"
    oj_col   = cols.get("adjo") or cols.get("adj_o") or "AdjO"
    dj_col   = cols.get("adjd") or cols.get("adj_d") or "AdjD"

    # Standardize names
    df = df.rename(columns={
        team_col: "Team",
        oj_col: "AdjO",
        dj_col: "AdjD",
    })
    df["team_clean"] = df["Team"].map(_clean)
    # Net efficiency we use for margin calc
    df["Net"] = df["AdjO"] - df["AdjD"]
    return df[["Team", "team_clean", "AdjO", "AdjD", "Net"]]

def compute_edges(
    ratings: pd.DataFrame,
    odds: pd.DataFrame,
    hca_points: float = 0.6,
    edge_threshold: float = 2.0,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    ratings: DataFrame with Team, AdjO, AdjD (or similar => normalized)
    odds:    DataFrame from scrape_odds.get_spreads()
             columns: home, away, home_spread, market_home_margin, likely_non_board
    Returns: (edges_df, diag_df)
    """
    r = _prep_ratings(ratings)

    o = odds.copy()
    # Clean names + keys
    o["home_clean"] = o["home"].map(_clean)
    o["away_clean"] = o["away"].map(_clean)
    o["__key"] = o.apply(lambda x: _pair_key(x["home"], x["away"]), axis=1)

    # Join ratings for both teams
    o = o.merge(r.add_prefix("h_"), left_on="home_clean", right_on="h_team_clean", how="left")
    o = o.merge(r.add_prefix("a_"), left_on="away_clean", right_on="a_team_clean", how="left")

    # Compute model home margin: (h_Net - a_Net) + HCA
    o["model_home_margin"] = (o["h_Net"] - o["a_Net"]) + hca_points

    # Edge vs market (market_home_margin already = "home by X")
    # edge_pts is model minus market; positive => model likes home by more
    o["edge_pts"] = o["model_home_margin"] - o["market_home_margin"]

    # Ticket label (just a quick readable pick, no “recommend” column)
    # Positive edge => lay with home (home -spread), Negative => take away
    def _ticket(row):
        mhm = row["market_home_margin"]
        if pd.isna(mhm):
            return ""
        if row["edge_pts"] >= edge_threshold:
            # Home side
            return f'{row["home"]} {mhm:+.1f}'.replace("+-", "-")
        elif row["edge_pts"] <= -edge_threshold:
            # Away side: market_home_margin = home by s -> away +s
            return f'{row["away"]} {(-mhm):+ .1f}'.replace("+ -", "-").replace("  ", " ")
        else:
            return ""  # small edge: no ticket

    o["ticket"] = o.apply(_ticket, axis=1)

    # Final tidy frame – only columns you asked for
    out = o[[
        "home", "away", "home_spread",
        "h_AdjO", "h_AdjD", "a_AdjO", "a_AdjD",
        "model_home_margin", "market_home_margin",
        "edge_pts", "ticket"
    ]].copy()

    # Sort by absolute edge descending (lined games first)
    out["__has_line"] = out["market_home_margin"].notna()
    out = out.sort_values(["__has_line", "edge_pts"], ascending=[False, False]).drop(columns="__has_line")

    # Diagnostics
    diag = pd.DataFrame([{
        "ratings_rows": len(ratings),
        "odds_rows": len(odds),
        "joined_rows": len(out),
        "lined_games": int(out["market_home_margin"].notna().sum()),
        "edge_ge_thresh": int((out["edge_pts"].abs() >= edge_threshold).sum()),
        "hca_points": hca_points,
        "edge_threshold": edge_threshold,
    }])

    return out.reset_index(drop=True), diag
