import pandas as pd
from typing import Optional

def _std_team(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _prep_ratings(ratings: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if ratings is None or ratings.empty:
        return None
    df = ratings.copy()
    df["Team"] = _std_team(df["Team"])
    # numeric
    for c in ("AdjO", "AdjD"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Team"])
    return df

def _merge_ratings(odds: pd.DataFrame, ratings: Optional[pd.DataFrame]) -> pd.DataFrame:
    out = odds.copy()
    out["home"] = _std_team(out["home"])
    out["away"] = _std_team(out["away"])

    if ratings is None:
        # create empty rating columns so downstream doesn’t blow up
        out["h_AdjO"] = pd.NA
        out["h_AdjD"] = pd.NA
        out["a_AdjO"] = pd.NA
        out["a_AdjD"] = pd.NA
        return out

    r = _prep_ratings(ratings)
    if r is None:
        out["h_AdjO"] = pd.NA
        out["h_AdjD"] = pd.NA
        out["a_AdjO"] = pd.NA
        out["a_AdjD"] = pd.NA
        return out

    left = out.merge(r.rename(columns={"Team":"home","AdjO":"h_AdjO","AdjD":"h_AdjD"}),
                     on="home", how="left")
    both = left.merge(r.rename(columns={"Team":"away","AdjO":"a_AdjO","AdjD":"a_AdjD"}),
                      on="away", how="left")
    return both

def _model_margin(row, hca_points: float) -> Optional[float]:
    # Require all four ratings to compute a margin
    try:
        ha = float(row["h_AdjO"])
        hd = float(row["h_AdjD"])
        aa = float(row["a_AdjO"])
        ad = float(row["a_AdjD"])
    except Exception:
        return None
    # simple efficiency model (possessions scale cancels in spread comparison)
    # offense minus opponent defense, averaged, + HCA
    h_off = (ha - ad)
    a_off = (aa - hd)
    return hca_points + 0.5 * (h_off - a_off)

def compute_edges(
    ratings: Optional[pd.DataFrame],
    odds: pd.DataFrame,
    hca_points: float,
    edge_threshold: float,
) -> pd.DataFrame:

    df = _merge_ratings(odds, ratings).copy()

    # Compute model margin only when ratings are available
    df["model_home_margin"] = df.apply(lambda r: _model_margin(r, hca_points), axis=1)

    # market_home_margin already computed by scraper; keep home_spread as original sign
    # Edge = model - market
    df["edge_pts"] = df["model_home_margin"] - df["market_home_margin"]

    # ticket text (remove any “recommend” concept)
    def _ticket(r):
        if pd.notna(r["home_spread"]):
            # present market ticket like "BYU +35.5"
            sign = "+" if float(r["home_spread"]) > 0 else ""
            return f"{r['home']} {sign}{float(r['home_spread']):.1f}"
        return None

    df["ticket"] = df.apply(_ticket, axis=1)

    # Select output columns; scraper should already supply these
    core_cols = [
        "home","away","home_spread",
        "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
        "model_home_margin","market_home_margin",
        "edge_pts","ticket",
    ]
    for c in core_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # keep ordering but don’t crash if something’s missing
    return df[core_cols]
