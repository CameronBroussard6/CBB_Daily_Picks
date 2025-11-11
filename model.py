import pandas as pd
from rapidfuzz import fuzz, process
from unidecode import unidecode

def _norm_name(s: str) -> str:
    s = unidecode((s or "").lower())
    repl = (
        (" st.", " state"),
        (" st ", " state "),
        ("&", " and "),
        ("'", ""),
        (".", " "),
    )
    for a,b in repl: s = s.replace(a,b)
    return " ".join(s.split())

def _fuzzy_map(names, ref_names):
    ref_series = pd.Series(ref_names)
    out={}
    for n in names:
        m = process.extractOne(_norm_name(n), [_norm_name(x) for x in ref_series], scorer=fuzz.WRatio, score_cutoff=86)
        out[n] = ref_series.iloc[m[2]] if m else None
    return out

def compute_edges(odds_df: pd.DataFrame, trank_df: pd.DataFrame, home_bump: float = 1.5, edge_thresh: float = 2.0) -> pd.DataFrame:
    # Build lookups
    trank_df = trank_df.copy()
    trank_df["key"] = trank_df["Team"]

    # Map home/away names to Torvik names
    home_map = _fuzzy_map(odds_df["home"].unique(), trank_df["Team"].tolist())
    away_map = _fuzzy_map(odds_df["away"].unique(), trank_df["Team"].tolist())

    df = odds_df.copy()
    df["home_tr"] = df["home"].map(home_map)
    df["away_tr"] = df["away"].map(away_map)
    df = df.dropna(subset=["home_tr","away_tr"])

    df = df.merge(trank_df.add_prefix("h_"), left_on="home_tr", right_on="h_Team", how="left")
    df = df.merge(trank_df.add_prefix("a_"), left_on="away_tr", right_on="a_Team", how="left")

    df["model_home_margin"] = (df["h_AdjEM"] - df["a_AdjEM"]) + home_bump
    df["market_home_margin"] = -df["home_spread"]
    df["edge_pts"] = df["model_home_margin"] - df["market_home_margin"]

    def rec(row):
        if row["edge_pts"] >= edge_thresh: 
            return "HOME", f"{row['home']} {row['home_spread']:+.1f}"
        if row["edge_pts"] <= -edge_thresh: 
            return "AWAY", f"{row['away']} {row['away_spread']:+.1f}"
        return "PASS", ""
    picks = df.apply(rec, axis=1)
    picks_df = pd.DataFrame(picks.tolist(), columns=["recommend", "ticket"])
    df = pd.concat([df.reset_index(drop=True), picks_df], axis=1)

    keep = ["home","away","home_spread","away_spread",
            "h_Team","a_Team","h_AdjO","h_AdjD","a_AdjO","a_AdjD",
            "model_home_margin","market_home_margin","edge_pts","recommend","ticket"]
    return df[keep].sort_values(["recommend","edge_pts"], ascending=[True, False]).reset_index(drop=True)
