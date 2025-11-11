import re
import pandas as pd
from rapidfuzz import fuzz, process
from unidecode import unidecode

ALIASES = {
    "uc santa barbara": "cal santa barbara",
    "uc riverside": "cal riverside",
    "uc irvine": "cal irvine",
    "uc davis": "cal davis",
    "uc san diego": "cal san diego",
    "miami fl": "miami florida",
    "miami (fl)": "miami florida",
    "miami oh": "miami ohio",
    "miami (oh)": "miami ohio",
    "unlv": "nevada las vegas",
    "texas a&m": "texas am",
    "texas a&m corpus christi": "texas am corpus christi",
    "st bonaventure": "saint bonaventure",
    "st johns": "saint johns",
    "st josephs": "saint josephs",
    "st marys": "saint marys",
    "st francis pa": "saint francis pa",
    "st francis ny": "saint francis ny",
    "loyola md": "loyola maryland",
    "loyola chi": "loyola chicago",
    "southern cal": "usc",
    "cal": "california",
}

def _clean_name(s: str) -> str:
    if not s:
        return ""
    s = unidecode(s).lower()
    s = re.sub(r"\bno\.\s*\d+\b", "", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s\(\)\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("(fl)", "fl").replace("(oh)", "oh")
    s = s.replace(" st ", " state ").replace(" univ ", " university ").replace(" univ. ", " university ")
    s = " ".join(s.split())
    return ALIASES.get(s, s)

def _norm(s: str) -> str:
    s = _clean_name(s)
    s = s.replace(" university", "")
    s = s.replace(" the ", " ")
    s = s.replace(" state university", " state")
    s = s.replace(" university of ", " ")
    s = s.replace("-", " ")
    return " ".join(s.split())

def _fuzzy_map(names, ref_names, cutoff=78):
    ref_norm = [_norm(x) for x in ref_names]
    out = {}
    for n in names:
        n_norm = _norm(n)
        result = process.extractOne(n_norm, ref_norm, scorer=fuzz.WRatio, score_cutoff=cutoff)
        if result:
            out[n] = ref_names[result[2]]
        else:
            out[n] = None
    return out

def compute_edges(odds_df: pd.DataFrame, trank_df: pd.DataFrame, home_bump: float = 0.8, edge_thresh: float = 2.0) -> pd.DataFrame:
    # NOTE: default home_bump lowered to 0.8 (you can override via env/CLI)
    if odds_df.empty:
        return pd.DataFrame()

    ref_names = trank_df["Team"].astype(str).tolist()
    home_map = _fuzzy_map(odds_df["home"].astype(str).unique(), ref_names, cutoff=78)
    away_map = _fuzzy_map(odds_df["away"].astype(str).unique(), ref_names, cutoff=78)

    df = odds_df.copy()
    df["home_tr"] = df["home"].map(home_map)
    df["away_tr"] = df["away"].map(away_map)

    # Join what we can; keep unmatched rows (they’ll yield PASS/blank)
    df = df.merge(trank_df.add_prefix("h_"), left_on="home_tr", right_on="h_Team", how="left")
    df = df.merge(trank_df.add_prefix("a_"), left_on="away_tr", right_on="a_Team", how="left")

    # Compute model only where we have both teams and a spread
    have_model = df["h_AdjEM"].notna() & df["a_AdjEM"].notna() & df["home_spread"].notna()
    df["model_home_margin"] = pd.NA
    df.loc[have_model, "model_home_margin"] = (df.loc[have_model, "h_AdjEM"] - df.loc[have_model, "a_AdjEM"]) + float(home_bump)

    df["market_home_margin"] = pd.NA
    df.loc[df["home_spread"].notna(), "market_home_margin"] = -df.loc[df["home_spread"].notna(), "home_spread"]

    df["edge_pts"] = pd.NA
    both = df["model_home_margin"].notna() & df["market_home_margin"].notna()
    df.loc[both, "edge_pts"] = df.loc[both, "model_home_margin"] - df.loc[both, "market_home_margin"]

    def rec(row):
        if pd.isna(row["edge_pts"]):
            return "PASS", ""
        if row["edge_pts"] >= edge_thresh:
            return "HOME", f"{row['home']} {row['home_spread']:+.1f}"
        if row["edge_pts"] <= -edge_thresh:
            return "AWAY", f"{row['away']} {row['away_spread']:+.1f}"
        return "PASS", ""

    picks = df.apply(rec, axis=1).tolist()
    df[["recommend", "ticket"]] = pd.DataFrame(picks, index=df.index)

    # Order: plays first by descending edge, then the rest (original order)
    df["is_play"] = (df["recommend"] != "PASS").astype(int)
    df["edge_sort"] = df["edge_pts"].fillna(-1e9)  # NAs go to bottom
    df = df.sort_values(["is_play", "edge_sort"], ascending=[False, False]).drop(columns=["is_play", "edge_sort"])

    # Final columns
    keep = [
        "home","away","home_spread","away_spread",
        "h_Team","a_Team","h_AdjO","h_AdjD","a_AdjO","a_AdjD",
        "model_home_margin","market_home_margin","edge_pts",
        "recommend","ticket"
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep].reset_index(drop=True)
