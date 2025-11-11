import re
import pandas as pd
from rapidfuzz import fuzz, process
from unidecode import unidecode

# Common alias normalizations for CBB
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
    # remove rankings like "No. 5"
    s = re.sub(r"\bno\.\s*\d+\b", "", s)
    # drop mascots after " - " or remove nickname by keeping only letters/spaces
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s\(\)\-]", " ", s)   # keep parens/dash for campus hints
    s = re.sub(r"\s+", " ", s).strip()

    # remove parenthetical qualifiers but keep FL/OH forms normalized later
    s = s.replace("(fl)", "fl").replace("(oh)", "oh")

    # canonical short forms
    s = s.replace(" st ", " state ")
    s = s.replace(" univ ", " university ").replace(" univ. ", " university ")

    # collapse
    s = " ".join(s.split())

    # apply alias map
    if s in ALIASES:
        s = ALIASES[s]
    return s

def _norm_for_match(s: str) -> str:
    # stronger normalization for matching
    s = _clean_name(s)
    s = s.replace(" university", "")
    s = s.replace(" the ", " ")
    s = s.replace(" state university", " state")
    s = s.replace(" university of ", " ")
    s = s.replace("-", " ")
    s = " ".join(s.split())
    return s

def _fuzzy_map(names, ref_names, cutoff=80):
    ref_norm = [_norm_for_match(x) for x in ref_names]
    out = {}
    for n in names:
        n_norm = _norm_for_match(n)
        result = process.extractOne(
            n_norm,
            ref_norm,
            scorer=fuzz.WRatio,
            score_cutoff=cutoff,
        )
        if result:
            idx = result[2]
            out[n] = ref_names[idx]
        else:
            out[n] = None
    return out

def compute_edges(odds_df: pd.DataFrame, trank_df: pd.DataFrame, home_bump: float = 1.5, edge_thresh: float = 2.0) -> pd.DataFrame:
    if odds_df.empty:
        return pd.DataFrame()

    # Torvik df: expect columns Team, AdjO, AdjD, AdjEM
    ref_names = trank_df["Team"].astype(str).tolist()

    # Map names
    home_map = _fuzzy_map(odds_df["home"].astype(str).unique(), ref_names, cutoff=80)
    away_map = _fuzzy_map(odds_df["away"].astype(str).unique(), ref_names, cutoff=80)

    df = odds_df.copy()
    df["home_tr"] = df["home"].map(home_map)
    df["away_tr"] = df["away"].map(away_map)
    df = df.dropna(subset=["home_tr", "away_tr"])

    if df.empty:
        # nothing matched; return empty to keep pipeline safe
        return pd.DataFrame()

    # Join Torvik numbers
    df = df.merge(trank_df.add_prefix("h_"), left_on="home_tr", right_on="h_Team", how="left")
    df = df.merge(trank_df.add_prefix("a_"), left_on="away_tr", right_on="a_Team", how="left")

    # Model margins
    df["model_home_margin"] = (df["h_AdjEM"] - df["a_AdjEM"]) + home_bump
    df["market_home_margin"] = -df["home_spread"]
    df["edge_pts"] = df["model_home_margin"] - df["market_home_margin"]

    def rec(row):
        if row["edge_pts"] >= edge_thresh:
            return "HOME", f"{row['home']} {row['home_spread']:+.1f}"
        if row["edge_pts"] <= -edge_thresh:
            return "AWAY", f"{row['away']} {row['away_spread']:+.1f}"
        return "PASS", ""

    picks_df = pd.DataFrame(df.apply(rec, axis=1).tolist(), columns=["recommend", "ticket"])
    df = pd.concat([df.reset_index(drop=True), picks_df], axis=1)

    keep = [
        "home", "away", "home_spread", "away_spread",
        "h_Team", "a_Team", "h_AdjO", "h_AdjD", "a_AdjO", "a_AdjD",
        "model_home_margin", "market_home_margin", "edge_pts",
        "recommend", "ticket"
    ]
    return df[keep].sort_values(["recommend", "edge_pts"], ascending=[True, False]).reset_index(drop=True)
