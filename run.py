#!/usr/bin/env python3
import os, io, sys, time, textwrap, re
from datetime import datetime, timezone
import requests
import pandas as pd

# ---------------- CONFIG (env overridable) ----------------
HOME_COURT_POINTS = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESHOLD    = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTPUT_DIR        = os.getenv("OUTPUT_DIR", "site")
TORVIK_YEAR       = os.getenv("TORVIK_YEAR", "2025")

TORVIK_URL = f"https://barttorvik.com/trank.php?year={TORVIK_YEAR}&csv=1"
TR_ODDS_URL = "https://www.teamrankings.com/ncb/odds/"

# ---------------- Utilities ----------------
def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.lower().strip()
    s = re.sub(r"&", "and", s)
    s = s.replace("’", "'").replace("‘", "'").replace("´","'")
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"\s+", " ", s)
    # common harmonizations seen in your tables
    repl = {
        "st "         : "saint ",
        "st. "        : "saint ",
        "cal st "     : "cal state ",
        "uc "         : "ucla " if s == "uc" else s,  # no-op except lone "uc"
        "texas a&m cc": "texas a&m corpus christi",
        "texas a&m-corpus christi": "texas a&m corpus christi",
        "long island university": "liu",
        "central connecticut state": "central connecticut",
        "saint josephs": "saint joseph's",
        "william and mary": "william & mary",
        "mount st marys": "mount st. mary's",
        "ucsb": "uc santa barbara",
    }
    for k,v in repl.items():
        if s.startswith(k):
            s = s.replace(k, v, 1)
    # trim again
    s = re.sub(r"\s+", " ", s).strip()
    return s

def pick(df: pd.DataFrame, names):
    for n in names:
        if n in df.columns:
            return df[n]
    raise KeyError(f"Missing expected column among ({', '.join(names)})")

# ---------------- Data Loads (LIVE ONLY) ----------------
def load_torvik():
    log(f"[INFO] Loading Torvik CSV from {TORVIK_URL}")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }
    r = requests.get(TORVIK_URL, headers=headers, timeout=25)
    r.raise_for_status()
    # Ensure we really got CSV (Torvik sometimes serves HTML if rate-limited)
    text = r.text
    if text.lstrip().startswith("<"):
        raise RuntimeError("Torvik returned HTML instead of CSV (rate limited or format change).")
    df = pd.read_csv(io.StringIO(text))
    # Expected cols: Team, AdjO, AdjD (names vary rarely)
    team = pick(df, ["Team","team","School","school"])
    adjo = pick(df, ["AdjO","AdjOE","AdjO."])
    adjd = pick(df, ["AdjD","AdjDE","AdjD."])
    out = pd.DataFrame({
        "team_raw": team.astype(str),
        "AdjO": pd.to_numeric(adjo, errors="coerce"),
        "AdjD": pd.to_numeric(adjd, errors="coerce"),
    }).dropna(subset=["AdjO","AdjD"])
    out["team_key"] = out["team_raw"].map(normalize_name)
    log(f"[INFO] Loaded Torvik rows: {len(out)}")
    return out

def parse_tr_spread_cell(cell: str):
    """
    TeamRankings 'Spread' cell pattern like 'Duke -12.5' or 'Pick'
    Returns (fav_team, value_float). Positive value means that listed team is +points? No:
    We interpret value sign literally from the cell ('Team -12.5' => favorite -12.5).
    """
    if not isinstance(cell, str):
        return (None, None)
    s = cell.strip()
    if not s or s.lower() == "pick":
        return (None, 0.0)
    m = re.match(r"^(.*)\s([+-]?\d+(?:\.\d+)?)$", s)
    if not m:
        return (None, None)
    fav = m.group(1).strip()
    val = float(m.group(2))
    return (fav, val)

def load_odds_from_teamrankings():
    log(f"[INFO] Loading market spreads from {TR_ODDS_URL}")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }
    r = requests.get(TR_ODDS_URL, headers=headers, timeout=25)
    r.raise_for_status()
    # Parse all tables and pick the one that has 'Matchup' + 'Spread'
    tables = pd.read_html(io.StringIO(r.text), flavor="lxml")
    odds = None
    for t in tables:
        cols = [c.lower() for c in t.columns.astype(str)]
        if any("matchup" in c for c in cols) and any("spread" == c or "spread" in c for c in cols):
            odds = t
            break
    if odds is None:
        raise RuntimeError("Could not find odds table on TeamRankings page.")
    # Normalize columns
    odds.columns = [str(c).strip() for c in odds.columns]
    matchup_col = [c for c in odds.columns if "Matchup" in c][0]
    spread_col  = [c for c in odds.columns if c.lower().startswith("spread")][0]

    # Expected "Away at Home" in matchup
    def split_matchup(s: str):
        s = str(s)
        if " at " in s.lower():
            parts = re.split(r"\s+at\s+", s, flags=re.I)
        elif " @ " in s:
            parts = s.split(" @ ")
        else:
            # Fallback—try first ' ' separator if format changes
            parts = s.split()
            if len(parts) >= 2:
                return parts[0], " ".join(parts[1:])
            return s, ""
        away = parts[0].strip()
        home = parts[1].strip() if len(parts) > 1 else ""
        return away, home

    recs = []
    for _, row in odds.iterrows():
        away_raw, home_raw = split_matchup(row.get(matchup_col, ""))
        if not away_raw or not home_raw:
            continue
        fav, val = parse_tr_spread_cell(str(row.get(spread_col, "")).strip())
        # Build home_spread: market line relative to HOME team (home negative = favored)
        home_spread = None
        if fav is None and val is not None:
            # pick'em
            home_spread = 0.0
        elif fav is not None and (val is not None):
            fav_key = normalize_name(fav)
            home_key = normalize_name(home_raw)
            away_key = normalize_name(away_raw)
            if fav_key == home_key:
                home_spread = float(val)  # TeamRankings prints "Home -12.5" -> keep sign (-12.5)
            elif fav_key == away_key:
                # away is favorite by -x; so home is +x
                home_spread = -float(val)  # invert (e.g., "Away -3" => home +3)
            else:
                # If names don't match, infer by sign: negative value => listed team favored; assume that's HOME if names differ.
                home_spread = float(val)
        if home_spread is None:
            continue
        recs.append({
            "home_raw": home_raw,
            "away_raw": away_raw,
            "home_spread": float(home_spread),
            "home_key": normalize_name(home_raw),
            "away_key": normalize_name(away_raw),
        })
    out = pd.DataFrame.from_records(recs)
    log(f"[INFO] Parsed markets: {len(out)} games")
    return out

# ---------------- Model ----------------
def model_margin(h_AdjO, h_AdjD, a_AdjO, a_AdjD, hcp=HOME_COURT_POINTS):
    """
    Simple efficiency differential -> margin (per 100 possessions) + HCA.
    You were using this earlier; keep identical to preserve behavior.
    """
    # (Home offense vs away defense) - (Away offense vs home defense) + HCA
    return (h_AdjO - a_AdjD) - (a_AdjO - h_AdjD) + hcp

# ---------------- Publish ----------------
def write_index(ok: bool, n_games: int, n_edges: int):
    ensure_dir(OUTPUT_DIR)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M %Z")
    if ok:
        body = f"""
        <h1>NCAAB Daily Edges</h1>
        <p>Latest run artifacts below. (Auto-published)</p>
        <p><b>{n_games}</b> games with lines. <b>{n_edges}</b> edges ≥ {EDGE_THRESHOLD:.1f} pts.</p>
        <ul>
          <li><a href="edges_full.csv">edges_full.csv</a></li>
          <li><a href="edges_top.csv">edges_top.csv</a></li>
          <li><a href="build_log.txt">build_log.txt</a></li>
        </ul>
        <p>Updated: {ts}</p>
        """
    else:
        body = f"""
        <h1>NCAAB Daily Edges</h1>
        <p>Latest run artifacts below. (Auto-published)</p>
        <p><b>No CSV outputs found</b></p>
        <p>See <a href="build_log.txt">build_log.txt</a> for details.</p>
        <p>Updated: {ts}</p>
        """
    html = "<!doctype html><meta charset='utf-8'><body style='font-family:Georgia,serif;font-size:18px;line-height:1.3'>" + body + "</body>"
    with open(os.path.join(OUTPUT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

# ---------------- Main ----------------
def main():
    ensure_dir(OUTPUT_DIR)
    log_path = os.path.join(OUTPUT_DIR, "build_log.txt")
    # tee logs also to file
    class Tee:
        def __init__(self, path):
            self.f = open(path, "w", encoding="utf-8")
        def write(self, s):
            self.f.write(s); self.f.flush()
        def close(self): self.f.close()
    tee = Tee(log_path)

    try:
        print_fn = print
        def p(*a, **k):
            msg = " ".join(str(x) for x in a)
            print_fn(msg)
            tee.write(msg + "\n")

        p(f"[INFO] Date={datetime.now(timezone.utc).strftime('%Y-%m-%d')} HCA={HOME_COURT_POINTS} Edge={EDGE_THRESHOLD}")

        ratings = load_torvik()
        odds    = load_odds_from_teamrankings()

        # merge
        home = ratings.add_prefix("h_")
        away = ratings.add_prefix("a_")
        merged = odds.merge(home, left_on="home_key", right_on="h_team_key") \
                     .merge(away, left_on="away_key", right_on="a_team_key")

        # compute model/edge
        merged["model_home_margin"] = merged.apply(
            lambda r: model_margin(r["h_AdjO"], r["h_AdjD"], r["a_AdjO"], r["a_AdjD"]), axis=1
        )
        # market home margin (what market expects home to win by)
        # If home_spread is negative (home favorite), market_home_margin = -home_spread (e.g., -7.5 -> +7.5 for home)
        merged["market_home_margin"] = -merged["home_spread"].astype(float)
        merged["edge_pts"] = merged["model_home_margin"] - merged["market_home_margin"]

        # user-facing columns
        out = merged[[
            "home_raw","away_raw","home_spread",
            "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
            "model_home_margin","market_home_margin","edge_pts"
        ]].rename(columns={
            "home_raw":"home",
            "away_raw":"away",
        }).copy()

        # ticket column (exactly as you've been using it)
        out["ticket"] = out.apply(lambda r: f"{r['home']} {r['home_spread']:+.1f}".replace("+0.0","PK").replace("-0.0","PK"), axis=1)

        # ordering: biggest absolute edge first
        out.sort_values("edge_pts", key=lambda s: s.abs(), ascending=False, inplace=True)

        # write outputs
        ensure_dir(OUTPUT_DIR)
        out.to_csv(os.path.join(OUTPUT_DIR, "edges_full.csv"), index=False)
        top = out[out["edge_pts"].abs() >= EDGE_THRESHOLD]
        top.to_csv(os.path.join(OUTPUT_DIR, "edges_top.csv"), index=False)

        p(f"[INFO] Wrote {len(out)} games; {len(top)} edges >= {EDGE_THRESHOLD}")
        write_index(True, len(out), len(top))
    except Exception as e:
        # fail closed but still publish index + log
        err = f"[ERROR] {type(e).__name__}: {e}"
        print(err); tee.write(err+"\n")
        write_index(False, 0, 0)
        sys.exit(1)
    finally:
        tee.close()

if __name__ == "__main__":
    main()
