# scrape_odds.py
import datetime as dt
import json
import re
from typing import List, Dict, Optional
import pandas as pd
import requests
from difflib import get_close_matches

HEADERS = {"User-Agent": "Mozilla/5.0 (NCAAB-edges)"}

# Common aliases / punctuation normalizations to boost joins
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

NON_DI_KEYWORDS = [
    "catawba","suny delhi","oakwood","wilson college","pensacola christian",
    "bethesda","southwestern adventist","southwestern christian","lincoln university",
    "iu columbus","cleary","new mexico highlands","coastal georgia",
]

def _clean(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    s = re.sub(r"[‐-–—−]", "-", s)        # dash variants
    s = re.sub(r"\s+", " ", s)
    s = s.replace("St.", "St").replace("Saint ", "Saint ")
    s_low = s.lower()
    s_low = s_low.replace("&", "and")
    s_low = re.sub(r"[^\w\s\-']", " ", s_low)
    s_low = re.sub(r"\s+", " ", s_low).strip()
    s_low = ALIASES.get(s_low, s_low)
    return s_low

def _is_nondi(name: str) -> bool:
    n = _clean(name)
    return any(k in n for k in NON_DI_KEYWORDS)

def _pair_key(home: str, away: str) -> str:
    # Order-independent key for merging odds and games
    a = _clean(home)
    b = _clean(away)
    return "|".join(sorted([a, b]))

def _espn(date: dt.date) -> pd.DataFrame:
    ymd = date.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={ymd}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    rows = []
    for ev in data.get("events", []):
        comp = ev.get("competitions", [{}])[0]
        teams = comp.get("competitors", [])
        if len(teams) != 2:
            continue
        t_home = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
        t_away = next((t for t in teams if t.get("homeAway") == "away"), teams[-1])
        home = t_home.get("team", {}).get("location") or t_home.get("team", {}).get("name")
        away = t_away.get("team", {}).get("location") or t_away.get("team", {}).get("name")

        spread = None
        market_home_margin = None
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            # ESPN gives "spread": -7.5 meaning favored team by 7.5
            espn_spread = o.get("spread")
            try:
                if espn_spread is not None:
                    # Convert to "home spread" with sign for home
                    fav_id = o.get("details", "")
                    # If details contain the favored team name, we can deduce sign.
                    # Simpler: ESPN's "spread" is relative to the favorite. We'll compute market_home_margin instead.
                    # Build implied home margin using favorite and spread when possible:
                    favorite = str(o.get("favorite", "")).lower()
                    h = _clean(home)
                    a = _clean(away)
                    s = float(espn_spread)
                    if favorite:
                        if favorite in h:
                            market_home_margin = s
                        elif favorite in a:
                            market_home_margin = -s
                    # Fall back to None if ambiguous
            except Exception:
                pass

        rows.append({
            "home": home, "away": away,
            "market_home_margin": market_home_margin,
            "home_spread": market_home_margin,  # same concept in our tables
            "source": "espn"
        })
    return pd.DataFrame(rows)

def _covers(date: dt.date) -> pd.DataFrame:
    # HTML page lists matchups & spreads; parse lightly.
    # If Covers layout shifts, this still returns gracefully.
    url = f"https://www.covers.com/sport/basketball/ncaab/odds?date={date.isoformat()}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        html = r.text
    except Exception:
        return pd.DataFrame(columns=["home","away","home_spread","market_home_margin","source"])

    # naive extraction by matchup rows
    # Pattern grabs "...<span>Team A</span> ... <span>Team B</span> ... spread ... "
    team_tags = re.findall(r'data-home-team="([^"]+)"\s+data-away-team="([^"]+)"[^>]*data-spread="([^"]*)"', html)
    rows = []
    for home, away, spread in team_tags:
        try:
            s = float(spread)
        except Exception:
            s = None
        rows.append({
            "home": home, "away": away,
            "home_spread": s,
            "market_home_margin": s,
            "source": "covers"
        })
    return pd.DataFrame(rows)

def get_spreads(date: dt.date) -> pd.DataFrame:
    """
    Return one row per game with best-available market spread (home margin).
    Columns: home, away, home_spread, market_home_margin
    """
    espn = _espn(date)
    covers = _covers(date)

    frames = []
    if not espn.empty:
        frames.append(espn)
    if not covers.empty:
        frames.append(covers)
    if not frames:
        return pd.DataFrame(columns=["home","away","home_spread","market_home_margin"])

    df = pd.concat(frames, ignore_index=True)

    # Mark non-DI games so run.py can report coverage correctly.
    df["non_di_home"] = df["home"].apply(_is_nondi)
    df["non_di_away"] = df["away"].apply(_is_nondi)
    df["likely_non_board"] = df["non_di_home"] | df["non_di_away"]

    # Build set-like key and dedupe; prefer rows that actually have a spread.
    df["__key"] = df.apply(lambda r: _pair_key(r["home"], r["away"]), axis=1)
    df["has_spread"] = df["market_home_margin"].notna()
    df = (
        df.sort_values(by=["has_spread","source"], ascending=[False, True])
          .drop_duplicates(subset="__key", keep="first")
          .drop(columns=["__key","has_spread"])
    )

    # Final numeric coercion
    for c in ("home_spread","market_home_margin"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[["home","away","home_spread","market_home_margin","likely_non_board"]]
