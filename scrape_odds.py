import datetime as dt
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {"User-Agent":"Mozilla/5.0"}

def _scrape_covers(date: dt.date) -> pd.DataFrame:
    # Covers main odds table (public). We’ll read current spread rows when present.
    url = "https://www.covers.com/sport/basketball/ncaab/odds"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # The page structure changes occasionally. We look for matchup rows that include team names and spreads.
    rows = []
    # Strategy: find all game containers that show two teams and numeric spreads.
    for game in soup.select("div.covers-CoversMatchupsTable-tableRow, tr"):
        txt = game.get_text(" ", strip=True)
        if not txt or "Odds help" in txt: 
            continue
        # Try to pick two team names and two spreads in the row text
        # This is intentionally flexible—fallback to next scraper if we miss.
        # Example hints of spreads: '+3.5','-7','PK'
        parts = txt.split()
        spreads = [p for p in parts if p.replace(".","",1).lstrip("+-").isdigit() or p in ("PK","pk")]
        if len(spreads) >= 2:
            # Pull team names via labels frequently near spreads
            # As a heuristic, take first two capitalized multiword chunks in the row:
            words = [w for w in parts if any(c.isalpha() for c in w)]
            guess = []
            acc = []
            for w in words:
                if w[0].isupper():
                    acc.append(w)
                elif acc:
                    if len(acc) >= 1:
                        guess.append(" ".join(acc))
                    acc=[]
            if acc: guess.append(" ".join(acc))
            if len(guess) >= 2:
                home, away = guess[1], guess[0]  # ordering on Covers often Away @ Home; adjust if needed
                try:
                    hs = float(spreads[1].replace("PK","0").replace("pk","0"))
                    as_ = float(spreads[0].replace("PK","0").replace("pk","0"))
                    rows.append({"home":home, "away":away, "home_spread":hs, "away_spread":as_, "source":"covers"})
                except:
                    continue
    return pd.DataFrame(rows)

def _scrape_vegasinsider(date: dt.date) -> pd.DataFrame:
    # VI matchups page lists "Consensus TEAM -X.X"; we’ll grab teams and consensus line.
    url = "https://www.vegasinsider.com/college-basketball/matchups/"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    cards = soup.select("a:contains('Matchup'), div")  # broad
    rows=[]
    # Better: scan sections with 'Consensus ' and 'Open ' lines
    for sec in soup.find_all(text=lambda t: isinstance(t, str) and "Consensus" in t):
        text = sec.strip()
        # e.g., "Consensus UNLV -9.5  o156.5  -535"
        toks = text.split()
        try:
            idx = toks.index("Consensus")
            team = toks[idx+1]
            spread_str = toks[idx+2]
            if spread_str.lower().startswith(("o","u")):  # sometimes total first—skip
                continue
            spread = float(spread_str)
        except Exception:
            continue
        # Find nearest team block above/below to get opponent and likely home/away cue
        block = sec.parent.get_text(" ", strip=True)
        # Very rough parse around it; we’ll emit favorite only, home set unknown
        rows.append({"fav_team": team, "consensus_spread": spread, "source":"vegasinsider", "raw": block})
    return pd.DataFrame(rows)

def get_spreads(date: dt.date) -> pd.DataFrame:
    df = _scrape_covers(date)
    if not df.empty:
        return df
    vi = _scrape_vegasinsider(date)
    # If only favorite side known, we can’t compute both sides reliably—return empty to avoid bad calls
    return pd.DataFrame()
