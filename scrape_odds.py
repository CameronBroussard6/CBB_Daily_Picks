import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ESPN site/v2 scoreboard for men's D-I. We’ll request groups=50 (Division I)
# and paginate through all pages for the date.
ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"

def _espn_school_name(team_obj: dict) -> str:
    # Prefer school/location (best match to Torvik)
    return team_obj.get("location") or team_obj.get("shortDisplayName") or team_obj.get("displayName") or team_obj.get("name")

def _parse_details_to_spreads(details: str, home_name: str, away_name: str):
    if not details:
        return None, None
    parts = details.strip().split()
    if len(parts) < 2:
        return None, None
    fav_name = " ".join(parts[:-1]).strip()
    try:
        num = float(parts[-1])
    except Exception:
        return None, None
    if fav_name == home_name:
        return -num, +num
    if fav_name == away_name:
        return +num, -num
    return None, None

def _fetch_espn_events(date: dt.date):
    datestr = date.strftime("%Y%m%d")
    all_events = []
    page = 1
    while True:
        params = {
            "dates": datestr,
            "groups": "50",   # Division I
            "limit": "500",   # be generous
            "page": str(page)
        }
        r = requests.get(ESPN_URL, params=params, headers=HEADERS, timeout=25)
        r.raise_for_status()
        js = r.json()
        events = js.get("events", []) or []
        all_events.extend(events)
        # stop when no more events returned (ESPN may not expose pageCount consistently)
        if not events:
            break
        page += 1
        if page > 10:
            break
    return all_events

def _scrape_espn(date: dt.date) -> pd.DataFrame:
    rows = []
    for ev in _fetch_espn_events(date):
        comps = ev.get("competitions") or []
        if not comps:
            continue
        comp = comps[0]
        teams = comp.get("competitors") or []
        if len(teams) != 2:
            continue

        home = next((t for t in teams if t.get("homeAway") == "home"), None)
        away = next((t for t in teams if t.get("homeAway") == "away"), None)
        if not home or not away:
            continue

        home_name = _espn_school_name((home.get("team") or {}))
        away_name = _espn_school_name((away.get("team") or {}))
        if not home_name or not away_name:
            continue

        # defaults when no line yet
        spread_home = spread_away = None

        # Try to extract a spread if present (several shapes in ESPN JSON)
        for o in comp.get("odds") or []:
            h_odds = o.get("homeTeamOdds") or {}
            a_odds = o.get("awayTeamOdds") or {}
            if "spread" in h_odds and "spread" in a_odds:
                try:
                    spread_home = float(h_odds["spread"])
                    spread_away = float(a_odds["spread"])
                except Exception:
                    spread_home = spread_away = None

            if spread_home is None or spread_away is None:
                sp = o.get("spread")
                fav = (o.get("favorite") or {}).get("displayName")
                try:
                    sp = float(sp) if sp is not None else None
                except Exception:
                    sp = None
                if sp is not None and fav:
                    if fav == home_name:
                        spread_home, spread_away = -abs(sp), +abs(sp)
                    elif fav == away_name:
                        spread_home, spread_away = +abs(sp), -abs(sp)

            if spread_home is None or spread_away is None:
                details = o.get("details") or ""
                h, a = _parse_details_to_spreads(details, home_name, away_name)
                if h is not None and a is not None:
                    spread_home, spread_away = h, a

            if spread_home is not None and spread_away is not None:
                # Enforce symmetry just in case
                if abs(spread_home + spread_away) > 0.1:
                    spread_away = -spread_home
                break  # got a line for this game

        rows.append({
            "home": home_name,
            "away": away_name,
            "home_spread": spread_home,
            "away_spread": spread_away,
            "source": "espn"
        })

    return pd.DataFrame(rows)

# Optional Covers fallback kept for completeness (rarely needed now)
def _scrape_covers(_date: dt.date) -> pd.DataFrame:
    url = "https://www.covers.com/sport/basketball/ncaab/odds"
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    for game in soup.select("div.covers-CoversMatchupsTable-tableRow, tr"):
        txt = game.get_text(" ", strip=True)
        if not txt or "Odds help" in txt:
            continue
        parts = txt.split()
        spreads = [p for p in parts if p.replace(".", "", 1).lstrip("+-").isdigit() or p.lower() == "pk"]
        if len(spreads) < 2:
            continue
        words = [w for w in parts if any(c.isalpha() for c in w)]
        guess, acc = [], []
        for w in words:
            if w[0].isupper():
                acc.append(w)
            elif acc:
                guess.append(" ".join(acc)); acc = []
        if acc: guess.append(" ".join(acc))
        if len(guess) < 2:
            continue
        home, away = guess[1], guess[0]
        try:
            hs = float(str(spreads[1]).replace("PK", "0").replace("pk", "0"))
            as_ = float(str(spreads[0]).replace("PK", "0").replace("pk", "0"))
        except Exception:
            hs = as_ = None
        rows.append({"home": home, "away": away, "home_spread": hs, "away_spread": as_, "source": "covers"})
    return pd.DataFrame(rows)

def get_spreads(date: dt.date) -> pd.DataFrame:
    df = _scrape_espn(date)
    if not df.empty:
        return df
    return _scrape_covers(date)
