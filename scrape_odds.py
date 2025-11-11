import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ---------- 1) ESPN public scoreboard (primary) ----------
# Example: https://site.api.espn.com/apis/v2/sports/basketball/mens-college-basketball/scoreboard?dates=20251111
# correct ESPN scoreboard endpoint
ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"


def _scrape_espn(date: dt.date) -> pd.DataFrame:
    datestr = date.strftime("%Y%m%d")
    r = requests.get(ESPN_URL, params={"dates": datestr}, headers=HEADERS, timeout=25)
    r.raise_for_status()
    js = r.json()

    rows = []
    for ev in js.get("events", []):
        comps = (ev.get("competitions") or [])
        if not comps:
            continue
        comp = comps[0]

        # Map teams & home/away
        teams = comp.get("competitors") or []
        if len(teams) != 2:
            continue

        home = next((t for t in teams if t.get("homeAway") == "home"), None)
        away = next((t for t in teams if t.get("homeAway") == "away"), None)
        if not home or not away:
            continue

        home_name = home.get("team", {}).get("displayName") or home.get("team", {}).get("name")
        away_name = away.get("team", {}).get("displayName") or away.get("team", {}).get("name")
        if not home_name or not away_name:
            continue

        # Odds block: ESPN often includes consensus/primary book
        odds_list = comp.get("odds") or []
        if not odds_list:
            # No spread yet, skip
            continue

        # Take the first odds entry with a point spread
        spread_home = None
        spread_away = None
        for o in odds_list:
            # Some payloads have "details": "Team -6.5" and "overUnder", plus "spread" numbers
            # Prefer explicit spread fields if present
            sp = o.get("spread")
            if sp is not None:
                try:
                    sp = float(sp)
                except Exception:
                    sp = None
            fav = (o.get("favorite") or {}).get("displayName")
            # If "spread" present and favorite known, derive home/away spreads
            if sp is not None and fav:
                if fav == home_name:
                    spread_home, spread_away = -sp, sp
                elif fav == away_name:
                    spread_home, spread_away = sp, -sp
            # Some variants expose "homeTeamOdds"/"awayTeamOdds" with "spread"
            h_odds = o.get("homeTeamOdds") or {}
            a_odds = o.get("awayTeamOdds") or {}
            if "spread" in h_odds and "spread" in a_odds:
                try:
                    spread_home = float(h_odds["spread"])
                    spread_away = float(a_odds["spread"])
                except Exception:
                    pass

            if spread_home is not None and spread_away is not None:
                rows.append({
                    "home": home_name,
                    "away": away_name,
                    "home_spread": spread_home,
                    "away_spread": spread_away,
                    "source": "espn"
                })
                break

    return pd.DataFrame(rows)

# ---------- 2) Covers fallback (HTML, best-effort) ----------
def _scrape_covers(date: dt.date) -> pd.DataFrame:
    url = "https://www.covers.com/sport/basketball/ncaab/odds"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    rows = []
    for game in soup.select("div.covers-CoversMatchupsTable-tableRow, tr"):
        txt = game.get_text(" ", strip=True)
        if not txt or "Odds help" in txt:
            continue
        parts = txt.split()
        spreads = [p for p in parts if p.replace(".", "", 1).lstrip("+-").isdigit() or p.lower() == "pk"]
        if len(spreads) >= 2:
            words = [w for w in parts if any(c.isalpha() for c in w)]
            guess, acc = [], []
            for w in words:
                if w[0].isupper():
                    acc.append(w)
                elif acc:
                    guess.append(" ".join(acc)); acc = []
            if acc: guess.append(" ".join(acc))
            if len(guess) >= 2:
                home, away = guess[1], guess[0]
                try:
                    hs = float(spreads[1].replace("PK", "0").replace("pk", "0"))
                    as_ = float(spreads[0].replace("PK", "0").replace("pk", "0"))
                    rows.append({"home": home, "away": away, "home_spread": hs, "away_spread": as_, "source": "covers"})
                except:
                    pass
    return pd.DataFrame(rows)

# ---------- entry ----------
def _parse_details_to_spreads(details: str, home_name: str, away_name: str):
    # details looks like "Duke -6.5" or "Kansas +2.0"
    # Return (home_spread, away_spread) if we can parse it, else (None, None)
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
    # If details says "Fav -6.5", then favorite is laying points
    if fav_name == home_name:
        return -num, +num
    if fav_name == away_name:
        return +num, -num
    # Sometimes details uses short names; fall back: if number negative -> first listed team is favorite.
    # ESPN's competitor order is consistent (home/away), but we guard anyway.
    if num < 0:
        # first token is favorite but we don't know which; can't trust
        return None, None
    return None, None

def _scrape_espn(date: dt.date) -> pd.DataFrame:
    datestr = date.strftime("%Y%m%d")
    r = requests.get(ESPN_URL, params={"dates": datestr}, headers=HEADERS, timeout=25)
    r.raise_for_status()
    js = r.json()

    rows = []
    events = js.get("events", []) or []
    for ev in events:
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
        home_name = (home.get("team") or {}).get("displayName") or (home.get("team") or {}).get("name")
        away_name = (away.get("team") or {}).get("displayName") or (away.get("team") or {}).get("name")
        if not home_name or not away_name:
            continue

        # Try multiple shapes of odds payload
        spread_home = spread_away = None
        for o in comp.get("odds") or []:
            # 1) Explicit home/away spreads
            h_odds = o.get("homeTeamOdds") or {}
            a_odds = o.get("awayTeamOdds") or {}
            if "spread" in h_odds and "spread" in a_odds:
                try:
                    spread_home = float(h_odds["spread"])
                    spread_away = float(a_odds["spread"])
                except Exception:
                    spread_home = spread_away = None

            # 2) Single numeric "spread" + favorite name
            if (spread_home is None or spread_away is None):
                sp = o.get("spread")
                fav = (o.get("favorite") or {}).get("displayName")
                if sp is not None and fav:
                    try:
                        sp = float(sp)
                        if fav == home_name:
                            spread_home, spread_away = -sp, +sp
                        elif fav == away_name:
                            spread_home, spread_away = +sp, -sp
                    except Exception:
                        pass

            # 3) String "details": "Team -6.5"
            if (spread_home is None or spread_away is None):
                details = o.get("details") or ""
                h, a = _parse_details_to_spreads(details, home_name, away_name)
                if h is not None and a is not None:
                    spread_home, spread_away = h, a

            if spread_home is not None and spread_away is not None:
                rows.append({
                    "home": home_name,
                    "away": away_name,
                    "home_spread": spread_home,
                    "away_spread": spread_away,
                    "source": "espn"
                })
                break

    return pd.DataFrame(rows)

        return df
    cv = _scrape_covers(date)
    return cv
