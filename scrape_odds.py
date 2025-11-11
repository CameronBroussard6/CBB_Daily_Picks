import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ---------------------------------------------------------
# 1) ESPN public scoreboard (PRIMARY)
# ---------------------------------------------------------
# Correct endpoint (site/v2)
ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"


def _parse_details_to_spreads(details: str, home_name: str, away_name: str):
    """
    ESPN 'details' example: 'Duke -6.5' or 'Kansas +2.0'
    Returns (home_spread, away_spread) or (None, None) if not parseable.
    """
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

    # If details says "Fav -6.5", favorite is laying points.
    if fav_name == home_name:
        return -num, +num
    if fav_name == away_name:
        return +num, -num

    # If names don't match exactly, don't guess.
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

        # --- pick school-style names (better for matching Torvik) ---
        team_home = home.get("team") or {}
        team_away = away.get("team") or {}

        # ESPN objects often have:
        #  - location: "Duke"
        #  - name:     "Blue Devils"   (nickname)
        #  - displayName: "Duke Blue Devils"
        #  - shortDisplayName: "Duke"
        def _espn_school_name(t):
            return t.get("location") or t.get("shortDisplayName") or t.get("displayName") or t.get("name")

        home_name = _espn_school_name(team_home)
        away_name = _espn_school_name(team_away)

        if not home_name or not away_name:
            continue

        spread_home = spread_away = None
        for o in comp.get("odds") or []:
            # Case A: explicit home/away spreads
            h_odds = o.get("homeTeamOdds") or {}
            a_odds = o.get("awayTeamOdds") or {}
            if "spread" in h_odds and "spread" in a_odds:
                try:
                    spread_home = float(h_odds["spread"])
                    spread_away = float(a_odds["spread"])
                except Exception:
                    spread_home = spread_away = None

            # Case B: single numeric spread + favorite team field
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

            # Case C: details string like "Duke -6.5"
            if (spread_home is None or spread_away is None):
                details = o.get("details") or ""
                h, a = _parse_details_to_spreads(details, home_name, away_name)
                if h is not None and a is not None:
                    spread_home, spread_away = h, a

            if spread_home is not None and spread_away is not None:
                # --- Normalize signs using favorite if available ---
                fav = (o.get("favorite") or {}).get("displayName")
                # ESPN also sometimes gives a single numeric 'spread' (absolute)
                abs_spread = None
                sp_val = o.get("spread")
                try:
                    abs_spread = abs(float(sp_val)) if sp_val is not None else None
                except Exception:
                    abs_spread = None

                if fav in (home_name, away_name) and abs_spread is not None:
                    if fav == home_name:
                        spread_home, spread_away = -abs_spread, +abs_spread
                    else:
                        spread_home, spread_away = +abs_spread, -abs_spread

                # --- Enforce symmetry: home + away ≈ 0 ---
                if abs((spread_home + spread_away)) > 0.1:
                    # If we still don't have symmetry, trust the home value and mirror it.
                    spread_away = -spread_home

                rows.append({
                    "home": home_name,
                    "away": away_name,
                    "home_spread": float(spread_home),
                    "away_spread": float(spread_away),
                    "source": "espn"
                })
                break


    return pd.DataFrame(rows)


# ---------------------------------------------------------
# 2) Covers (FALLBACK) – best-effort HTML parse
# ---------------------------------------------------------
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
                guess.append(" ".join(acc))
                acc = []
        if acc:
            guess.append(" ".join(acc))
        if len(guess) < 2:
            continue

        home, away = guess[1], guess[0]
        try:
            hs = float(str(spreads[1]).replace("PK", "0").replace("pk", "0"))
            as_ = float(str(spreads[0]).replace("PK", "0").replace("pk", "0"))
            rows.append({
                "home": home,
                "away": away,
                "home_spread": hs,
                "away_spread": as_,
                "source": "covers"
            })
        except Exception:
            continue

    return pd.DataFrame(rows)


# ---------------------------------------------------------
# Entry point for the rest of the project
# ---------------------------------------------------------
def get_spreads(date: dt.date) -> pd.DataFrame:
    """Return a DataFrame with columns: home, away, home_spread, away_spread, source."""
    df = _scrape_espn(date)
    if not df.empty:
        return df

    # Fallback to Covers if ESPN returns nothing yet
    cv = _scrape_covers(date)
    return cv
