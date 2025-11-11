import datetime as dt
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ---- 1) DraftKings public API (primary) ----
# NCAAB event group id (college basketball)
DK_EVENTGROUP = "92453"
DK_URL = f"https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/{DK_EVENTGROUP}?format=json"

def _scrape_draftkings(date: dt.date) -> pd.DataFrame:
    r = requests.get(DK_URL, headers=HEADERS, timeout=25)
    r.raise_for_status()
    js = r.json()

    # Build event lookup (teams, start date)
    events = {e["eventId"]: e for e in js.get("eventGroup", {}).get("events", []) if "eventId" in e}
    # Filter to today's games
    ymd = date.isoformat()
    event_ids_today = {eid for eid, e in events.items() if e.get("startDate", "")[:10] == ymd}

    # Find "Game Lines" -> "Spread" offers
    rows = []
    for cat in js.get("eventGroup", {}).get("offerCategories", []):
        if cat.get("name") != "Game Lines":
            continue
        for sub in cat.get("offerSubcategoryDescriptors", []):
            if sub.get("name") != "Game Lines":
                continue
            for desc in sub.get("offerSubcategory", {}).get("offers", []):
                # offers is a list of lists: [[offer_for_event]]
                if not desc:
                    continue
                offer = desc[0]
                eid = offer.get("eventId")
                if eid not in event_ids_today:
                    continue
                if offer.get("betType") != "Spread":
                    continue

                ev = events[eid]
                home = ev.get("homeTeamName")
                away = ev.get("awayTeamName")
                outcomes = offer.get("outcomes", []) or []

                # outcomes typically contain two sides with "label" and "line"
                line_map = {}
                for o in outcomes:
                    label = o.get("label")
                    line = o.get("line")
                    # Some DK payloads store tenths as ints (e.g., -75 => -7.5)
                    if isinstance(line, int):
                        line = line / 10.0
                    try:
                        line = float(line)
                    except Exception:
                        continue
                    line_map[label] = line

                if home in line_map and away in line_map:
                    rows.append({
                        "home": home,
                        "away": away,
                        "home_spread": line_map[home],
                        "away_spread": line_map[away],
                        "source": "draftkings"
                    })

    return pd.DataFrame(rows)

# ---- 2) Covers fallback (brittle HTML) ----
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

# ---- 3) VegasInsider (very limited; used as last-resort placeholder) ----
def _scrape_vegasinsider(date: dt.date) -> pd.DataFrame:
    return pd.DataFrame()  # keep disabled unless needed; VI markup changes often

# ---- entry point ----
def get_spreads(date: dt.date) -> pd.DataFrame:
    dk = _scrape_draftkings(date)
    if not dk.empty:
        return dk
    cv = _scrape_covers(date)
    if not cv.empty:
        return cv
    vi = _scrape_vegasinsider(date)
    return vi
