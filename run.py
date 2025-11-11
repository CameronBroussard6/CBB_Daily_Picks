#!/usr/bin/env python3
import os
import datetime as dt
import pandas as pd

from ratings_trank import load_trank_team_eff
from scrape_odds import get_spreads
from model import compute_edges

# Tunables via env
HOME_BUMP   = float(os.getenv("HOME_COURT_POINTS", "0.6"))   # smaller, more realistic default
EDGE_THRESH = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTDIR      = os.getenv("OUTPUT_DIR", "site")

TODAY = dt.date.today()


def _html_table(df: pd.DataFrame) -> str:
    """Render HTML, highlighting only ticket cells when nonblank."""
    if df.empty:
        return "<p>No games.</p>"

    df = df.copy()

    num_cols = [
        "home_spread", "away_spread",
        "h_AdjO", "h_AdjD", "a_AdjO", "a_AdjD",
        "model_home_margin", "market_home_margin", "edge_pts"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").map(lambda x: "" if pd.isna(x) else f"{x:.2f}")

    if "ticket" in df.columns:
        def wrap_ticket(x):
            s = "" if pd.isna(x) else str(x)
            return f'<span class="pick">{s}</span>' if s.strip() else ""
        df["ticket"] = df["ticket"].apply(wrap_ticket)

    table_html = df.to_html(index=False, escape=False, justify="center")
    css = """
    <style>
      .pick { background: #d4edda; padding: 2px 6px; border-radius: 6px; display: inline-block; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #ddd; padding: 8px; }
      tr:nth-child(even) { background: #fafafa; }
    </style>
    """
    return css + table_html


def build_page(df: pd.DataFrame, date: dt.date, diag: str, csv_name: str) -> str:
    table_html = _html_table(df)
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NCAAB Edges – {date}</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:24px;max-width:1100px;margin:auto}}
h1{{margin:0 0 8px}} .meta{{color:#666;margin:8px 0 16px}}
</style>
</head><body>
<h1>NCAAB model vs spread</h1>
<div class="meta">{date} · Home bump={HOME_BUMP} · Threshold={EDGE_THRESH} pts</div>
{table_html}
<p class="meta">Diagnostics: {diag}</p>
<p class="meta"><a href="{csv_name}">Download CSV</a></p>
<p class="meta">Ratings from Bart Torvik (T-Rank). Spreads from ESPN (fallback: Covers).
Model margin = (Home AdjEM − Away AdjEM) + home bump; compared to market spread.</p>
</body></html>"""


def _normalize_and_dedupe(odds: pd.DataFrame) -> pd.DataFrame:
    """Trim, lowercase-key, and drop duplicate matchups."""
    if odds.empty:
        return odds
    df = odds.copy()
    for c in ("home", "away"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    # unique key by normalized matchup
    df["__key"] = (df["home"] + "|" + df["away"]).str.lower()
    # keep the first occurrence (typically the primary ESPN line)
    df = df.drop_duplicates(subset="__key", keep="first").drop(columns="__key")
    return df


def main(date: dt.date):
    print(f"[INFO] Date={date}  HCA={HOME_BUMP}  Edge={EDGE_THRESH}")

    # 1) Ratings
    trank = load_trank_team_eff(date)
    print(f"[INFO] Loaded Torvik rows: {len(trank)}")

    # 2) Odds (may include many sections/endpoints; dedupe afterward)
    raw_odds = get_spreads(date)
    raw_count = len(raw_odds)
    odds = _normalize_and_dedupe(raw_odds)
    with_spread = int(odds["home_spread"].notna().sum()) if not odds.empty else 0
    print(f"[INFO] Parsed odds rows: {raw_count} -> after dedupe: {len(odds)}  | with spreads: {with_spread}")

    # 3) Model
    edges = compute_edges(odds, trank, home_bump=HOME_BUMP, edge_thresh=EDGE_THRESH)

    # order: picks first (best edge top-down), then the rest
    if not edges.empty:
        edges["edge_numeric"] = pd.to_numeric(edges.get("edge_pts"), errors="coerce")
        # True first for picks: recommend != PASS
        edges["__is_pick"] = (edges.get("recommend") != "PASS")
        edges = edges.sort_values(by=["__is_pick", "edge_numeric"], ascending=[False, False]).drop(columns=["__is_pick", "edge_numeric"])

    plays = int((edges["recommend"] != "PASS").sum()) if not edges.empty else 0
    diag = f"Games scraped: {raw_count} · after dedupe: {len(odds)} · modeled rows: {len(edges)} · plays: {plays}."

    # 4) Outputs
    os.makedirs(OUTDIR, exist_ok=True)
    csv_name = f"ncaab_edges_{date.isoformat()}.csv"
    csv_path = os.path.join(OUTDIR, csv_name)
    edges.to_csv(csv_path, index=False)
    print(f"[DONE] CSV: {csv_path}")

    html = build_page(edges, date, diag, csv_name)
    with open(os.path.join(OUTDIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print("[DONE] HTML written")


if __name__ == "__main__":
    main(TODAY)
