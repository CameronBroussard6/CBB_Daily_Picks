#!/usr/bin/env python3
"""
Builds NCAAB model edges vs market spreads and publishes a simple HTML page.
- Ratings: Bart Torvik (AdjO/AdjD -> AdjEM)
- Spreads: ESPN scoreboard (primary), Covers (fallback)
- Model: model_home_margin = (Home AdjEM − Away AdjEM) + HOME_BUMP
         edge_pts = model_home_margin − (−home_spread)

Env knobs:
  HOME_COURT_POINTS  (default 1.5)
  EDGE_THRESHOLD     (default 2.0)
  OUTPUT_DIR         (default "site")
"""

import os
import datetime as dt
import pandas as pd

from ratings_trank import load_trank_team_eff
from scrape_odds import get_spreads
from model import compute_edges

# -------------------------
# Config (from env)
# -------------------------
HOME_BUMP   = float(os.getenv("HOME_COURT_POINTS", "1.5"))
EDGE_THRESH = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTDIR      = os.getenv("OUTPUT_DIR", "site")

TODAY = dt.date.today()


def build_page(df: pd.DataFrame, date: dt.date, diag: str) -> str:
    """Return HTML string for GitHub Pages."""
    table_html = (
        "" if df.empty
        else df.to_html(index=False, justify="center",
                        float_format=lambda x: f"{x:.2f}")
    )
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NCAAB Edges – {date}</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:24px;max-width:1100px;margin:auto}}
h1{{margin:0 0 8px}}
.meta{{color:#666;margin-bottom:16px}}
table{{border-collapse:collapse;width:100%}}
td,th{{border:1px solid #ddd;padding:8px}}
tr:nth-child(even){{background:#fafafa}}
.badge{{display:inline-block;padding:2px 8px;border-radius:12px;background:#eee;margin-left:8px}}
</style></head><body>
<h1>NCAAB model vs spread</h1>
<div class="meta">{date} · Home bump={HOME_BUMP} · Threshold={EDGE_THRESH} pts</div>
{table_html if table_html else "<p>No edges today (or scrape returned empty).</p>"}
<p class="meta">Diagnostics: {diag}</p>
<p class="meta">Ratings from Bart Torvik (T-Rank). Spreads from public odds pages (ESPN first, Covers fallback).
This is a simple AdjEM model: (Home AdjEM − Away AdjEM) + home bump.</p>
</body></html>"""


def main(date: dt.date):
    # 1) Ratings
    print(f"[INFO] Building edges for {date} (HOME_BUMP={HOME_BUMP}, EDGE_THRESH={EDGE_THRESH})")
    trank = load_trank_team_eff(date)
    print(f"[INFO] Loaded Torvik teams: {len(trank)}")

    # 2) Odds
    print("[INFO] Fetching odds ...")
    odds = get_spreads(date)
    print(f"[INFO] ESPN/Covers rows: {len(odds)}")

    # 3) Model edges
    if odds.empty:
        edges = pd.DataFrame()
        diag = f"No odds rows parsed for {date}."
    else:
        edges = compute_edges(odds, trank, home_bump=HOME_BUMP, edge_thresh=EDGE_THRESH)
        diag = f"Parsed {len(odds)} odds rows; produced {len(edges)} modeled rows."

    # 4) Write outputs
    os.makedirs(OUTDIR, exist_ok=True)
    csv_path = os.path.join(OUTDIR, f"ncaab_edges_{date.isoformat()}.csv")
    edges.to_csv(csv_path, index=False)
    print(f"[DONE] Wrote CSV: {csv_path} ({len(edges)} rows)")

    html = build_page(edges, date, diag)
    index_path = os.path.join(OUTDIR, "index.html")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[DONE] Wrote HTML: {index_path}")


if __name__ == "__main__":
    main(TODAY)
