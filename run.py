#!/usr/bin/env python3
import os
import datetime as dt
import pandas as pd

from ratings_trank import load_trank_team_eff
from scrape_odds import get_spreads
from model import compute_edges

HOME_BUMP   = float(os.getenv("HOME_COURT_POINTS", "0.8"))  # lowered default
EDGE_THRESH = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTDIR      = os.getenv("OUTPUT_DIR", "site")
TODAY = dt.date.today()

def style_table(df: pd.DataFrame) -> str:
    fmt = {c: "{:.2f}" for c in ["h_AdjO","h_AdjD","a_AdjO","a_AdjD","model_home_margin","market_home_margin","edge_pts"] if c in df.columns}
    def ticket_color(col):
        return ["background-color: #d4edda" if (val and str(val).strip()) else "" for val in col]
    styler = (df.style
                .hide_index()
                .format(fmt, na_rep="")
                .apply(ticket_color, subset=["ticket"]))
    return styler.to_html()

def build_page(df: pd.DataFrame, date: dt.date, diag: str, csv_name: str) -> str:
    table_html = style_table(df) if not df.empty else "<p>No games.</p>"
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NCAAB Edges – {date}</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:24px;max-width:1100px;margin:auto}}
h1{{margin:0 0 8px}} .meta{{color:#666;margin:8px 0 16px}}
table{{border-collapse:collapse;width:100%}}
</style></head><body>
<h1>NCAAB model vs spread</h1>
<div class="meta">{date} · Home bump={HOME_BUMP} · Threshold={EDGE_THRESH} pts</div>
{table_html}
<p class="meta">Diagnostics: {diag}</p>
<p class="meta"><a href="{csv_name}">Download CSV</a></p>
<p class="meta">Ratings from Bart Torvik (T-Rank). Spreads from ESPN (fallback: Covers). Model margin = (Home AdjEM − Away AdjEM) + home bump. We compare that to the market.</p>
</body></html>"""

def main(date: dt.date):
    print(f"[INFO] Date={date}  HCA={HOME_BUMP}  Edge={EDGE_THRESH}")
    trank = load_trank_team_eff(date)
    print(f"[INFO] Loaded Torvik rows: {len(trank)}")
    odds = get_spreads(date)
    print(f"[INFO] Parsed odds rows (including no-line games): {len(odds)}")

    edges = compute_edges(odds, trank, home_bump=HOME_BUMP, edge_thresh=EDGE_THRESH)
    diag = f"Parsed {len(odds)} odds rows; produced {len(edges)} modeled rows."

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
