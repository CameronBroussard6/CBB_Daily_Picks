#!/usr/bin/env python3
import datetime as dt, os, pandas as pd
from ratings_trank import load_trank_team_eff
from scrape_odds import get_spreads
from model import compute_edges
from slugify import slugify

HOME_BUMP = float(os.getenv("HOME_COURT_POINTS", "1.5"))
EDGE_THRESH = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTDIR = os.getenv("OUTPUT_DIR","site")
TODAY = dt.date.today()

def main(date: dt.date):
    trank = load_trank_team_eff(date)
    odds = get_spreads(date)
    if odds.empty:
        print("[WARN] No spreads parsed today.")
        edges = pd.DataFrame()
    else:
        edges = compute_edges(odds, trank, home_bump=HOME_BUMP, edge_thresh=EDGE_THRESH)

    os.makedirs(OUTDIR, exist_ok=True)
    csv_path = os.path.join(OUTDIR, f"ncaab_edges_{date.isoformat()}.csv")
    edges.to_csv(csv_path, index=False)

    # publish HTML
    html = build_page(edges, date)
    with open(os.path.join(OUTDIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[DONE] Wrote {csv_path} and site/index.html")

def build_page(df: pd.DataFrame, date: dt.date) -> str:
    table = ("" if df.empty else df.to_html(index=False, justify="center", float_format=lambda x: f"{x:.2f}"))
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NCAAB Edges – {date}</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:24px;max-width:1100px;margin:auto}}
h1{{margin:0 0 8px}} .meta{{color:#666;margin-bottom:16px}} table{{border-collapse:collapse;width:100%}}
td,th{{border:1px solid #ddd;padding:8px}} tr:nth-child(even){{background:#fafafa}}
.badge{{display:inline-block;padding:2px 8px;border-radius:12px;background:#eee;margin-left:8px}}
</style></head><body>
<h1>NCAAB model vs spread</h1>
<div class="meta">{date} · Home bump={HOME_BUMP} · Threshold={EDGE_THRESH} pts</div>
{table if table else "<p>No edges today (or scrape returned empty).</p>"}
<p class="meta">Ratings from Bart Torvik (T-Rank). Spreads from public odds pages (Covers / VegasInsider). This is a simple AdjEM model: (Home AdjEM − Away AdjEM) + home bump.</p>
</body></html>"""

if __name__ == "__main__":
    main(TODAY)
