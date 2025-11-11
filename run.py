#!/usr/bin/env python3
import os
import datetime as dt
import pandas as pd

from ratings_trank import load_trank_team_eff
from scrape_odds import get_spreads
from model import compute_edges

# Tunables via env
HOME_BUMP   = float(os.getenv("HOME_COURT_POINTS", "0.6"))
EDGE_THRESH = float(os.getenv("EDGE_THRESHOLD", "2.0"))
OUTDIR      = os.getenv("OUTPUT_DIR", "site")
TODAY       = dt.date.today()


def _html_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>No games.</p>"
    df = df.copy()

    num_cols = [
        "home_spread",
        "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
        "model_home_margin","market_home_margin","edge_pts"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").map(lambda x: "" if pd.isna(x) else f"{x:.2f}")

    if "ticket" in df.columns:
        def wrap(x):
            s = "" if pd.isna(x) else str(x)
            return f'<span class="pick">{s}</span>' if s.strip() else ""
        df["ticket"] = df["ticket"].apply(wrap)

    css = """
    <style>
      .pick { background:#d4edda; padding:2px 6px; border-radius:6px; display:inline-block }
      table { border-collapse: collapse; width: 100% }
      th, td { border: 1px solid #ddd; padding: 8px }
      tr:nth-child(even) { background: #fafafa }
    </style>
    """
    return css + df.to_html(index=False, escape=False, justify="center")


def build_page(df: pd.DataFrame, date: dt.date, diag: str, csv_name: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NCAAB Edges – {date}</title>
<style>
body{{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:24px;max-width:1100px;margin:auto}}
h1{{margin:0 0 8px}} .meta{{color:#666;margin:8px 0 16px}}
</style></head><body>
<h1>NCAAB model vs spread</h1>
<div class="meta">{date} · Home bump={HOME_BUMP} · Threshold={EDGE_THRESH} pts</div>
{_html_table(df)}
<p class="meta">Diagnostics: {diag}</p>
<p class="meta"><a href="{csv_name}">Download CSV</a></p>
<p class="meta">Ratings: Bart Torvik (T-Rank). Spreads: ESPN (fallback Covers). Model margin = (Home AdjEM − Away AdjEM) + home bump; compared to market spread.</p>
</body></html>"""


def _normalize_and_dedupe(odds: pd.DataFrame) -> pd.DataFrame:
    """Clean team names and dedupe home/away pairs robustly."""
    if odds is None or odds.empty:
        return pd.DataFrame(columns=["home","away","home_spread","market_home_margin"])

    df = odds.copy()

    # Make sure columns exist
    for col in ("home", "away"):
        if col not in df.columns:
            df[col] = pd.NA

    # robust normalization (works if column is object, category, or numeric/NaN)
    for c in ("home", "away"):
        df[c] = df[c].astype(str).str.strip()
        # Normalize common unicode dashes, double spaces, etc.
        df[c] = (df[c]
                 .str.replace(r"[‐-‒–—]", "-", regex=True)
                 .str.replace(r"\s+", " ", regex=True))

    # build dedupe key; protect against "nan"
    df["__key"] = (df["home"].str.lower() + "|" + df["away"].str.lower())
    df = df.drop_duplicates(subset="__key", keep="first").drop(columns="__key", errors="ignore")
    return df


def main(date: dt.date):
    print(f"[INFO] Date={date}  HCA={HOME_BUMP}  Edge={EDGE_THRESH}")

    trank = load_trank_team_eff(date)
    print(f"[INFO] Loaded Torvik rows: {len(trank)}")

    raw_odds = get_spreads(date)
    raw_count = 0 if raw_odds is None else len(raw_odds)
    odds = _normalize_and_dedupe(raw_odds)
    with_spread = int(odds["home_spread"].notna().sum()) if not odds.empty and "home_spread" in odds.columns else 0
    print(f"[INFO] Parsed odds rows: {raw_count} -> deduped: {len(odds)} | with market spreads: {with_spread}")

    edges = compute_edges(odds, trank, home_bump=HOME_BUMP, edge_thresh=EDGE_THRESH)

    # ---- Column prune & order (requested) ----
    drop_cols = [c for c in ["away_spread","h_Team","a_Team","recommend"] if c in edges.columns]
    edges = edges.drop(columns=drop_cols, errors="ignore")

    ordered = [
        "home","away","home_spread",
        "h_AdjO","h_AdjD","a_AdjO","a_AdjD",
        "model_home_margin","market_home_margin","edge_pts","ticket"
    ]
    edges = edges[[c for c in ordered if c in edges.columns]]

    # sort: picks first by edge desc, then others
    if not edges.empty:
        edges["__pick"] = edges["ticket"].fillna("").ne("")
        edges["__edge"] = pd.to_numeric(edges["edge_pts"], errors="coerce")
        edges = edges.sort_values(by=["__pick","__edge"], ascending=[False, False]).drop(columns=["__pick","__edge"])

    plays = int(edges["ticket"].fillna("").ne("").sum()) if not edges.empty else 0
    diag = f"Games scraped: {raw_count} · after dedupe: {len(odds)} · modeled: {len(edges)} · with market spreads: {with_spread} · picks: {plays}."

    os.makedirs(OUTDIR, exist_ok=True)
    csv_name = f"ncaab_edges_{date.isoformat()}.csv"
    edges.to_csv(os.path.join(OUTDIR, csv_name), index=False, encoding="utf-8")
    with open(os.path.join(OUTDIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(build_page(edges, date, diag, csv_name))
    print("[DONE] HTML & CSV written")


if __name__ == "__main__":
    main(TODAY)
