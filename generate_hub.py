"""
generate_hub.py
----------------
Renders the salmofin frontpage — a static hub linking out to the three
standalone reports (fiskehelse, traffic, big vessel tracker). No
BigQuery calls: this page carries no live numbers of its own yet, only
links + short descriptions of what's on each page. Deliberately kept
that way rather than duplicating summary stats that would need to be
kept in sync with each report's own logic — add real KPI tiles here
later only if they're derived once and shared, not recomputed
per-page.

Writes docs/index.html — this is now the site root; the fish-health
report moved to docs/fiskehelse.html to make room for it (2026-08-16).
"""

import os
import datetime

OUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "index.html")

PAGES = [
    {
        "href": "fiskehelse.html",
        "title": "Fiskehelse",
        "sub": "Ukesrapport",
        "desc": "Aktive sykdomstilfeller, lusenivå siste 12 uker, og kart over pågående saker. Oppdateres hver natt.",
    },
    {
        "href": "traffic.html",
        "title": "Trafikkrapport",
        "sub": "Brønnbåt og prosesseringsfartøy",
        "desc": "Slakteaktivitet og eksportvolum — anløpsprognose for inneværende uke og anslått eksporttonnasje, justert for sesongvariasjon og trend.",
    },
    {
        "href": "foring.html",
        "title": "Fôringsrapport",
        "sub": "Fôr- og ensilasjefartøy",
        "desc": "Fôrbåtanløp ved lokaliteter og en egenutviklet fiskehelseindikator basert på ensilasje/fôr-anløpsforhold.",
    },
    {
        "href": "big_vessels.html",
        "title": "Store fartøy",
        "sub": "Rutetracker",
        "desc": "Live kart over de største brønnbåtene og prosesseringsfartøyene i drift.",
    },
]

def build_page_cards():
    cards = []
    for p in PAGES:
        cards.append(f"""
    <a href="{p['href']}" class="card" style="display:block;text-decoration:none;color:inherit;margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px;">
        <div style="font-size:16px;font-weight:500;">{p['title']}</div>
        <div style="font-size:12px;color:var(--text-muted);">{p['sub']} →</div>
      </div>
      <div style="font-size:13px;color:var(--text-secondary);line-height:1.5;">{p['desc']}</div>
    </a>""")
    return "".join(cards)

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>salmofin</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; --accent:#2a78d6; --accent2:#d68a2a; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  a {{ color:var(--text-secondary); }}
  .card {{ background:var(--surface-2); border-radius:8px; padding:1rem 1.1rem; border:0.5px solid var(--border); }}
  .card:hover {{ border-color:var(--accent); }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:1.75rem;">
    <div style="width:26px;height:26px;border-radius:6px;background:var(--accent);display:flex;align-items:center;justify-content:center;color:#fff;font-size:14px;font-weight:500;">s</div>
    <div style="font-size:18px;font-weight:500;">salmofin</div>
  </div>

  {page_cards}

  <div style="font-size:11px;color:var(--text-muted);margin-top:1.5rem;">Oppdatert {updated}</div>
</div>
</body>
</html>
"""

if __name__ == "__main__":
    html = TEMPLATE.format(
        page_cards=build_page_cards(),
        updated=datetime.date.today().isoformat(),
    )
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
