"""
generate_hub.py
----------------
Renders the salmofin frontpage — six tiles matching the health/traffic/
feed/export/capacity areas of the project, two trend charts, and three
"last 5" feeds. Not every section has a live data source wired up yet:

  - Lakselus, Sykdom, Trafikk, Slakt og eksport, Fôring tiles: real,
    BigQuery-backed, refit live every run.
  - Kapasitetsvekst tile, and the MOM-B / sykdomsoppdateringer /
    heftelser "last 5" panels: intentionally left as empty placeholders
    (2026-08-17) — the underlying pipelines (aqua_applications/licenses
    for capacity, env_reports/mattilsynet_disease/license_liens for the
    feeds) exist but aren't wired into a generator yet. Fill in one at
    a time rather than all at once.

  All navigation lives in the KPI tiles/charts above — deliberately no
  bottom link-card nav block (removed 2026-08-17, was fully redundant
  with the tiles; Store fartøy/big_vessels.html is on hold, not linked
  from the hub for now).

Writes docs/index.html.
"""

import os
import json
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
OUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "index.html")

MIN_LICE_ROWS = 400  # a week's Voksne_hunnlus count reliably lands ~560-580
                      # once reporting is complete; a still-filling-in week
                      # (the most recent 1-2) sits far below that, so this
                      # threshold cleanly separates "usable" from "too early".

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def diff_label(val, unit="%", signed_word=None):
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.0f}{unit}"

def diff_color_bad_if_up(val):
    """Lakselus, Sykdom: more is worse."""
    return "var(--text-danger)" if val > 0 else ("var(--text-success)" if val < 0 else "var(--text-muted)")

def diff_color_good_if_up(val):
    """Trafikk, Slakt og eksport: more activity/volume is a positive
    signal — matches traffic.html's own diff_color convention
    (positive=green) rather than treating every increase as bad."""
    return "var(--text-success)" if val > 0 else ("var(--text-danger)" if val < 0 else "var(--text-muted)")

def fetch_lice(client):
    """National avg adult female lice, most recent complete week vs the
    week before, plus a 12-week trend. Skips any week whose non-null
    Voksne_hunnlus count is below MIN_LICE_ROWS — still filling in, not
    representative yet (same "don't trust the newest partial week"
    lesson as the rest of this project)."""
    rows = list(client.query(f"""
        SELECT Ar, Uke, AVG(Voksne_hunnlus) AS avg_lice, COUNTIF(Voksne_hunnlus IS NOT NULL) AS n
        FROM salmofin.salmofin.lice_bw
        WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
          AND Uke BETWEEN EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 16
                       AND EXTRACT(ISOWEEK FROM CURRENT_DATE())
        GROUP BY Ar, Uke
        HAVING n >= {MIN_LICE_ROWS}
        ORDER BY Uke
    """).result())
    if len(rows) < 2:
        return None
    trend = rows[-12:]
    current, prior = rows[-1], rows[-2]
    delta_pct = (current.avg_lice - prior.avg_lice) / prior.avg_lice * 100 if prior.avg_lice else 0
    return {
        "value": round(current.avg_lice, 2),
        "delta_label": diff_label(delta_pct),
        "delta_color": diff_color_bad_if_up(delta_pct),
        "trend_labels": [f"U{r.Uke}" for r in trend],
        "trend_values": [round(r.avg_lice, 3) for r in trend],
    }

def fetch_sykdom(client):
    """Active case count (snapshot, matches fiskehelse.html) as the
    headline, with new-cases-this-week-vs-last-week as the delta — the
    snapshot table itself has no history to diff against, so the delta
    uses a different, well-defined comparison instead of faking one."""
    row = list(client.query("""
        SELECT
          (SELECT COUNT(*) FROM salmofin.salmofin.mattilsynet_helsestatus) AS active_cases,
          COUNTIF(opprettet >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS new_7d,
          COUNTIF(opprettet >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
                  AND opprettet < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS prev_7d
        FROM salmofin.salmofin.mattilsynet_disease
    """).result())[0]
    delta = row.new_7d - row.prev_7d
    return {
        "value": row.active_cases,
        "delta_label": f"{'+' if delta >= 0 else ''}{delta} nye siste uke",
        "delta_color": diff_color_bad_if_up(delta),
    }

def _last_complete_weeks(rows, n):
    """rows: [(yr, wk, value)], newest first. Drops the current calendar
    ISO week (often partial/not-yet-published) and returns the next n,
    oldest -> newest."""
    today = datetime.date.today()
    cur_yr, cur_wk = today.isocalendar()[0], today.isocalendar()[1]
    filtered = [r for r in rows if not (r[0] == cur_yr and r[1] == cur_wk)]
    return list(reversed(filtered[:n]))

def fetch_trafikk(client):
    """Wellboat + processing vessel locality visits, last complete week
    vs the week before — same fleet scope as traffic.html."""
    rows = list(client.query("""
        SELECT EXTRACT(ISOYEAR FROM vv.startTime) AS yr, EXTRACT(ISOWEEK FROM vv.startTime) AS wk, COUNT(*) AS visits
        FROM salmofin.salmofin.vessel_visits vv
        JOIN salmofin.salmofin.vessel_categories vc ON vv.mmsi = vc.MMSI
        WHERE TRIM(vc.Type) IN ('Wellboat','Processing vessel')
        GROUP BY yr, wk ORDER BY yr DESC, wk DESC LIMIT 4
    """).result())
    pairs = _last_complete_weeks([(r.yr, r.wk, r.visits) for r in rows], 2)
    if len(pairs) < 2:
        return None
    prior, current = pairs
    delta_pct = (current[2] - prior[2]) / prior[2] * 100 if prior[2] else 0
    return {
        "value": current[2],
        "delta_label": diff_label(delta_pct),
        "delta_color": diff_color_good_if_up(delta_pct),
    }

def fetch_foring(client):
    """Feed carrier locality visits, last complete week vs the week
    before — same fleet-filter/comparison shape as fetch_trafikk, just
    scoped to 'Fish feed carrier' instead of wellboat/processing. This
    promotes foring.html from a link-card to a real KPI tile."""
    rows = list(client.query("""
        SELECT EXTRACT(ISOYEAR FROM vv.startTime) AS yr, EXTRACT(ISOWEEK FROM vv.startTime) AS wk, COUNT(*) AS visits
        FROM salmofin.salmofin.vessel_visits vv
        JOIN salmofin.salmofin.vessel_categories vc ON vv.mmsi = vc.MMSI
        WHERE TRIM(vc.Type) = 'Fish feed carrier'
        GROUP BY yr, wk ORDER BY yr DESC, wk DESC LIMIT 4
    """).result())
    pairs = _last_complete_weeks([(r.yr, r.wk, r.visits) for r in rows], 2)
    if len(pairs) < 2:
        return None
    prior, current = pairs
    delta_pct = (current[2] - prior[2]) / prior[2] * 100 if prior[2] else 0
    return {
        "value": current[2],
        "delta_label": diff_label(delta_pct),
        "delta_color": diff_color_good_if_up(delta_pct),
    }

def fetch_export(client):
    """Actual export tonnage, last complete week vs the week before, plus
    a 12-week trend. Deliberately the simple real-vs-real comparison, not
    the seasonality+trend forecast model traffic.html uses — that model
    answers "what should this week be", this tile answers "what did it
    actually do last week", a different and simpler question."""
    rows = list(client.query("""
        SELECT year AS yr, week AS wk, SUM(vekt_tonn) AS export_tonn
        FROM salmofin.salmofin.salmon_export_weekly
        GROUP BY yr, wk ORDER BY yr DESC, wk DESC LIMIT 14
    """).result())
    pairs_all = _last_complete_weeks([(r.yr, r.wk, r.export_tonn) for r in rows], 12)
    if len(pairs_all) < 2:
        return None
    prior, current = pairs_all[-2], pairs_all[-1]
    delta_pct = (current[2] - prior[2]) / prior[2] * 100 if prior[2] else 0
    return {
        "value": current[2],
        "delta_label": diff_label(delta_pct),
        "delta_color": diff_color_good_if_up(delta_pct),
        "trend_labels": [f"U{wk}" for _, wk, _ in pairs_all],
        "trend_values": [round(v) for _, _, v in pairs_all],
    }

def build_sparkline_svg(chart_id, labels, values, color):
    return f"""<svg id="{chart_id}" viewBox="0 0 560 90" style="width:100%;height:90px" role="img"><title>Trend, siste {len(values)} uker</title></svg>
<script>
(function(){{
  var svg = document.getElementById('{chart_id}');
  var data = {json.dumps(values)};
  var labels = {json.dumps(labels)};
  var w = 560, h = 90, padL = 20, padR = 10, padT = 10, padB = 16;
  var min = Math.min.apply(null, data), max = Math.max.apply(null, data);
  var range = (max - min) || 1;
  var pts = data.map(function(v,i){{
    var x = padL + (i/(data.length-1))*(w-padL-padR);
    var y = h-padB - ((v-min)/range)*(h-padT-padB);
    return x.toFixed(1)+','+y.toFixed(1);
  }});
  var ns = 'http://www.w3.org/2000/svg';
  var poly = document.createElementNS(ns,'polyline');
  poly.setAttribute('points', pts.join(' '));
  poly.setAttribute('fill','none');
  poly.setAttribute('stroke','{color}');
  poly.setAttribute('stroke-width','2');
  svg.appendChild(poly);
  [0, data.length-1].forEach(function(i){{
    var coords = pts[i].split(',');
    var t = document.createElementNS(ns,'text');
    t.setAttribute('x',coords[0]); t.setAttribute('y', h-2);
    t.setAttribute('font-size','9'); t.setAttribute('text-anchor', i===0?'start':'end');
    t.setAttribute('fill','var(--text-muted)');
    t.textContent = labels[i];
    svg.appendChild(t);
  }});
}})();
</script>"""

TILES = [
    {"icon": "🐛", "title": "Lakselus og behandling", "unit": " voksne/fisk", "href": "fiskehelse.html", "linktext": "Nivå og behandlinger"},
    {"icon": "🦠", "title": "Sykdom", "unit": "", "suffix": " aktive tilfeller", "href": "fiskehelse.html", "linktext": "Status og dødelighet"},
    {"icon": "🚢", "title": "Trafikk", "unit": "", "suffix": " besøk/uke", "href": "traffic.html", "linktext": "Wellbåt og prosesseringsfartøy"},
    {"icon": "🐟", "title": "Slakt og eksport", "unit": "", "suffix": "", "href": "traffic.html", "linktext": "Volum og sesongmodell"},
    {"icon": "🌾", "title": "Fôring", "unit": "", "suffix": " besøk/uke", "href": "foring.html", "linktext": "Fôrbåtanløp og fiskehelseindikator"},
]

def build_tile(key, data, tile):
    if not data:
        return f"""
    <div class="card placeholder">
      <div class="tile-header">{tile['icon']} {tile['title']}</div>
      <div class="tile-placeholder">Kommer snart</div>
    </div>"""
    if key == "export":
        val_str = f"{data['value']/1000:.1f}k t"
    elif key == "lakselus":
        val_str = f"{data['value']}{tile['unit']}"
    else:
        val_str = f"{data['value']:,.0f}{tile.get('suffix','')}"
    return f"""
    <a href="{tile['href']}" class="card">
      <div class="tile-header">{tile['icon']} {tile['title']}</div>
      <div class="tile-value">{val_str}</div>
      <div class="tile-delta" style="color:{data['delta_color']}">{data['delta_label']}</div>
      <div class="tile-link">{tile['linktext']} →</div>
    </a>"""

PLACEHOLDER_FEED = """
    <div class="card placeholder">
      <div class="feed-header">{title}</div>
      <div class="tile-placeholder">Kommer snart</div>
    </div>"""

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>salmofin</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; --accent:#2a78d6; --text-danger:#a32d2d; --text-success:#008300; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  a {{ color:var(--text-secondary); text-decoration:none; }}
  .card {{ display:block; background:var(--surface-2); border-radius:12px; padding:1rem 1.1rem; border:0.5px solid var(--border); margin-bottom:12px; }}
  .card.placeholder {{ border-style:dashed; }}
  .grid {{ display:grid; grid-template-columns:repeat(2,1fr); gap:12px; margin-bottom:12px; }}
  .grid .card {{ margin-bottom:0; }}
  .tile-header {{ font-size:13px; color:var(--text-secondary); margin-bottom:6px; }}
  .tile-value {{ font-size:22px; font-weight:500; margin-bottom:2px; }}
  .tile-delta {{ font-size:12px; margin-bottom:4px; }}
  .tile-link {{ font-size:12px; color:var(--text-secondary); }}
  .tile-placeholder {{ font-size:13px; color:var(--text-muted); }}
  .feed-header {{ font-size:13px; color:var(--text-secondary); margin-bottom:8px; }}
  .chart-title {{ font-size:13px; color:var(--text-secondary); margin-bottom:4px; }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.5rem;">
    <div style="display:flex;align-items:center;gap:8px;">
      <div style="width:26px;height:26px;border-radius:6px;background:var(--accent);display:flex;align-items:center;justify-content:center;color:#fff;font-size:14px;font-weight:500;">s</div>
      <div style="font-size:18px;font-weight:500;">salmofin</div>
    </div>
    <div style="font-size:11px;color:var(--text-muted);">oppdatert {updated}</div>
  </div>

  <div class="grid">
    {tile_lakselus}
    {tile_sykdom}
    {tile_trafikk}
    {tile_export}
  </div>
  {tile_foring}
  {tile_kapasitet}

  <div class="card">
    <div class="chart-title">Voksne hunnlus, snitt/uke</div>
    {lice_chart}
  </div>
  <div class="card">
    <div class="chart-title">Eksport, tonn/uke</div>
    {export_chart}
  </div>

  {feed_momb}
  {feed_sykdom}
  {feed_liens}
</div>
</body>
</html>
"""

if __name__ == "__main__":
    client = get_bq_client()

    lice = fetch_lice(client)
    sykdom = fetch_sykdom(client)
    trafikk = fetch_trafikk(client)
    export = fetch_export(client)
    foring = fetch_foring(client)

    now = datetime.datetime.now(datetime.timezone.utc)
    html = TEMPLATE.format(
        updated=now.strftime("%d.%m.%Y %H:%M UTC"),
        tile_lakselus=build_tile("lakselus", lice, TILES[0]),
        tile_sykdom=build_tile("sykdom", sykdom, TILES[1]),
        tile_trafikk=build_tile("trafikk", trafikk, TILES[2]),
        tile_export=build_tile("export", export, TILES[3]),
        tile_foring=build_tile("foring", foring, TILES[4]),
        tile_kapasitet=f"""
    <div class="card placeholder">
      <div class="tile-header">📈 Kapasitetsvekst</div>
      <div class="tile-placeholder">Kommer snart — netto MTB og selskap</div>
    </div>""",
        lice_chart=build_sparkline_svg("liceChart", lice["trend_labels"], lice["trend_values"], "#D85A30") if lice else '<div class="tile-placeholder">Kommer snart</div>',
        export_chart=build_sparkline_svg("exportChart", export["trend_labels"], export["trend_values"], "#2a78d6") if export else '<div class="tile-placeholder">Kommer snart</div>',
        feed_momb=PLACEHOLDER_FEED.format(title="Siste 5 MOM-B"),
        feed_sykdom=PLACEHOLDER_FEED.format(title="Siste 5 sykdomsoppdateringer"),
        feed_liens=PLACEHOLDER_FEED.format(title="Siste 5 heftelser"),
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
    print(f"Lakselus: {lice}")
    print(f"Sykdom: {sykdom}")
    print(f"Trafikk: {trafikk}")
    print(f"Export: {export}")
    print(f"Foring: {foring}")
