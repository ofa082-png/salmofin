"""
generate_report.py
-------------------
Renders a static HTML disease report from BigQuery data and writes it
to docs/fiskehelse.html, for GitHub Pages to serve. Nightly script.

Moved off docs/index.html (2026-08-16) — that path is now the hub
frontpage (see generate_hub.py), which links here instead of this
page being the site root.

Scope narrowed to lice+mortality-free disease tracking only, in two
steps on 2026-08-19: Lusenivå/Avlusningsfartøy moved out first to
generate_lakselus.py/lakselus.html, then Dødelighet/Utkast/
Fiskehelseindikator moved out to generate_dodelighet.py/dodelighet.html
— this page was bundling three conceptually separate signals (lice,
mortality, disease) and kept growing. Now it's just sykdomstilfeller
tracking (active cases, new cases, the map).
"""

import os
import json
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
OUT_PATH   = os.path.join(os.path.dirname(__file__), "docs", "fiskehelse.html")

STATUS_LABEL = {
    "PANKREASSYKDOM": "PD",
    "INFEKSIOES_LAKSEANEMI": "ILA",
    "BAKTERIELL_NYRESYKE": "BKD",
    "FRANCISELLOSE": "Francisellose",
}
DISEASE_COLOR = {
    "PANKREASSYKDOM": "#2a78d6",
    "INFEKSIOES_LAKSEANEMI": "#1baf7a",
    "BAKTERIELL_NYRESYKE": "#eda100",
    "FRANCISELLOSE": "#008300",
}

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_data(client):
    kpis = list(client.query("""
        SELECT
          (SELECT COUNT(*) FROM salmofin.salmofin.mattilsynet_helsestatus) AS active_cases,
          (SELECT COUNT(DISTINCT lokalitetsnummer) FROM salmofin.salmofin.mattilsynet_helsestatus) AS active_localities,
          (SELECT COUNT(DISTINCT sykdomstype) FROM salmofin.salmofin.mattilsynet_helsestatus) AS active_diseases,
          (SELECT COUNT(DISTINCT id) FROM salmofin.salmofin.mattilsynet_disease
             WHERE opprettet >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)) AS new_cases_14d
    """).result())[0]

    recent = list(client.query("""
        SELECT lokalitetsnummer, lokalitetsnavn, sykdomstype,
          CASE WHEN avslutningsdato IS NOT NULL THEN 'Avsluttet'
               WHEN diagnosedato IS NOT NULL THEN 'Bekreftet'
               ELSE 'Mistanke' END AS status,
          COALESCE(avslutningsdato, diagnosedato, kvalitetssikretMistankedato, varslingsdato, opprettet) AS status_date
        FROM salmofin.salmofin.mattilsynet_disease
        WHERE COALESCE(avslutningsdato, diagnosedato, kvalitetssikretMistankedato, varslingsdato, opprettet)
              >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
        ORDER BY status_date DESC
        LIMIT 15
    """).result())

    map_rows = list(client.query("""
        SELECT h.lokalitetsnummer, h.lokalitetsnavn, h.sykdomstype, l.latitude, l.longitude
        FROM salmofin.salmofin.mattilsynet_helsestatus h
        LEFT JOIN salmofin.salmofin.localities l ON h.lokalitetsnummer = l.siteNr
        WHERE l.latitude IS NOT NULL
    """).result())

    return kpis, recent, map_rows

def build_sites_json(map_rows):
    by_site = {}
    for r in map_rows:
        s = by_site.setdefault(r.lokalitetsnummer, {
            "id": str(r.lokalitetsnummer), "name": r.lokalitetsnavn.title(),
            "lat": r.latitude, "lon": r.longitude, "diseases": []
        })
        s["diseases"].append(r.sykdomstype)
    return list(by_site.values())

def build_table_rows(recent):
    status_bg = {"Mistanke": "#fac775", "Bekreftet": "#f7c1c1", "Avsluttet": "#c0dd97"}
    status_fg = {"Mistanke": "#633806", "Bekreftet": "#791f1f", "Avsluttet": "#27500a"}
    rows = []
    for r in recent:
        label = STATUS_LABEL.get(r.sykdomstype, r.sykdomstype)
        date_str = r.status_date.strftime("%d.%m") if r.status_date else ""
        rows.append(f"""
      <tr style="border-top:0.5px solid var(--border);">
        <td style="padding:8px 10px;">{r.lokalitetsnavn.title()}</td>
        <td style="padding:8px 10px;">{label}</td>
        <td style="padding:8px 10px;"><span style="background:{status_bg[r.status]};color:{status_fg[r.status]};font-size:11px;padding:2px 8px;border-radius:4px;">{r.status}</span></td>
        <td style="padding:8px 10px;text-align:right;color:var(--text-secondary);">{date_str}</td>
      </tr>""")
    return "".join(rows)

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fiskehelse — ukesrapport</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  table {{ border-collapse:collapse; }}
  a {{ color:var(--text-secondary); }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:1.25rem;">
    <div>
      <div style="font-size:18px;font-weight:500;">Fiskehelse — ukesrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Uke {week}, {year} · oppdatert {updated}</div>
    </div>
    <div style="display:flex;gap:8px;align-items:baseline;">
      <a href="index.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">hjem →</a>
      <a href="dodelighet.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">dødelighet →</a>
      <a href="lakselus.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">lakselus →</a>
      <a href="traffic.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">trafikk →</a>
      <a href="foring.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fôring →</a>
      <div style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;">kilde: mattilsynet.io</div>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:1.5rem;">
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Aktive sykdomstilfeller</div>
      <div style="font-size:24px;font-weight:500;">{active_cases}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Aktive lokaliteter</div>
      <div style="font-size:24px;font-weight:500;">{active_localities}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Ulike sykdommer</div>
      <div style="font-size:24px;font-weight:500;">{active_diseases}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Nye siste 14 dager</div>
      <div style="font-size:24px;font-weight:500;">{new_cases_14d}</div>
    </div>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Nye og pågående sykdomstilfeller, siste 14 dager</div>
  <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:1.75rem;">
    <table style="width:100%;font-size:13px;table-layout:fixed;">
      <tr style="background:var(--surface-2);">
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Lokalitet</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Sykdom</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Status</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Dato</td>
      </tr>
      {table_rows}
    </table>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Kart over aktive tilfeller</div>
  <div style="display:flex;flex-wrap:wrap;gap:16px;margin-bottom:8px;font-size:12px;color:var(--text-secondary);">
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#2a78d6;"></span>PD</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#1baf7a;"></span>ILA</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#eda100;"></span>BKD</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#008300;"></span>Francisellose</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#4a3aa7;"></span>Flere</span>
  </div>
  <div id="map" style="width:100%;margin-bottom:1.5rem;"></div>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Data: Mattilsynet offentlig API, via salmofin BigQuery-pipeline. "Aktive sykdomstilfeller"/"aktive lokaliteter"/"ulike sykdommer" er et øyeblikksbilde (mattilsynet_helsestatus), ikke en rullerende periode. Generert automatisk hver natt.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js"></script>
<script>
const sites = {sites_json};
const diseaseColor = {{ 'PANKREASSYKDOM': '#2a78d6', 'INFEKSIOES_LAKSEANEMI': '#1baf7a', 'BAKTERIELL_NYRESYKE': '#eda100', 'FRANCISELLOSE': '#008300', 'multi': '#4a3aa7' }};
function colorFor(d) {{ return d.diseases.length > 1 ? diseaseColor.multi : diseaseColor[d.diseases[0]]; }}
const mw = 640, mh = 620;
const msvg = d3.select('#map').append('svg').attr('viewBox', `0 0 ${{mw}} ${{mh}}`).attr('width', '100%');
const isDark = matchMedia('(prefers-color-scheme: dark)').matches;
d3.json('https://cdn.jsdelivr.net/npm/datamaps@0.5.10/src/js/data/nor.topo.json').then(topo => {{
  const geoms = topo.objects.nor.geometries.filter(g => g.id && g.id.startsWith('NO.'));
  const features = topojson.feature(topo, {{ type: 'GeometryCollection', geometries: geoms }});
  const projection = d3.geoMercator().fitSize([mw, mh], features);
  const path = d3.geoPath(projection);
  msvg.append('g').selectAll('path').data(features.features).join('path')
    .attr('d', path).attr('fill', isDark ? '#2c2c2a' : '#e1e0d9')
    .attr('stroke', isDark ? '#1a1a19' : '#fcfcfb').attr('stroke-width', 0.75);
  msvg.append('g').selectAll('circle').data(sites).join('circle')
    .attr('cx', d => projection([d.lon, d.lat])[0]).attr('cy', d => projection([d.lon, d.lat])[1])
    .attr('r', 5).attr('fill', colorFor).attr('fill-opacity', 0.85)
    .attr('stroke', isDark ? '#1a1a19' : '#fcfcfb').attr('stroke-width', 1.2);
}});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Fetching data from BigQuery...")
    client = get_bq_client()
    kpis, recent, map_rows = fetch_data(client)

    sites = build_sites_json(map_rows)
    table_rows = build_table_rows(recent)
    now = datetime.datetime.now(datetime.timezone.utc)

    html = TEMPLATE.format(
        week=now.isocalendar()[1],
        year=now.year,
        updated=now.strftime("%d.%m.%Y"),
        active_cases=kpis.active_cases,
        active_localities=kpis.active_localities,
        active_diseases=kpis.active_diseases,
        new_cases_14d=kpis.new_cases_14d,
        table_rows=table_rows,
        sites_json=json.dumps(sites, ensure_ascii=False),
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
