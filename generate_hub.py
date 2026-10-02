"""
generate_hub.py
----------------
Renders the salmofin front page (docs/index.html): one KPI card per report,
each with its headline figure, change against a year earlier and a one-line
highlight, linking to the report.

Rebuilt 2026-10-02 in the control-report visual system
(templates/hub_template.html). No BigQuery queries of its own: every figure
is read from the data the report pages already embed (`const D=` in
kontroll/foring/lakselus/fiskehelse/traffic.html), so a card always matches
its page. The workflow runs after the report generators. A page whose data
cannot be read gets a card saying so instead of failing the whole hub.

Mortality (dodelighet.html, older page without embedded data) is fed from the
fish health page's mortality series.
"""

import os
import json
import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS     = os.path.join(BASE_DIR, "docs")
OUT_PATH = os.path.join(DOCS, "index.html")
TEMPLATE = os.path.join(BASE_DIR, "templates", "hub_template.html")
MO = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def load(page):
    with open(os.path.join(DOCS, page), encoding="utf-8") as f:
        s = f.read()
    i = s.index("const D=") + len("const D=")
    return json.JSONDecoder().raw_decode(s[i:])[0]


def mlabel(t):
    return f"{MO[t % 12]} {t // 12}"


def pct(a, b):
    return (a / b - 1) * 100 if a is not None and b else None


def fmt(v, d=0):
    return f"{v:,.{d}f}"


def sign(v, d=0, unit="%"):
    return ("+" if v > 0 else "") + f"{v:,.{d}f}{unit}"


def card(key, href, area, title, value, unit, delta=None, good=None, highlight="", period=""):
    return {"key": key, "href": href, "area": area, "title": title, "value": value, "unit": unit,
            "delta": delta, "good": good, "highlight": highlight, "period": period}


def control():
    d = load("kontroll.html")
    m = d["m"]["Norge"]; last = m[-1]; ly = next((r for r in m if r["t"] == last["t"] - 12), None)
    bio_y, ht_y, n_y = (pct(last["bio"], ly["bio"]), pct(last["ht"], ly["ht"]), pct(last["n"], ly["n"])) if ly else (None,) * 3
    hl = (f"Harvest {fmt(last['ht'] / 1000)} kt in {MO[last['t'] % 12]} ({sign(ht_y)} on a year earlier). "
          f"{fmt(last['n'])} M fish standing ({sign(n_y)})"
          + (": more tonnes on fewer fish." if bio_y > 0 > n_y else ".")) if ly else ""
    return card("control", "kontroll.html", "Biological control", "Standing biomass", fmt(last["bio"] / 1000), "kt",
                sign(bio_y, 1) + " vs a year ago" if bio_y is not None else None, None, hl, mlabel(last["t"]))


def feed():
    d = load("foring.html")["areas"]["Norge"]; fd = d["fd"]
    est = fd["estimates"][0] if fd.get("estimates") else None
    fcr = d["fcr"]["bio"][-1] if d.get("fcr") else None
    hl = (f"{est['label']} estimate from feed-vessel visits: {fmt(est['value'] / 1000, 1)} kt (±{fmt(est['band'], 0)}%). " if est else "") + \
         (f"Biological FCR {fmt(fcr, 2)} in {fd['last_label'].split()[0]}." if fcr else "")
    return card("feed", "foring.html", "Feeding", "Feed consumption", fmt(fd["last_value"] / 1000, 1), "kt",
                sign(fd["last_yoy"], 1) + " vs a year ago", None, hl, fd["last_label"])


def lice():
    d = load("lakselus.html"); y0, w0 = d["last"]; a = d["areas"]["Norge"]; now = a["now"]
    nm = a["norm"][str(w0)]; gap = pct(now["af"], nm); fe = a["fc"][-1] if a["fc"] else None
    tl = a["treat"][-1]; tn = sum(tl["by"].values())
    hl = (f"Outlook week {fe['w']}: {fmt(fe['mid'], 2)} (normal {fmt(a['norm'][str(fe['w'])], 2)}). " if fe else "") + \
         f"{tn} sites treated in week {w0}, {a['treatLY']} a year ago."
    return card("lice", "lakselus.html", "Lice and treatments", "Adult female lice per fish", fmt(now["af"], 2), "",
                f"{sign(gap)} vs normal {fmt(nm, 2)}", gap <= 0, hl, f"Week {w0} {y0}")


def fish_health_and_mortality():
    d = load("fiskehelse.html"); a = d["areas"]["Norge"]; rows = a["rows"]
    rep = [r for r in rows if r["deadt"] is not None]; last = rep[-1]
    ly = next((r for r in rows if r["t"] == last["t"] - 12), None)
    nxt = next((r for r in rows if r["deadt"] is None and r["est"] is not None), None)
    nly = next((r for r in rows if nxt and r["t"] == nxt["t"] - 12), None)
    dis = a["dis"]; dl = dis[-2] if len(dis) > 1 else dis[-1]; dly = next((r for r in dis if r["t"] == dl["t"] - 12), None)
    sites = d["cases"]["sites"]; ncase = sum(len(s["d"]) for s in sites); new = sum(1 for r in d["cases"]["recent"] if r["new"])
    isapd, isapd_ly = dl["pd"] + dl["ila"], (dly["pd"] + dly["ila"]) if dly else None
    health = card("health", "fiskehelse.html", "Fish health", "ISA and PD", fmt(isapd), "sites",
                  f"{sign(isapd - isapd_ly, 0, '')} vs a year ago" if isapd_ly is not None else None,
                  isapd <= isapd_ly if isapd_ly is not None else None,
                  f"PD {dl['pd']}, ISA {dl['ila']}. Mattilsynet: {ncase} active cases at {len(sites)} sites, "
                  f"{new} new in the last 14 days.", mlabel(dl["t"]))
    dt = pct(last["deadt"], ly["deadt"]) if ly else None
    hl = f"{fmt(last['dead'], 2)} M fish, {fmt(last['deadt'] / 1000, 1)} kt ({sign(dt)} on a year earlier). " if dt is not None else ""
    if nxt:
        hl += f"Silage-vessel visits point to {fmt(nxt['est'] / 1000, 1)} kt in {MO[nxt['t'] % 12]}" + \
              (f" ({sign(pct(nxt['est'], nly['deadt']))} on a year earlier)." if nly and nly["deadt"] else ".")
    rate_ly = ly["rate"] if ly else None
    mort = card("mortality", "dodelighet.html", "Mortality", "Mortality rate", fmt(last["rate"], 2), "% / month",
                f"{sign(last['rate'] - rate_ly, 2, ' pp')} vs a year ago" if rate_ly is not None else None,
                last["rate"] <= rate_ly if rate_ly is not None else None, hl, mlabel(last["t"]))
    return health, mort


def traffic():
    d = load("traffic.html"); e = d.get("export"); t = d["types"]["Alle"]
    bt = [r for r in (e or {}).get("backtest", []) if r.get("actual") is not None]
    lastw = bt[-1] if bt else None
    hl = (f"{lastw['label'].replace('U', 'Week ')} exported {fmt(lastw['actual'] / 1000, 1)} kt (model {fmt(lastw['predicted'] / 1000, 1)}). " if lastw else "") + \
         f"Farm-site visits this week {t['wtd_diff_label']} vs the same point last week."
    yd = datetime.date.fromisoformat(d["yesterday"])
    return card("traffic", "traffic.html", "Harvest traffic and exports", f"Export estimate, week {e['week']}" if e else "Export estimate",
                fmt(e["forecast"] / 1000, 1) if e and e.get("forecast") else "–", "kt",
                f"±{e['rmse_pct']}% typical error" if e else None, None, hl, f"Visits through {yd.day} {MO[yd.month - 1]}")


def safe(fn, key, href, area):
    try:
        return fn()
    except Exception as ex:          # one broken page must not take the hub down
        print(f"  {key}: could not read ({ex!r})")
        return card(key, href, area, "Not available", "–", "", highlight="The page's data could not be read on this run.")


def main():
    cards = [safe(control, "control", "kontroll.html", "Biological control")]
    hm = safe(fish_health_and_mortality, "health", "fiskehelse.html", "Fish health")
    health, mort = hm if isinstance(hm, tuple) else (hm, card("mortality", "dodelighet.html", "Mortality", "Not available", "–", ""))
    cards += [safe(traffic, "traffic", "traffic.html", "Harvest traffic and exports"),
              safe(feed, "feed", "foring.html", "Feeding"),
              safe(lice, "lice", "lakselus.html", "Lice and treatments"),
              health, mort]
    data = {"cards": cards, "updated": datetime.datetime.now(datetime.timezone.utc).strftime("%d %b %Y %H:%M UTC")}
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read().replace("__DATA__", json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    for c in cards:
        print(f"{c['area']:30} {c['value']:>8} {c['unit']:8} {c['delta'] or ''} | {c['highlight']}")
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")


if __name__ == "__main__":
    main()
