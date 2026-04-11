#!/usr/bin/env python3
"""
Nigeria Mineral Data Fetcher
Pulls from USGS MRDS, OpenStreetMap Overpass, and EITI/ResourceProjects.
Deduplicates and outputs minerals_enriched.json ready for map integration.

Usage:
    python3 fetch_minerals.py

Output:
    minerals_enriched.json   — full deduplicated dataset
    source_report.txt        — per-source stats and errors
    gap_report.txt           — known sites not found in any source
"""

import json, csv, io, sys, time, urllib.request, urllib.error, urllib.parse
from collections import defaultdict
from math import radians, cos, sin, asin, sqrt

# ── Config ──────────────────────────────────────────────────────────────────
NIGERIA_BBOX = {"lat_min": 4.0, "lat_max": 13.9, "lng_min": 2.7, "lng_max": 14.7}
DEDUP_THRESHOLD_DEG = 0.05  # ~5.5 km
OUTPUT_FILE = "minerals_enriched.json"
REPORT_FILE = "source_report.txt"
GAP_FILE = "gap_report.txt"

# Sites already on the map — skip these
EXISTING_SITES = [
    "Anka Gold","Birnin Gwari Gold","Maru Gold","Ilesa-Imesi Gold","Itakpe Iron Ore",
    "Agbaja Iron Ore","Ajaokuta Steel","Barkin Ladi Cassiterite","Bukuru-Jos Tin",
    "Abakaliki Lead-Zinc","Ishiagu Lead-Zinc","Nasarawa Critical Hub","Iva Valley Coal",
    "Okaba Coal","Agbabu Bitumen","Edo Bitumen","Ogun Bitumen","Obajana Limestone",
    "Ewekoro Limestone","Nkalagu Limestone","Sokoto Gypsum","Aliade Baryte",
    "Azara Baryte","Tiken Baryte","Ijero-Ekiti Kaolin","Kaduna Gemstone","Katsina Diamond",
    "Segilola Gold","Bin Yauri Gold","Komu Pegmatite","Ririwai Ring Complex",
    "Owukpa Coal","Lafia-Obi Coal","Mfamosing Limestone","Ashaka Limestone",
    "Igbeti Marble","Tajimi Iron Ore","Enyigba Lead-Zinc","Isanlu Talc","Oyo Tourmaline",
]

# Known high-value targets to verify
HIGH_VALUE_TARGETS = [
    {"name": "Bukuru Tin", "lat": 9.80, "lng": 8.87, "commodity": "Tin"},
    {"name": "Rayfield Tin", "lat": 9.85, "lng": 8.87, "commodity": "Tin"},
    {"name": "Ropp Tin", "lat": 9.78, "lng": 8.83, "commodity": "Tin"},
    {"name": "Sura Tin", "lat": 9.50, "lng": 9.30, "commodity": "Tin"},
    {"name": "Nasarawa Lithium (Kogin Baba)", "lat": 8.50, "lng": 8.20, "commodity": "Lithium"},
    {"name": "Kwatarkwashi Gold", "lat": 12.20, "lng": 6.50, "commodity": "Gold"},
    {"name": "Agbaja Iron Ore Plateau", "lat": 7.70, "lng": 6.60, "commodity": "Iron Ore"},
    {"name": "Obudu Barite", "lat": 6.60, "lng": 9.20, "commodity": "Barite"},
    {"name": "Ishiagu Lead-Zinc", "lat": 5.90, "lng": 7.60, "commodity": "Lead-Zinc"},
    {"name": "Taraba Gemstones", "lat": 8.00, "lng": 10.50, "commodity": "Gemstones"},
]

# ── Commodity normalisation ─────────────────────────────────────────────────
COMMODITY_MAP = {
    "gold": ("Gold", "precious"), "silver": ("Silver", "precious"),
    "platinum": ("Platinum", "precious"), "gemstone": ("Gemstones", "precious"),
    "sapphire": ("Gemstones", "precious"), "tourmaline": ("Gemstones", "precious"),
    "ruby": ("Gemstones", "precious"), "topaz": ("Gemstones", "precious"),
    "aquamarine": ("Gemstones", "precious"), "emerald": ("Gemstones", "precious"),
    "diamond": ("Diamonds", "precious"), "diamonds": ("Diamonds", "precious"),
    "zircon": ("Gemstones", "precious"),
    "iron": ("Iron Ore", "base_iron"), "iron ore": ("Iron Ore", "base_iron"),
    "steel": ("Steel", "base_iron"),
    "columbite": ("Columbite", "base_iron"), "niobium": ("Columbite", "base_iron"),
    "tantalite": ("Tantalite", "critical_battery"), "tantalum": ("Tantalite", "critical_battery"),
    "tin": ("Tin", "base_iron"), "cassiterite": ("Tin", "base_iron"),
    "lead": ("Lead", "base_iron"), "zinc": ("Zinc", "base_iron"),
    "copper": ("Copper", "base_iron"),
    "lithium": ("Lithium", "critical_battery"), "cobalt": ("Cobalt", "critical_battery"),
    "manganese": ("Manganese", "critical_battery"), "graphite": ("Graphite", "critical_battery"),
    "ree": ("REE", "critical_battery"), "rare earth": ("REE", "critical_battery"),
    "coal": ("Coal", "energy"), "bitumen": ("Bitumen", "energy"),
    "uranium": ("Uranium", "energy"),
    "limestone": ("Limestone", "industrial"), "marble": ("Limestone", "industrial"),
    "gypsum": ("Gypsum", "industrial"), "barite": ("Baryte", "industrial"),
    "baryte": ("Baryte", "industrial"), "barium": ("Baryte", "industrial"),
    "kaolin": ("Kaolin", "industrial"), "clay": ("Kaolin", "industrial"),
    "salt": ("Salt", "industrial"), "halite": ("Salt", "industrial"),
    "feldspar": ("Feldspar", "industrial"), "mica": ("Mica", "industrial"),
    "talc": ("Kaolin", "industrial"), "phosphate": ("Phosphate", "energy"),
    "granite": ("Granite", "industrial"), "sand": ("Sand", "industrial"),
    "gravel": ("Gravel", "industrial"), "laterite": ("Laterite", "industrial"),
    "glass sand": ("Sand", "industrial"), "silica": ("Sand", "industrial"),
    "molybdenum": ("Molybdenum", "base_iron"), "tungsten": ("Tungsten", "base_iron"),
    "beryllium": ("Beryllium", "critical_battery"),
    "monazite": ("REE", "critical_battery"),
}

def normalise_commodity(raw):
    """Map a raw commodity string to (display_name, category)."""
    if not raw:
        return ("Unknown", "industrial")
    key = raw.strip().lower().replace("-", " ").replace("_", " ")
    # Try exact match
    if key in COMMODITY_MAP:
        return COMMODITY_MAP[key]
    # Try partial match
    for k, v in COMMODITY_MAP.items():
        if k in key or key in k:
            return v
    return (raw.strip().title(), "industrial")


def in_nigeria(lat, lng):
    """Check if coordinates are within Nigeria bounding box."""
    return (NIGERIA_BBOX["lat_min"] <= lat <= NIGERIA_BBOX["lat_max"] and
            NIGERIA_BBOX["lng_min"] <= lng <= NIGERIA_BBOX["lng_max"])


def haversine(lat1, lng1, lat2, lng2):
    """Distance in degrees (approximate)."""
    return ((lat1 - lat2)**2 + (lng1 - lng2)**2)**0.5


def is_existing(name, lat, lng):
    """Check if a site matches an existing map entry."""
    name_lower = name.lower() if name else ""
    for existing in EXISTING_SITES:
        if existing.lower() in name_lower or name_lower in existing.lower():
            return True
    return False


def fetch_url(url, timeout=30):
    """Fetch URL with error handling."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "NigeriaMinералFetcher/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8"), resp.status
    except urllib.error.HTTPError as e:
        return None, e.code
    except Exception as e:
        return None, str(e)


# ── Source 1: USGS MRDS ─────────────────────────────────────────────────────
def fetch_usgs_mrds():
    """Fetch from USGS MRDS API."""
    records = []
    report = {"source": "USGS MRDS", "retrieved": 0, "usable": 0, "dropped": 0, "errors": []}

    # Try GeoJSON format first
    urls = [
        "https://mrdata.usgs.gov/api/mrds?country=Nigeria&fmt=geojson",
        "https://mrdata.usgs.gov/api/mrds?country=Nigeria&fmt=json",
        "https://mrdata.usgs.gov/mrds/find-mrds.php?country=NI&recno=&op=Select&fmt=csv",
    ]

    data = None
    for url in urls:
        print(f"  Trying: {url[:80]}...")
        content, status = fetch_url(url, timeout=60)
        if content and status == 200:
            data = content
            report["errors"].append(f"OK from {url}")
            break
        else:
            report["errors"].append(f"{status} from {url}")

    if not data:
        print("  USGS MRDS: All endpoints failed")
        return records, report

    # Try parsing as GeoJSON
    try:
        gj = json.loads(data)
        features = gj.get("features", [])
        report["retrieved"] = len(features)
        for f in features:
            props = f.get("properties", {})
            geom = f.get("geometry", {})
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                report["dropped"] += 1
                continue
            lng, lat = float(coords[0]), float(coords[1])
            if not in_nigeria(lat, lng):
                report["dropped"] += 1
                continue
            name = props.get("site_name", props.get("name", "Unknown"))
            if is_existing(name, lat, lng):
                report["dropped"] += 1
                continue
            commod = props.get("commod1", props.get("commodity", ""))
            commod2 = props.get("commod2", "")
            commod3 = props.get("commod3", "")
            dev_stat = props.get("dev_stat", props.get("oper_type", ""))
            display, category = normalise_commodity(commod)
            records.append({
                "name": name,
                "lat": round(lat, 6),
                "lng": round(lng, 6),
                "commodity": display,
                "commodity_raw": commod,
                "commodity2": commod2,
                "commodity3": commod3,
                "category": category,
                "source": "USGS",
                "dev_stat": dev_stat,
                "priority": "producer" in dev_stat.lower() if dev_stat else False,
                "notes": f"MRDS dep_id={props.get('dep_id', 'N/A')}",
            })
            report["usable"] += 1
        print(f"  USGS GeoJSON: {report['usable']} usable records")
        return records, report
    except (json.JSONDecodeError, KeyError):
        pass

    # Try parsing as CSV
    try:
        reader = csv.DictReader(io.StringIO(data))
        rows = list(reader)
        report["retrieved"] = len(rows)
        for row in rows:
            lat_str = row.get("latitude", row.get("lat", ""))
            lng_str = row.get("longitude", row.get("lon", row.get("long", "")))
            if not lat_str or not lng_str:
                report["dropped"] += 1
                continue
            try:
                lat, lng = float(lat_str), float(lng_str)
            except ValueError:
                report["dropped"] += 1
                continue
            if not in_nigeria(lat, lng):
                report["dropped"] += 1
                continue
            name = row.get("site_name", row.get("name", "Unknown"))
            if is_existing(name, lat, lng):
                report["dropped"] += 1
                continue
            commod = row.get("commod1", row.get("commodity", ""))
            display, category = normalise_commodity(commod)
            dev_stat = row.get("dev_stat", row.get("oper_type", ""))
            records.append({
                "name": name,
                "lat": round(lat, 6),
                "lng": round(lng, 6),
                "commodity": display,
                "commodity_raw": commod,
                "commodity2": row.get("commod2", ""),
                "commodity3": row.get("commod3", ""),
                "category": category,
                "source": "USGS",
                "dev_stat": dev_stat,
                "priority": "producer" in dev_stat.lower() if dev_stat else False,
                "notes": f"MRDS rec={row.get('rec_id', row.get('dep_id', 'N/A'))}",
            })
            report["usable"] += 1
        print(f"  USGS CSV: {report['usable']} usable records")
    except Exception as e:
        report["errors"].append(f"CSV parse error: {e}")

    return records, report


# ── Source 2: OpenStreetMap Overpass ─────────────────────────────────────────
def fetch_overpass():
    """Fetch mines/quarries from Overpass API."""
    records = []
    report = {"source": "OpenStreetMap Overpass", "retrieved": 0, "usable": 0, "dropped": 0, "errors": []}

    query = """[out:json][timeout:60];
(
  node["landuse"="quarry"](4.0,2.7,13.9,14.7);
  node["man_made"="mineshaft"](4.0,2.7,13.9,14.7);
  node["industrial"="mine"](4.0,2.7,13.9,14.7);
  way["landuse"="quarry"](4.0,2.7,13.9,14.7);
  node["man_made"="works"]["product"](4.0,2.7,13.9,14.7);
  node["industrial"="mineral_processing"](4.0,2.7,13.9,14.7);
);
out center;"""

    url = "https://overpass-api.de/api/interpreter"
    encoded = urllib.parse.urlencode({"data": query})
    print(f"  Trying: Overpass API...")

    content, status = fetch_url(f"{url}?{encoded}", timeout=90)
    if not content or status != 200:
        report["errors"].append(f"Overpass API: {status}")
        # Try mirror
        content, status = fetch_url(f"https://overpass.kumi.systems/api/interpreter?{encoded}", timeout=90)
        if not content or status != 200:
            report["errors"].append(f"Overpass mirror: {status}")
            print("  Overpass: All endpoints failed")
            return records, report

    try:
        data = json.loads(content)
        elements = data.get("elements", [])
        report["retrieved"] = len(elements)

        for el in elements:
            lat = el.get("lat", el.get("center", {}).get("lat"))
            lng = el.get("lon", el.get("center", {}).get("lon"))
            if not lat or not lng:
                report["dropped"] += 1
                continue
            if not in_nigeria(float(lat), float(lng)):
                report["dropped"] += 1
                continue

            tags = el.get("tags", {})
            name = tags.get("name", tags.get("operator", f"OSM-{el.get('id', 'unknown')}"))
            if is_existing(name, float(lat), float(lng)):
                report["dropped"] += 1
                continue

            # Determine commodity from tags
            resource = tags.get("resource", tags.get("product", tags.get("raw_material", "")))
            if not resource:
                # Guess from name
                name_lower = name.lower()
                for keyword, (display, cat) in COMMODITY_MAP.items():
                    if keyword in name_lower:
                        resource = keyword
                        break

            display, category = normalise_commodity(resource) if resource else ("Quarry/Mine", "industrial")

            records.append({
                "name": name,
                "lat": round(float(lat), 6),
                "lng": round(float(lng), 6),
                "commodity": display,
                "commodity_raw": resource,
                "commodity2": "",
                "commodity3": "",
                "category": category,
                "source": "OSM",
                "dev_stat": "Active" if tags.get("disused") != "yes" else "Disused",
                "priority": False,
                "notes": f"OSM id={el.get('id', '')} type={el.get('type', '')}",
            })
            report["usable"] += 1

        print(f"  Overpass: {report['usable']} usable records")
    except Exception as e:
        report["errors"].append(f"Parse error: {e}")

    return records, report


# ── Source 3: EITI / ResourceProjects ────────────────────────────────────────
def fetch_eiti():
    """Try EITI and ResourceProjects APIs."""
    records = []
    report = {"source": "EITI/ResourceProjects", "retrieved": 0, "usable": 0, "dropped": 0, "errors": []}

    urls = [
        "https://eiti.org/api/countries/NG/production",
        "https://eiti.org/api/v1.0/organisation?filter[country]=NG",
        "https://resourceprojects.org/api/projects?country=NG&sector=mining",
    ]

    for url in urls:
        print(f"  Trying: {url[:80]}...")
        content, status = fetch_url(url, timeout=30)
        if content and status == 200:
            report["errors"].append(f"OK from {url}")
            try:
                data = json.loads(content)
                # Try to extract records — structure varies by API
                items = data if isinstance(data, list) else data.get("data", data.get("results", []))
                if isinstance(items, list):
                    report["retrieved"] += len(items)
                    for item in items:
                        lat = item.get("latitude", item.get("lat"))
                        lng = item.get("longitude", item.get("lng", item.get("lon")))
                        if lat and lng:
                            name = item.get("name", item.get("project_name", "Unknown"))
                            commod = item.get("commodity", item.get("sector", ""))
                            display, category = normalise_commodity(commod)
                            records.append({
                                "name": name,
                                "lat": round(float(lat), 6),
                                "lng": round(float(lng), 6),
                                "commodity": display,
                                "commodity_raw": commod,
                                "commodity2": "", "commodity3": "",
                                "category": category,
                                "source": "EITI",
                                "dev_stat": item.get("status", ""),
                                "priority": False,
                                "notes": f"EITI/RP",
                            })
                            report["usable"] += 1
            except Exception as e:
                report["errors"].append(f"Parse error from {url}: {e}")
        else:
            report["errors"].append(f"{status} from {url}")

    print(f"  EITI: {report['usable']} usable records")
    return records, report


# ── Source 4: Mining Cadastre ────────────────────────────────────────────────
def fetch_cadastre():
    """Try Nigerian Mining Cadastre Office."""
    records = []
    report = {"source": "Nigeria Mining Cadastre", "retrieved": 0, "usable": 0, "dropped": 0, "errors": []}

    urls = [
        "https://minesng.gov.ng/api/licenses",
        "https://minesng.gov.ng/api/data",
        "https://cadastre.minesng.gov.ng/api/licenses",
    ]

    for url in urls:
        print(f"  Trying: {url}...")
        content, status = fetch_url(url, timeout=20)
        if content and status == 200:
            report["errors"].append(f"OK from {url}")
            try:
                data = json.loads(content)
                items = data if isinstance(data, list) else data.get("data", data.get("features", []))
                report["retrieved"] = len(items) if isinstance(items, list) else 0
            except Exception as e:
                report["errors"].append(f"Parse error: {e}")
        else:
            report["errors"].append(f"{status} from {url}")

    print(f"  Cadastre: {report['usable']} usable records")
    return records, report


# ── Deduplication ────────────────────────────────────────────────────────────
def deduplicate(records):
    """Remove duplicates within DEDUP_THRESHOLD_DEG and same commodity."""
    if not records:
        return records

    # Sort by priority (True first) then by source quality (USGS > OSM > EITI)
    source_rank = {"USGS": 0, "EITI": 1, "OSM": 2}
    records.sort(key=lambda r: (not r["priority"], source_rank.get(r["source"], 9)))

    kept = []
    for rec in records:
        is_dup = False
        for existing in kept:
            dist = haversine(rec["lat"], rec["lng"], existing["lat"], existing["lng"])
            if dist < DEDUP_THRESHOLD_DEG:
                # Same area — check if same commodity family
                if rec["category"] == existing["category"]:
                    is_dup = True
                    break
        if not is_dup:
            kept.append(rec)

    return kept


# ── Gap analysis ─────────────────────────────────────────────────────────────
def find_gaps(records):
    """Check which high-value targets were NOT found."""
    gaps = []
    for target in HIGH_VALUE_TARGETS:
        found = False
        for rec in records:
            dist = haversine(target["lat"], target["lng"], rec["lat"], rec["lng"])
            if dist < 0.15:  # ~16 km
                found = True
                break
        # Also check existing sites
        if not found:
            for existing in EXISTING_SITES:
                if target["name"].split()[0].lower() in existing.lower():
                    found = True
                    break
        if not found:
            gaps.append(target)
    return gaps


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Nigeria Mineral Data Fetcher")
    print("=" * 60)

    all_records = []
    all_reports = []

    # Step 1: USGS MRDS
    print("\n[1/4] USGS MRDS...")
    records, report = fetch_usgs_mrds()
    all_records.extend(records)
    all_reports.append(report)

    # Step 2: Overpass
    print("\n[2/4] OpenStreetMap Overpass...")
    records, report = fetch_overpass()
    all_records.extend(records)
    all_reports.append(report)

    # Step 3: EITI
    print("\n[3/4] EITI / ResourceProjects...")
    records, report = fetch_eiti()
    all_records.extend(records)
    all_reports.append(report)

    # Step 4: Cadastre
    print("\n[4/4] Mining Cadastre...")
    records, report = fetch_cadastre()
    all_records.extend(records)
    all_reports.append(report)

    # Deduplicate
    print(f"\n{'=' * 60}")
    print(f"Total raw records: {len(all_records)}")
    deduped = deduplicate(all_records)
    print(f"After deduplication: {len(deduped)}")

    # Category breakdown
    cats = defaultdict(int)
    for r in deduped:
        cats[r["category"]] += 1
    print("\nBy category:")
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

    # Priority breakdown
    priority_count = sum(1 for r in deduped if r["priority"])
    print(f"\nPriority (producers): {priority_count}")
    print(f"Non-priority: {len(deduped) - priority_count}")

    # Write output
    with open(OUTPUT_FILE, "w") as f:
        json.dump(deduped, f, indent=2)
    print(f"\nWrote {len(deduped)} records to {OUTPUT_FILE}")

    # Write category split
    by_cat = defaultdict(list)
    for r in deduped:
        by_cat[r["category"]].append(r)
    with open("minerals_by_category.json", "w") as f:
        json.dump(dict(by_cat), f, indent=2)
    print(f"Wrote minerals_by_category.json")

    # Source report
    with open(REPORT_FILE, "w") as f:
        f.write("Nigeria Mineral Data — Source Report\n")
        f.write("=" * 50 + "\n\n")
        for rep in all_reports:
            f.write(f"Source: {rep['source']}\n")
            f.write(f"  Retrieved: {rep['retrieved']}\n")
            f.write(f"  Usable:    {rep['usable']}\n")
            f.write(f"  Dropped:   {rep['dropped']}\n")
            f.write(f"  Errors:\n")
            for err in rep["errors"]:
                f.write(f"    - {err}\n")
            f.write("\n")
        f.write(f"Total after dedup: {len(deduped)}\n")
    print(f"Wrote {REPORT_FILE}")

    # Gap analysis
    gaps = find_gaps(deduped)
    with open(GAP_FILE, "w") as f:
        f.write("Gap Report — Known sites NOT found in any free source\n")
        f.write("=" * 50 + "\n")
        f.write("These need manual sourcing from MoFI/NGSA/field data.\n\n")
        if gaps:
            for g in gaps:
                f.write(f"- {g['name']} ({g['commodity']}) — approx {g['lat']:.2f}°N, {g['lng']:.2f}°E\n")
        else:
            f.write("All high-value targets were found in at least one source.\n")
    print(f"Wrote {GAP_FILE} ({len(gaps)} gaps)")

    print(f"\n{'=' * 60}")
    print(f"Done. Next step: paste {OUTPUT_FILE} contents into the Claude Code")
    print(f"session and I will add all records to the map.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
