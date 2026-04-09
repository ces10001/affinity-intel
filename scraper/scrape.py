#!/usr/bin/env python3
"""AFFINITY SCRAPER v8b — Affinity via Dutchie, competitors via Leefii"""

import json, os, sys, time, re, requests
from datetime import datetime

SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

STORES = [
    # Affinity — Dutchie direct pages
    {"name": "Affinity NH (Rec)", "url": "https://dutchie.com/dispensary/affinity-health-and-wellness-rec"},
    {"name": "Affinity NH (Med)", "url": "https://dutchie.com/dispensary/affinity-health-and-wellness"},
    {"name": "Affinity BP (Rec)", "url": "https://dutchie.com/dispensary/affinity-bridgeport-rec"},
    {"name": "Affinity BP (Med)", "url": "https://dutchie.com/dispensary/affinity-bridgeport"},
    # Competitors — verified Leefii URLs + 1 Weedmaps
    {"name": "Higher Collective Bridgeport", "url": "https://leefii.com/dispensary/higher-collective-bridgeport"},
    {"name": "Lit New Haven Cannabis", "url": "https://leefii.com/dispensary/lit-new-haven-cannabis"},
    {"name": "Insa New Haven", "url": "https://leefii.com/dispensary/insa-cannabis-dispensary-new-haven"},
    {"name": "RISE Dispensary Orange", "url": "https://leefii.com/dispensary/rise-medical-recreational-cannabis-dispensary-orange"},
    {"name": "RISE Dispensary Branford", "url": "https://leefii.com/dispensary/rise-medical-recreational-cannabis-dispensary-branford"},
    {"name": "High Profile Hamden", "url": "https://weedmaps.com/dispensaries/high-profile-hamden"},
    {"name": "Hi! People Derby", "url": "https://leefii.com/dispensary/hi-people-derby"},
    {"name": "Budr Cannabis Stratford", "url": "https://leefii.com/dispensary/budr-cannabis-stratford"},
    {"name": "Sweetspot Stamford", "url": "https://leefii.com/dispensary/sweetspot-cannabis-dispensary-stamford"},
    {"name": "Fine Fettle Newington", "url": "https://leefii.com/dispensary/fine-fettle-newington-dispensary"},
    {"name": "Curaleaf Stamford", "url": "https://leefii.com/dispensary/curaleaf-dispensary-stamford"},
]

HEADERS = {}

def init():
    global HEADERS
    HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def scrape_page(name, url):
    print(f"  -> {name}")
    print(f"     {url}")
    try:
        resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code == 402:
            print(f"     OUT OF CREDITS"); return "no_credits"
        if resp.status_code == 429:
            print(f"     rate limited, waiting 25s..."); time.sleep(25)
            resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code != 200:
            print(f"     HTTP {resp.status_code}"); return None
        data = resp.json()
        if not data.get("success"):
            print(f"     failed: {data.get('error','?')[:100]}"); return None
        md = data.get("data", {}).get("markdown", "")
        if not md:
            print(f"     empty page"); return None
        products = parse_leefii(md)
        deals = parse_deals(md)
        print(f"     OK: {len(products)} products, {len(deals)} deals")
        return {"products": products, "deals": deals}
    except Exception as e:
        print(f"     error: {e}"); return None


def parse_leefii(md):
    """
    Parses Leefii and Dutchie markdown.
    Leefii format:
      ### Product Name Here
      Brand Name  Weight  StrainType
      $XX.00
    Dutchie format varies but also has product names + prices.
    """
    products = []
    lines = md.split("\n")
    price_pat = re.compile(r'^\$(\d+\.?\d*)$')
    header_pat = re.compile(r'^#{1,4}\s+(.+)')
    weight_pat = re.compile(r'(\d+(?:/\d+)?\s*(?:g|oz|mg|ml)\b)', re.I)
    seen = set()

    pending_name = None
    pending_meta = None
    global_cat = "Other"

    for i, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line:
            continue

        # Track global category from filter tabs
        lo = line.lower()
        if lo in ["flower", "🌿 flower", "pre-rolls", "🚬 pre-rolls", "edibles", "🍪 edibles",
                   "concentrates", "💎 concentrates", "vaporizers", "tinctures", "accessories",
                   "🌱 accessories", "🌱 drink", "drink", "drinks"]:
            cat_map = {"flower": "Flower", "🌿 flower": "Flower", "pre-rolls": "Pre-Rolls",
                       "🚬 pre-rolls": "Pre-Rolls", "edibles": "Edibles", "🍪 edibles": "Edibles",
                       "concentrates": "Concentrates", "💎 concentrates": "Concentrates",
                       "vaporizers": "Vaporizers", "tinctures": "Tinctures",
                       "🌱 drink": "Edibles", "drink": "Edibles", "drinks": "Edibles",
                       "accessories": "SKIP", "🌱 accessories": "SKIP"}
            global_cat = cat_map.get(lo, "Other")
            continue

        if global_cat == "SKIP":
            # Reset if we hit a new category header
            if header_pat.match(line) and any(c in lo for c in ["flower", "pre-roll", "edible", "vape", "concentrate", "tincture"]):
                global_cat = "Other"
            continue

        # Detect ### headers = product names
        hm = header_pat.match(line)
        if hm:
            pending_name = hm.group(1).strip()
            pending_meta = None
            continue

        # Detect price line
        pm = price_pat.match(line)
        if pm and pending_name:
            price = float(pm.group(1))
            if price < 1 or price > 500:
                pending_name = None
                pending_meta = None
                continue

            name = pending_name
            brand = "Unknown"
            weight = ""
            strain = ""

            # Clean emoji prefixes
            name = re.sub(r'^[🌿🚬🍪💎🌱]\s*', '', name).strip()

            # Extract brand from "Brand | Product" format
            if " | " in name:
                parts = name.split(" | ", 1)
                brand = parts[0].strip()
                name = parts[1].strip()
            elif " - " in name and len(name.split(" - ", 1)[0]) < 30:
                parts = name.split(" - ", 1)
                if not any(c.isdigit() for c in parts[0]):
                    brand = parts[0].strip()
                    name = parts[1].strip()

            # Parse metadata line
            if pending_meta:
                meta = pending_meta
                wm = weight_pat.search(meta)
                if wm:
                    weight = wm.group(1)
                for st in ["Hybrid", "Indica", "Sativa"]:
                    if st in meta:
                        strain = st
                        break
                if brand == "Unknown":
                    meta_clean = meta
                    meta_clean = re.sub(r'\d+(?:/\d+)?\s*(?:g|oz|mg|ml)\b', '', meta_clean, flags=re.I)
                    meta_clean = re.sub(r'(Hybrid|Indica|Sativa|each)', '', meta_clean, flags=re.I)
                    meta_clean = meta_clean.strip()
                    if meta_clean and len(meta_clean) > 1 and len(meta_clean) < 50:
                        brand = meta_clean

            # Determine category
            cat = global_cat
            name_lo = name.lower()
            if any(w in name_lo for w in ["pre-roll", "preroll", "pre roll", "shortie", "fatboy", "dogwalker", "mini", "5 pack", "5pk"]):
                cat = "Pre-Rolls"
            elif any(w in name_lo for w in ["vape", "cart", "pen", "disposable", "510"]):
                cat = "Vaporizers"
            elif any(w in name_lo for w in ["gummies", "gummy", "chocolate", "chew", "edible", "tablet", "seltzer", "lemonade", "iced tea", "soda", "jellies"]):
                cat = "Edibles"
            elif any(w in name_lo for w in ["rosin", "badder", "shatter", "wax", "sugar", "concentrate", "diamonds", "sauce"]):
                cat = "Concentrates"
            elif any(w in name_lo for w in ["tincture", "rso"]):
                cat = "Tinctures"
            elif any(w in name_lo for w in ["flower", "ground", "whole flower"]) or weight in ["1/8 oz", "3.5 g", "7 g", "14 g", "28 g"]:
                cat = "Flower"

            # Skip non-cannabis
            if any(w in name_lo for w in ["paper", "lighter", "grinder", "tray", "pipe", "journal", "book",
                                           "shirt", "tee", "hat", "sock", "mug", "sweatshirt", "beanie",
                                           "battery", "accessories", "spray", "ozium", "rolling"]):
                pending_name = None
                pending_meta = None
                continue

            # Deduplicate
            key = f"{name}:{price}"
            if key in seen:
                pending_name = None
                pending_meta = None
                continue
            seen.add(key)

            products.append({
                "name": name[:100],
                "brand": brand[:50],
                "category": cat if cat != "SKIP" else "Other",
                "price": price,
                "weight": weight,
                "strain": strain,
            })

            pending_name = None
            pending_meta = None
            continue

        # Metadata line (between header and price)
        if pending_name and not hm:
            if len(line) < 100 and not line.startswith("[") and not line.startswith("http"):
                pending_meta = line

    return products


def parse_deals(md):
    deals = []
    pats = [
        re.compile(r'(\d+%\s*off[^.!\n]{3,60})', re.I),
        re.compile(r'(buy\s+\d+\s+get\s+\d+[^.!\n]{3,60})', re.I),
        re.compile(r'(bogo[^.!\n]{3,60})', re.I),
        re.compile(r'(happy\s+hour[^.!\n]{3,60})', re.I),
        re.compile(r'(first.time[^.!\n]{3,40}(?:discount|off)[^.!\n]{0,30})', re.I),
    ]
    seen = set()
    for pat in pats:
        for m in pat.finditer(md):
            t = m.group(1).strip()[:100]
            if t not in seen:
                seen.add(t)
                deals.append({"title": t, "discount_type": "percent", "category": "All"})
    return deals


def build_dash(results):
    pm = {}
    for disp, data in results.items():
        if not data or not isinstance(data, dict): continue
        # Normalize Affinity display names
        display = disp
        if "Affinity NH" in disp:
            display = "Affinity - New Haven"
        elif "Affinity BP" in disp:
            display = "Affinity - Bridgeport"
        menu_type = " (Rec)" if "(Rec)" in disp else " (Med)" if "(Med)" in disp else ""

        for p in data.get("products", []):
            nm = (p.get("name") or "").strip()
            br = (p.get("brand") or "Unknown").strip()
            ct = (p.get("category") or "Other").strip()
            pr = p.get("price")
            wt = (p.get("weight") or "").strip()
            if not nm or not pr or pr <= 0: continue
            key = f"{nm}".lower()
            if key not in pm:
                pm[key] = {"name": nm, "brand": br, "category": ct, "weight": wt, "dispensaries": {}}
            label = f"{display}{menu_type}".strip()
            if label in pm[key]["dispensaries"]:
                pm[key]["dispensaries"][label] = min(pm[key]["dispensaries"][label], round(float(pr), 2))
            else:
                pm[key]["dispensaries"][label] = round(float(pr), 2)

    comp = sorted([v for v in pm.values() if len(v["dispensaries"]) >= 2], key=lambda x: (x["category"], x["name"]))
    allp = sorted(pm.values(), key=lambda x: (x["category"], x["name"]))
    out = comp if len(comp) >= 5 else allp
    deals = []
    for disp, data in results.items():
        if not data or not isinstance(data, dict): continue
        for d in data.get("deals", []):
            if d.get("title"):
                deals.append({"dispensary": disp, "title": d["title"], "type": d.get("discount_type", "other"), "category": "All", "expires": None})
    return {
        "scraped_at": datetime.now().isoformat(),
        "products": out, "deals": deals,
        "stats": {
            "total": sum(len(d["products"]) for d in results.values() if d and isinstance(d, dict)),
            "comparable": len(comp), "all": len(allp),
            "success": len([v for v in results.values() if v and isinstance(v, dict) and v.get("products")]),
            "failed": len([v for v in results.values() if not v or not isinstance(v, dict) or not v.get("products")]),
            "deals": len(deals),
        },
    }


def main():
    if not API_KEY:
        print("ERROR: FIRECRAWL_API_KEY not set"); sys.exit(1)
    init()
    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER v8b (Dutchie + Leefii)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(STORES)} dispensaries")
    print(f"{'='*60}\n")
    results = {}
    for s in STORES:
        result = scrape_page(s["name"], s["url"])
        if result == "no_credits":
            print("\n  !!! OUT OF CREDITS !!!\n"); break
        results[s["name"]] = result
        time.sleep(6)
    dash = build_dash(results)
    print(f"\n{'='*60}")
    for k, v in dash["stats"].items(): print(f"  {k}: {v}")
    print(f"{'='*60}\n")
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"data/raw_{ts}.json", "w") as f: json.dump(results, f, indent=2, default=str)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out, "w") as f: json.dump(dash, f, indent=2, default=str)
    print("  DONE\n")

if __name__ == "__main__":
    main()
