#!/usr/bin/env python3
"""
AFFINITY COMPETITIVE INTEL — Firecrawl Scraper v3
Uses /v2/extract for AI extraction, /v2/scrape as markdown fallback.
"""

import json
import os
import sys
import time
import re
import requests
from datetime import datetime

EXTRACT_URL = "https://api.firecrawl.dev/v2/extract"
SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

AFFINITY_STORES = [
    {"name": "Affinity - New Haven", "url": "https://affinitydispensary.com/new-haven-menu/"},
    {"name": "Affinity - Bridgeport", "url": "https://affinitydispensary.com/bridgeport-menu/"},
]

COMPETITORS = [
    {"name": "Higher Collective Bridgeport", "url": "https://highercollectivect.com/bridgeport-menu/"},
    {"name": "Lit New Haven Cannabis", "url": "https://www.litnhv.com/menu"},
    {"name": "Insa New Haven", "url": "https://www.insacannabis.com/dispensaries/new-haven-ct"},
    {"name": "RISE Dispensary Orange", "url": "https://risecannabis.com/dispensaries/connecticut/orange"},
    {"name": "High Profile Hamden", "url": "https://highprofilecannabisnow.com/locations/hamden-ct"},
    {"name": "Hi! People Derby", "url": "https://hipeople.co/derby"},
    {"name": "Rejoice Seymour", "url": "https://rejoicedispensary.com/seymour"},
    {"name": "Budr Cannabis Stratford", "url": "https://www.budr.com/stratford"},
    {"name": "RISE Dispensary Branford", "url": "https://risecannabis.com/dispensaries/connecticut/branford"},
    {"name": "Zen Leaf Naugatuck", "url": "https://zenleafdispensaries.com/location/naugatuck-ct"},
    {"name": "Shangri-La Waterbury", "url": "https://www.shangriladispensary.com/waterbury-menu"},
    {"name": "Zen Leaf Meriden", "url": "https://zenleafdispensaries.com/location/meriden-ct"},
    {"name": "Curaleaf Stamford", "url": "https://curaleaf.com/locations/connecticut/curaleaf-ct-stamford"},
    {"name": "Sweetspot Stamford", "url": "https://sweetspotfarms.com/stamford-menu"},
    {"name": "Fine Fettle Newington", "url": "https://finefettle.com/locations/newington"},
]

SCHEMA = {
    "type": "object",
    "properties": {
        "products": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "brand": {"type": "string"},
                    "category": {"type": "string"},
                    "price": {"type": "number"},
                    "weight": {"type": "string"},
                },
                "required": ["name", "price"],
            },
        },
        "deals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "discount_type": {"type": "string"},
                },
            },
        },
    },
    "required": ["products"],
}

PROMPT = (
    "Extract ALL cannabis products from this dispensary menu. "
    "For each: name, brand, category (Flower/Pre-Rolls/Vaporizers/Edibles/Concentrates/Tinctures), "
    "price in dollars, weight/size. Also get any deals or specials."
)

HEADERS = {}


def init_headers():
    global HEADERS
    HEADERS = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }


def try_extract(name, url):
    """Primary method: use /v2/extract endpoint."""
    payload = {
        "urls": [url],
        "prompt": PROMPT,
        "schema": SCHEMA,
    }
    try:
        resp = requests.post(EXTRACT_URL, json=payload, headers=HEADERS, timeout=120)
        if resp.status_code != 200:
            try:
                err = resp.json()
                msg = err.get("error", str(err))[:200]
            except Exception:
                msg = resp.text[:200]
            print(f"    extract failed: HTTP {resp.status_code} -> {msg}")
            return None

        data = resp.json()
        if not data.get("success"):
            # Extract is async - check if we got a job ID
            job_id = data.get("id")
            if job_id:
                return poll_extract_job(name, job_id)
            print(f"    extract failed: {data.get('error', 'unknown')}")
            return None

        result = data.get("data", {})
        products = result.get("products", [])
        deals = result.get("deals", [])
        products = [p for p in products if p.get("name") and p.get("price") and p["price"] > 0]
        return {"products": products, "deals": deals or []}

    except Exception as e:
        print(f"    extract error: {e}")
        return None


def poll_extract_job(name, job_id):
    """Poll an async extract job until complete."""
    poll_url = f"{EXTRACT_URL}/{job_id}"
    for attempt in range(20):
        time.sleep(5)
        try:
            resp = requests.get(poll_url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                continue
            data = resp.json()
            status = data.get("status", "")
            if status == "completed":
                result = data.get("data", {})
                products = result.get("products", [])
                deals = result.get("deals", [])
                products = [p for p in products if p.get("name") and p.get("price") and p["price"] > 0]
                return {"products": products, "deals": deals or []}
            elif status == "failed":
                print(f"    extract job failed")
                return None
        except Exception:
            continue
    print(f"    extract job timed out")
    return None


def try_scrape_markdown(name, url):
    """Fallback: scrape page as markdown, parse for products."""
    payload = {"url": url}
    try:
        resp = requests.post(SCRAPE_URL, json=payload, headers=HEADERS, timeout=90)
        if resp.status_code != 200:
            print(f"    markdown failed: HTTP {resp.status_code}")
            return None
        data = resp.json()
        if not data.get("success"):
            print(f"    markdown failed: {data.get('error','unknown')[:100]}")
            return None
        md = data.get("data", {}).get("markdown", "")
        if not md:
            print(f"    no markdown content")
            return None
        products = parse_markdown(md)
        return {"products": products, "deals": []}
    except Exception as e:
        print(f"    markdown error: {e}")
        return None


def parse_markdown(md):
    """Parse product names and prices from markdown text."""
    products = []
    lines = md.split("\n")
    price_pat = re.compile(r'\$(\d+\.?\d*)')
    current_cat = "Other"

    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        lower = line.lower()
        for cat, label in [("flower","Flower"),("pre-roll","Pre-Rolls"),("preroll","Pre-Rolls"),
                           ("vaporizer","Vaporizers"),("vape","Vaporizers"),("cartridge","Vaporizers"),
                           ("edible","Edibles"),("gummies","Edibles"),("chocolate","Edibles"),
                           ("concentrate","Concentrates"),("wax","Concentrates"),("shatter","Concentrates"),
                           ("tincture","Tinctures")]:
            if cat in lower and len(line) < 50:
                current_cat = label
                break

        prices = price_pat.findall(line)
        if not prices:
            continue

        price = float(prices[0])
        if price < 5 or price > 500:
            continue

        name_line = re.sub(r'\$[\d.]+', '', line).strip()
        name_line = re.sub(r'[#*_\[\]()>|]', '', name_line).strip()
        name_line = re.sub(r'\s+', ' ', name_line).strip()

        if len(name_line) < 3 and i > 0:
            name_line = re.sub(r'[#*_\[\]()>|]', '', lines[i-1]).strip()

        if not name_line or len(name_line) < 3:
            continue

        parts = re.split(r'\s*[-–—|]\s*', name_line, 1)
        brand = parts[0].strip()[:40] if len(parts) > 1 else "Unknown"
        pname = parts[1].strip()[:80] if len(parts) > 1 else name_line[:80]

        products.append({
            "name": pname,
            "brand": brand,
            "category": current_cat,
            "price": price,
            "weight": "",
        })

    return products


def scrape_dispensary(name, url):
    """Try extract first, fall back to markdown scrape."""
    print(f"  -> {name}")

    # Try AI extract
    result = try_extract(name, url)
    if result and len(result["products"]) > 0:
        print(f"     OK: {len(result['products'])} products (extract)")
        return result

    # Fallback to markdown
    print(f"     trying markdown fallback...")
    result = try_scrape_markdown(name, url)
    if result and len(result["products"]) > 0:
        print(f"     OK: {len(result['products'])} products (markdown)")
        return result

    print(f"     FAILED: 0 products")
    return None


def build_dashboard(results):
    product_map = {}
    for disp, data in results.items():
        if not data:
            continue
        for p in data.get("products", []):
            name = (p.get("name") or "").strip()
            brand = (p.get("brand") or "Unknown").strip()
            cat = (p.get("category") or "Other").strip()
            price = p.get("price")
            wt = (p.get("weight") or "").strip()
            if not name or not price or price <= 0:
                continue
            key = f"{brand}::{name}".lower()
            if key not in product_map:
                product_map[key] = {"name": name, "brand": brand, "category": cat, "weight": wt, "dispensaries": {}}
            product_map[key]["dispensaries"][disp] = round(float(price), 2)

    comparable = sorted([v for v in product_map.values() if len(v["dispensaries"]) >= 2], key=lambda x: (x["category"], x["name"]))
    all_prods = sorted(product_map.values(), key=lambda x: (x["category"], x["name"]))
    output = comparable if len(comparable) >= 5 else all_prods

    deals = []
    for disp, data in results.items():
        if not data:
            continue
        for d in data.get("deals", []):
            if d.get("title"):
                deals.append({"dispensary": disp, "title": d["title"], "type": d.get("discount_type", "other"), "category": d.get("category", "All"), "expires": None})

    return {
        "scraped_at": datetime.now().isoformat(),
        "products": output,
        "deals": deals,
        "stats": {
            "total_products": sum(len(d["products"]) for d in results.values() if d),
            "comparable": len(comparable),
            "all_products": len(all_prods),
            "success": len([v for v in results.values() if v]),
            "failed": len([v for v in results.values() if not v]),
            "deals": len(deals),
        },
    }


def main():
    if not API_KEY:
        print("ERROR: FIRECRAWL_API_KEY not set")
        sys.exit(1)

    init_headers()
    stores = AFFINITY_STORES + COMPETITORS
    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER (Firecrawl v3)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(COMPETITORS)} competitors + {len(AFFINITY_STORES)} own stores")
    print(f"{'='*60}\n")

    results = {}
    for s in stores:
        results[s["name"]] = scrape_dispensary(s["name"], s["url"])
        time.sleep(3)

    dash = build_dashboard(results)
    print(f"\n{'='*60}")
    for k, v in dash["stats"].items():
        print(f"  {k}: {v}")
    print(f"{'='*60}\n")

    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"data/raw_{ts}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out, "w") as f:
        json.dump(dash, f, indent=2, default=str)
    with open(f"data/dashboard_{ts}.json", "w") as f:
        json.dump(dash, f, indent=2, default=str)
    print("  DONE\n")


if __name__ == "__main__":
    main()
