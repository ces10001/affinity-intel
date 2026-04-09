#!/usr/bin/env python3
"""
AFFINITY COMPETITIVE INTEL — Firecrawl Scraper
Scrapes the 15 closest competitor dispensary menus using Firecrawl API.
Firecrawl handles JS rendering, anti-bot, and extracts structured data via AI.

Usage:
    FIRECRAWL_API_KEY=fc-xxx python scrape.py
"""

import json
import os
import sys
import time
import requests
from datetime import datetime

FIRECRAWL_API = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

# ─── YOUR STORES (scraped for price comparison baseline) ──
AFFINITY_STORES = [
    {"name": "Affinity - New Haven", "url": "https://weedmaps.com/dispensaries/affinity-new-haven"},
    {"name": "Affinity - Bridgeport", "url": "https://weedmaps.com/dispensaries/affinity-dispensary-bridgeport"},
]

# ─── 15 CLOSEST COMPETITORS (sorted by distance to either Affinity location) ──
COMPETITORS = [
    {"name": "Higher Collective Bridgeport", "url": "https://weedmaps.com/dispensaries/higher-collective-3", "lat": 41.1547, "lng": -73.2353, "chain": "Higher Collective"},
    {"name": "Lit New Haven Cannabis", "url": "https://weedmaps.com/dispensaries/lit-new-haven-cannabis", "lat": 41.3029, "lng": -72.9103, "chain": "Lit"},
    {"name": "Insa New Haven", "url": "https://dutchie.com/dispensary/insa-new-haven", "lat": 41.2942, "lng": -72.9227, "chain": "Insa"},
    {"name": "RISE Dispensary Orange", "url": "https://dutchie.com/dispensary/rise-orange", "lat": 41.2727, "lng": -72.9951, "chain": "RISE"},
    {"name": "High Profile Hamden", "url": "https://dutchie.com/dispensary/high-profile-hamden", "lat": 41.3912, "lng": -72.8978, "chain": "High Profile"},
    {"name": "Hi! People Derby", "url": "https://weedmaps.com/dispensaries/hi-people-derby", "lat": 41.3276, "lng": -73.0847, "chain": "Hi! People"},
    {"name": "Rejoice Seymour", "url": "https://weedmaps.com/dispensaries/rejoice-dispensary-seymour", "lat": 41.3976, "lng": -73.0619, "chain": "Rejoice"},
    {"name": "Budr Cannabis Stratford", "url": "https://weedmaps.com/dispensaries/budr-cannabis-stratford", "lat": 41.2539, "lng": -73.1011, "chain": "Budr"},
    {"name": "RISE Dispensary Branford", "url": "https://dutchie.com/dispensary/rise-branford", "lat": 41.2979, "lng": -72.773, "chain": "RISE"},
    {"name": "Zen Leaf Naugatuck", "url": "https://weedmaps.com/dispensaries/zen-leaf-cannabis-dispensary-naugatuck", "lat": 41.4786, "lng": -73.0482, "chain": "Zen Leaf"},
    {"name": "Shangri-La Waterbury", "url": "https://weedmaps.com/dispensaries/shangri-la-dispensary-waterbury", "lat": 41.5355, "lng": -72.9978, "chain": "Shangri-La"},
    {"name": "Zen Leaf Meriden", "url": "https://weedmaps.com/dispensaries/zen-leaf-cannabis-dispensary-meriden-2", "lat": 41.5253, "lng": -72.7569, "chain": "Zen Leaf"},
    {"name": "Curaleaf Stamford", "url": "https://dutchie.com/dispensary/curaleaf-ct-stamford", "lat": 41.0559, "lng": -73.5279, "chain": "Curaleaf"},
    {"name": "Sweetspot Stamford", "url": "https://dutchie.com/dispensary/sweetspot-cannabis-dispensary-stamford", "lat": 41.0759, "lng": -73.549, "chain": "Sweetspot"},
    {"name": "Fine Fettle Newington", "url": "https://dutchie.com/dispensary/fine-fettle-newington", "lat": 41.6905, "lng": -72.7054, "chain": "Fine Fettle"},
]

# Schema tells Firecrawl exactly what data structure to extract
EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "dispensary_name": {"type": "string"},
        "products": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Product name"},
                    "brand": {"type": "string", "description": "Brand or grower name"},
                    "category": {"type": "string", "description": "Category: Flower, Pre-Rolls, Vaporizers, Edibles, Concentrates, Tinctures, or Topicals"},
                    "price": {"type": "number", "description": "Price in dollars (lowest/default price shown)"},
                    "weight": {"type": "string", "description": "Weight or size (e.g. 3.5g, 1g, 0.5g, 100mg)"},
                    "strain_type": {"type": "string", "description": "Indica, Sativa, Hybrid, or empty"},
                },
                "required": ["name", "price"],
            },
        },
        "deals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Deal or special name"},
                    "description": {"type": "string"},
                    "discount_type": {"type": "string", "description": "percent, dollar, bogo, or other"},
                    "category": {"type": "string"},
                },
            },
        },
    },
    "required": ["products"],
}

EXTRACT_PROMPT = (
    "Extract ALL cannabis products visible on this dispensary menu page. "
    "For each product, get the name, brand, category (Flower, Pre-Rolls, Vaporizers, Edibles, Concentrates, Tinctures, Topicals), "
    "the displayed price in dollars, and weight/size. "
    "Also extract any deals, specials, or promotions currently shown. "
    "Get as many products as you can see on the page."
)


def scrape_dispensary(name, url):
    """Scrape a single dispensary using Firecrawl."""
    print(f"  → Scraping {name}...")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "url": url,
        "formats": ["extract"],
        "extract": {
            "schema": EXTRACT_SCHEMA,
            "prompt": EXTRACT_PROMPT,
        },
        "timeout": 60000,
        "waitFor": 5000,
    }

    try:
        resp = requests.post(FIRECRAWL_API, json=payload, headers=headers, timeout=90)

        if resp.status_code == 402:
            print(f"  ✗ {name}: Out of Firecrawl credits")
            return None
        if resp.status_code == 429:
            print(f"  ✗ {name}: Rate limited — waiting 30s")
            time.sleep(30)
            resp = requests.post(FIRECRAWL_API, json=payload, headers=headers, timeout=90)

        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            print(f"  ✗ {name}: Firecrawl returned success=false")
            return None

        extract = data.get("data", {}).get("extract", {})
        products = extract.get("products", [])
        deals = extract.get("deals", [])

        # Filter out products with no price or no name
        products = [p for p in products if p.get("name") and p.get("price") and p["price"] > 0]

        print(f"  ✓ {name}: {len(products)} products, {len(deals)} deals")
        return {"products": products, "deals": deals}

    except requests.exceptions.Timeout:
        print(f"  ✗ {name}: Timeout")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"  ✗ {name}: HTTP {e.response.status_code}")
        return None
    except Exception as e:
        print(f"  ✗ {name}: {e}")
        return None


def build_dashboard_data(results):
    """Convert all scrape results into dashboard JSON format."""
    product_map = {}

    for comp_name, data in results.items():
        if data is None:
            continue
        for p in data.get("products", []):
            name = p.get("name", "").strip()
            brand = p.get("brand", "Unknown").strip() if p.get("brand") else "Unknown"
            category = p.get("category", "Other").strip() if p.get("category") else "Other"
            price = p.get("price")
            weight = p.get("weight", "").strip() if p.get("weight") else ""

            if not name or not price or price <= 0:
                continue

            key = f"{brand}::{name}".lower()
            if key not in product_map:
                product_map[key] = {
                    "name": name,
                    "brand": brand,
                    "category": category,
                    "weight": weight,
                    "dispensaries": {},
                }
            product_map[key]["dispensaries"][comp_name] = round(float(price), 2)

    # Keep products found at 2+ dispensaries for comparison
    comparable = [v for v in product_map.values() if len(v["dispensaries"]) >= 2]
    # Also keep all products (even single-store) for complete visibility
    all_products = list(product_map.values())

    comparable.sort(key=lambda x: (x["category"], x["name"]))
    all_products.sort(key=lambda x: (x["category"], x["name"]))

    # Collect deals
    all_deals = []
    for comp_name, data in results.items():
        if data is None:
            continue
        for d in data.get("deals", []):
            if d.get("title"):
                all_deals.append({
                    "dispensary": comp_name,
                    "title": d["title"],
                    "type": d.get("discount_type", "other"),
                    "category": d.get("category", "All"),
                    "expires": None,
                })

    return {
        "scraped_at": datetime.now().isoformat(),
        "products": all_products if len(comparable) < 5 else comparable,
        "deals": all_deals,
        "stats": {
            "total_products": sum(
                len(d["products"]) for d in results.values() if d
            ),
            "comparable_products": len(comparable),
            "all_products": len(all_products),
            "dispensaries_scraped": len([k for k, v in results.items() if v]),
            "dispensaries_failed": len([k for k, v in results.items() if v is None]),
            "total_deals": len(all_deals),
        },
    }


def main():
    if not API_KEY:
        print("ERROR: FIRECRAWL_API_KEY environment variable not set.")
        print("Set it as a GitHub secret or export it locally.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  🌿 AFFINITY SCRAPER (Firecrawl)")
    print(f"  📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    all_stores = AFFINITY_STORES + COMPETITORS
    print(f"  📍 {len(COMPETITORS)} competitors + {len(AFFINITY_STORES)} Affinity stores")
    print(f"{'='*60}\n")

    results = {}

    for comp in all_stores:
        data = scrape_dispensary(comp["name"], comp["url"])
        results[comp["name"]] = data
        # Respect rate limits — 3 second pause between requests
        time.sleep(3)

    # Build dashboard data
    dashboard = build_dashboard_data(results)

    print(f"\n{'='*60}")
    print(f"  📦 Products scraped: {dashboard['stats']['total_products']}")
    print(f"  📊 Comparable (2+ stores): {dashboard['stats']['comparable_products']}")
    print(f"  🏪 Dispensaries with data: {dashboard['stats']['dispensaries_scraped']}")
    print(f"  ❌ Dispensaries failed: {dashboard['stats']['dispensaries_failed']}")
    print(f"  🏷  Deals found: {dashboard['stats']['total_deals']}")
    print(f"{'='*60}\n")

    # Save to data/ directory
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = f"data/raw_{ts}.json"
    with open(raw_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"  ✓ Raw data → {raw_path}")

    # Save dashboard JSON to parent directory (for GitHub Pages)
    dash_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(dash_path, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    print(f"  ✓ Dashboard data → dashboard_data.json")

    # Also save a local copy
    with open(f"data/dashboard_{ts}.json", "w") as f:
        json.dump(dashboard, f, indent=2, default=str)

    print(f"\n  ✅ DONE\n")


if __name__ == "__main__":
    main()
