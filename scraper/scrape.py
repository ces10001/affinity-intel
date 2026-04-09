#!/usr/bin/env python3
"""
AFFINITY SCRAPER v9 — Hoodie Analytics API
Pulls real POS-backed pricing data for all CT dispensaries directly from Hoodie's API.
No web scraping needed — uses the same data source Hoodie's dashboard uses.
"""

import json, os, sys, time, requests
from datetime import datetime, date

# ─── CONFIG ──────────────────────────────────────────────────
AUTH0_DOMAIN = "dev-cfqdc946.us.auth0.com"
AUTH0_CLIENT_ID = "3lL2GMZKQYHw0en00bS4okH5wf02nRDu"
HOODIE_API = "https://app.hoodieanalytics.com/api"
REFRESH_TOKEN = os.environ.get("HOODIE_REFRESH_TOKEN", "")

# Target cities covering our 15 closest competitors + Affinity
TARGET_CITIES = [
    "New Haven", "Bridgeport", "Orange", "Branford", "Hamden",
    "Derby", "Stratford", "Naugatuck", "Meriden", "Stamford",
    "Newington", "Waterbury", "Seymour",
]

# Dispensaries we track (exact names as they appear in Hoodie)
AFFINITY_NAMES = [
    "Affinity Dispensary - New Haven",
    "Affinity Dispensary - Bridgeport",
]

COMPETITOR_NAMES = [
    "Higher Collective - Bridgeport",
    "Lit - New Haven",
    "INSA - New Haven - Sargent Dr",
    "RISE - Orange",
    "RISE - Branford",
    "High Profile - Hamden",
    "Hi! People - Derby",
    "Budr Cannabis - Stratford",
    "Zen Leaf - Naugatuck",
    "Zen Leaf - Meriden",
    "Curaleaf - Stamford",
    "Sweetspot - Stamford",
    "Fine Fettle - Newington",
    "Shangri-La - Waterbury",
    "Rejoice - Seymour",
]


def get_auth_token():
    """Use refresh token to get a fresh id_token from Auth0."""
    print("  Authenticating with Hoodie...")
    resp = requests.post(f"https://{AUTH0_DOMAIN}/oauth/token", json={
        "grant_type": "refresh_token",
        "client_id": AUTH0_CLIENT_ID,
        "refresh_token": REFRESH_TOKEN,
    }, timeout=30)

    if resp.status_code != 200:
        print(f"  Auth failed: {resp.status_code} — {resp.text[:200]}")
        return None

    data = resp.json()
    token = data.get("id_token")
    if not token:
        print(f"  No id_token in response. Keys: {list(data.keys())}")
        return None

    print(f"  Authenticated OK")
    return token


def fetch_dispensary_skus(token, city, page=0, size=50):
    """Fetch one page of SKUs from Hoodie's OpenSearch API."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }

    payload = {
        "variables": {
            "date": date.today().isoformat(),
            "sort": [{"field": "UNITS_7_ROLLING", "order": "desc"}],
            "search": "",
            "size": size,
            "from": page * size,
        },
        "filterset": {
            "filterBy": {
                "states": ["Connecticut"],
                "cities": [city],
                "dispensaries": [],
                "brands": [],
                "categories": [],
            }
        }
    }

    resp = requests.post(
        f"{HOODIE_API}/openSearch.getDispensarySKUs",
        json=payload, headers=headers, timeout=60,
    )

    if resp.status_code != 200:
        print(f"    HTTP {resp.status_code}")
        return None

    data = resp.json()
    result = data.get("result", {}).get("data", {})
    return {
        "items": result.get("page", []),
        "total": result.get("totalSKUs", 0),
    }


def fetch_all_city_skus(token, city, max_pages=20):
    """Fetch all SKUs for a city, paginating as needed."""
    all_items = []
    for page in range(max_pages):
        result = fetch_dispensary_skus(token, city, page=page)
        if not result or not result["items"]:
            break
        all_items.extend(result["items"])
        total = result["total"]
        print(f"    {city}: page {page+1}, got {len(all_items)}/{total}")
        if len(all_items) >= total:
            break
        time.sleep(0.5)  # Be nice to the API
    return all_items


def build_dashboard(all_items):
    """Build dashboard JSON from Hoodie data."""
    # Group products by dispensary
    by_dispensary = {}
    for item in all_items:
        disp = item.get("DISPENSARY_NAME", "Unknown")
        if disp not in by_dispensary:
            by_dispensary[disp] = []
        by_dispensary[disp].append({
            "name": item.get("NAME", ""),
            "brand": item.get("BRAND", "Unknown"),
            "category": item.get("CATEGORY", "Other"),
            "price": item.get("ACTUAL_PRICE"),
            "original_price": item.get("ORIGINAL_PRICE"),
            "discounted_price": item.get("DISCOUNTED_PRICE"),
            "cannabis_type": item.get("CANNABIS_TYPE", ""),
            "pack_size": item.get("PACK_SIZE", ""),
            "is_promo": item.get("IS_ON_PROMOTION", False),
            "units_7d": item.get("UNITS_7_ROLLING"),
            "sales_7d": item.get("SALES_7_ROLLING"),
        })

    # Build cross-dispensary product comparison
    product_map = {}
    for disp, products in by_dispensary.items():
        for p in products:
            name = (p.get("name") or "").strip()
            brand = (p.get("brand") or "Unknown").strip()
            cat = (p.get("category") or "Other").strip()
            price = p.get("price")
            if not name or not price or price <= 0:
                continue

            # Normalize key for matching across dispensaries
            key = f"{name}".lower()
            if key not in product_map:
                product_map[key] = {
                    "name": name,
                    "brand": brand,
                    "category": cat,
                    "weight": p.get("pack_size") or "",
                    "dispensaries": {},
                }
            product_map[key]["dispensaries"][disp] = round(float(price), 2)

    # Separate comparable vs all
    comparable = sorted(
        [v for v in product_map.values() if len(v["dispensaries"]) >= 2],
        key=lambda x: (x["category"], x["name"])
    )
    all_products = sorted(product_map.values(), key=lambda x: (x["category"], x["name"]))
    output = comparable if len(comparable) >= 10 else all_products

    # Extract deals (items on promotion)
    deals = []
    for item in all_items:
        if item.get("IS_ON_PROMOTION") and item.get("PROMO_NAME"):
            deals.append({
                "dispensary": item.get("DISPENSARY_NAME", ""),
                "title": item.get("PROMO_NAME", ""),
                "product": item.get("NAME", ""),
                "original_price": item.get("ORIGINAL_PRICE"),
                "discounted_price": item.get("DISCOUNTED_PRICE"),
                "type": "percent",
                "category": item.get("CATEGORY", "All"),
                "expires": None,
            })

    # Deduplicate deals
    seen_deals = set()
    unique_deals = []
    for d in deals:
        key = f"{d['dispensary']}:{d['title']}"
        if key not in seen_deals:
            seen_deals.add(key)
            unique_deals.append(d)

    return {
        "scraped_at": datetime.now().isoformat(),
        "source": "hoodie_analytics",
        "products": output,
        "deals": unique_deals[:50],
        "stats": {
            "total_skus": len(all_items),
            "dispensaries": len(by_dispensary),
            "comparable": len(comparable),
            "all_products": len(all_products),
            "deals": len(unique_deals),
            "dispensary_counts": {k: len(v) for k, v in sorted(by_dispensary.items(), key=lambda x: -len(x[1]))},
        },
    }


def main():
    if not REFRESH_TOKEN:
        print("ERROR: HOODIE_REFRESH_TOKEN not set")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER v9 (Hoodie Analytics API)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(TARGET_CITIES)} cities to scrape")
    print(f"{'='*60}\n")

    # Step 1: Authenticate
    token = get_auth_token()
    if not token:
        print("  FATAL: Could not authenticate")
        sys.exit(1)

    # Step 2: Fetch data for each target city
    all_items = []
    for city in TARGET_CITIES:
        print(f"\n  -> {city}")
        items = fetch_all_city_skus(token, city)
        all_items.extend(items)
        print(f"    Total: {len(items)} SKUs")

    print(f"\n{'='*60}")
    print(f"  Total SKUs collected: {len(all_items)}")

    # Step 3: Build dashboard
    dashboard = build_dashboard(all_items)

    print(f"  Dispensaries found: {dashboard['stats']['dispensaries']}")
    print(f"  Comparable products: {dashboard['stats']['comparable']}")
    print(f"  Deals: {dashboard['stats']['deals']}")
    print(f"\n  Dispensary breakdown:")
    for disp, count in list(dashboard['stats']['dispensary_counts'].items())[:20]:
        marker = " <-- YOU" if "Affinity" in disp else ""
        print(f"    {disp}: {count} SKUs{marker}")
    print(f"{'='*60}\n")

    # Step 4: Save
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    with open(f"data/raw_{ts}.json", "w") as f:
        json.dump({"items": all_items[:100], "total": len(all_items)}, f, indent=2, default=str)

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)

    print(f"  Dashboard saved to dashboard_data.json")
    print(f"  DONE\n")


if __name__ == "__main__":
    main()
