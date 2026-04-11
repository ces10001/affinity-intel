#!/usr/bin/env python3
"""
AFFINITY COMPETITIVE INTEL — FINAL SCRAPER
Pulls live, active-only products from ALL CT dispensaries via Hoodie Analytics API.
Runs on GitHub Actions, outputs dashboard_data.json for the frontend.
"""

import json, os, sys, time, requests
from datetime import datetime, date

# ── CONFIG ───────────────────────────────────────────────────
AUTH0_DOMAIN    = "dev-cfqdc946.us.auth0.com"
AUTH0_CLIENT_ID = "3lL2GMZKQYHw0en00bS4okH5wf02nRDu"
HOODIE_API      = "https://app.hoodieanalytics.com/api"
REFRESH_TOKEN   = os.environ.get("HOODIE_REFRESH_TOKEN", "")

CT_CITIES = [
    "New Haven", "Bridgeport", "Orange", "Branford", "Hamden",
    "Derby", "Stratford", "Naugatuck", "Meriden", "Stamford",
    "Newington", "Waterbury", "Seymour", "Hartford", "Groton",
    "Milford", "Bristol", "Manchester", "Willimantic", "Norwalk",
    "Old Saybrook", "West Hartford", "Canton",
]

PAGE_SIZE = 50
MAX_PAGES_PER_CITY = 80  # 80 * 50 = 4000 max per city


# ── AUTH ─────────────────────────────────────────────────────
def authenticate():
    """Get id_token via Auth0 refresh token. Saves rotated token if returned."""
    if not REFRESH_TOKEN:
        print("  FATAL: HOODIE_REFRESH_TOKEN env var not set")
        sys.exit(1)

    print("  Authenticating with Hoodie Analytics...")
    try:
        resp = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/token",
            json={
                "grant_type": "refresh_token",
                "client_id": AUTH0_CLIENT_ID,
                "refresh_token": REFRESH_TOKEN,
            },
            timeout=30,
        )
    except requests.RequestException as e:
        print(f"  Auth request failed: {e}")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"  Auth failed ({resp.status_code}): {resp.text[:300]}")
        print("\n  >>> REFRESH TOKEN EXPIRED. To fix:")
        print("  >>> 1. Log into app.hoodieanalytics.com")
        print("  >>> 2. Open DevTools Console (Cmd+Option+I)")
        print("  >>> 3. Paste this command:")
        print("  >>>    JSON.parse(localStorage.getItem('@@auth0spajs@@::3lL2GMZKQYHw0en00bS4okH5wf02nRDu::default::openid profile email offline_access')).body.refresh_token")
        print("  >>> 4. Copy the output and update HOODIE_REFRESH_TOKEN in GitHub Secrets")
        sys.exit(1)

    data = resp.json()
    id_token = data.get("id_token")
    if not id_token:
        print(f"  No id_token returned. Keys: {list(data.keys())}")
        sys.exit(1)

    # Auth0 rotates refresh tokens — save new one
    new_rt = data.get("refresh_token")
    if new_rt and new_rt != REFRESH_TOKEN:
        os.makedirs("data", exist_ok=True)
        with open("data/new_refresh_token.txt", "w") as f:
            f.write(new_rt)
        print("  !! Token rotated — new token saved to data/new_refresh_token.txt")
        print("  !! UPDATE HOODIE_REFRESH_TOKEN in GitHub Secrets with this value!")

    print("  Auth OK")
    return id_token


# ── DATA FETCHING ────────────────────────────────────────────
def fetch_city(token, city):
    """Fetch all active products from one city. Returns list of raw items."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    active_items = []
    for page in range(MAX_PAGES_PER_CITY):
        payload = {
            "variables": {
                "date": date.today().isoformat(),
                "sort": [{"field": "UNITS_7_ROLLING", "order": "desc"}],
                "search": "",
                "size": PAGE_SIZE,
                "from": page * PAGE_SIZE,
            },
            "filterset": {
                "filterBy": {
                    "states": ["Connecticut"],
                    "cities": [city],
                    "dispensaries": [],
                    "brands": [],
                    "categories": [],
                }
            },
        }
        try:
            resp = requests.post(
                f"{HOODIE_API}/openSearch.getDispensarySKUs",
                json=payload,
                headers=headers,
                timeout=60,
            )
        except requests.RequestException as e:
            print(f"      Network error on page {page+1}: {e}")
            break

        if resp.status_code != 200:
            print(f"      HTTP {resp.status_code} on page {page+1}")
            break

        try:
            result = resp.json().get("result", {}).get("data", {})
        except (ValueError, KeyError):
            print(f"      Invalid JSON on page {page+1}")
            break

        items = result.get("page", [])
        total = result.get("totalSKUs", 0)
        if not items:
            break

        # Filter: active only
        active = [i for i in items if i.get("IS_ACTIVE") is True]
        active_items.extend(active)

        # Progress (every 5 pages)
        if (page + 1) % 5 == 0 or (page + 1) * PAGE_SIZE >= total:
            print(f"      pg {page+1}: {len(active)}/{len(items)} active, total {len(active_items)}")

        # Done if we've fetched all pages
        if (page + 1) * PAGE_SIZE >= total:
            break

        time.sleep(0.25)  # Rate limiting

    return active_items


# ── DATA PROCESSING ──────────────────────────────────────────
def build_dashboard(all_items):
    """Transform raw Hoodie items into dashboard JSON."""
    # Group by dispensary
    by_dispensary = {}
    for item in all_items:
        dn = item.get("DISPENSARY_NAME", "Unknown")
        if dn not in by_dispensary:
            by_dispensary[dn] = {
                "city": item.get("CITY", ""),
                "lat": item.get("DISPENSARY_LOCATION", {}).get("lat") if isinstance(item.get("DISPENSARY_LOCATION"), dict) else None,
                "lon": item.get("DISPENSARY_LOCATION", {}).get("lon") if isinstance(item.get("DISPENSARY_LOCATION"), dict) else None,
                "items": [],
            }
        by_dispensary[dn]["items"].append(item)

    # Build product comparison map
    product_map = {}
    for dn, info in by_dispensary.items():
        for item in info["items"]:
            name = (item.get("NAME") or "").strip()
            price = item.get("ACTUAL_PRICE")
            if not name or not price or price <= 0:
                continue

            key = name.lower()
            if key not in product_map:
                product_map[key] = {
                    "name": name,
                    "brand": item.get("BRAND", "Unknown"),
                    "category": item.get("CATEGORY", "Other"),
                    "cannabis_type": item.get("CANNABIS_TYPE", ""),
                    "dispensaries": {},
                }
            product_map[key]["dispensaries"][dn] = round(float(price), 2)

    # Split comparable vs all
    comparable = sorted(
        [v for v in product_map.values() if len(v["dispensaries"]) >= 2],
        key=lambda x: (x["category"], x["name"]),
    )
    all_products = sorted(
        product_map.values(),
        key=lambda x: (x["category"], x["name"]),
    )

    # Dispensary metadata for the map
    dispensaries_meta = {}
    for dn, info in by_dispensary.items():
        dispensaries_meta[dn] = {
            "city": info["city"],
            "lat": info.get("lat"),
            "lon": info.get("lon"),
            "product_count": len(info["items"]),
            "is_affinity": "affinity" in dn.lower(),
        }

    # Deals (items on promotion with actual discounts)
    deals = []
    seen = set()
    for item in all_items:
        if item.get("IS_ON_PROMOTION") and item.get("ORIGINAL_PRICE") and item.get("DISCOUNTED_PRICE"):
            orig = item["ORIGINAL_PRICE"]
            disc = item["DISCOUNTED_PRICE"]
            if orig > 0 and disc > 0 and orig > disc:
                pct = round((1 - disc / orig) * 100)
                key = f"{item.get('DISPENSARY_NAME')}:{item.get('NAME')}"
                if key not in seen:
                    seen.add(key)
                    deals.append({
                        "dispensary": item.get("DISPENSARY_NAME", ""),
                        "product": item.get("NAME", ""),
                        "brand": item.get("BRAND", ""),
                        "category": item.get("CATEGORY", ""),
                        "original": round(orig, 2),
                        "discounted": round(disc, 2),
                        "pct_off": pct,
                    })

    deals.sort(key=lambda x: -x["pct_off"])

    return {
        "scraped_at": datetime.now().isoformat(),
        "source": "hoodie_analytics_live",
        "products": comparable if len(comparable) >= 20 else all_products,
        "deals": deals[:200],
        "dispensaries": dispensaries_meta,
        "stats": {
            "total_active": len(all_items),
            "dispensary_count": len(by_dispensary),
            "comparable": len(comparable),
            "unique_products": len(all_products),
            "deals": len(deals),
            "dispensary_counts": {
                k: len(v["items"])
                for k, v in sorted(by_dispensary.items(), key=lambda x: -len(x[1]["items"]))
            },
        },
    }


# ── MAIN ─────────────────────────────────────────────────────
def main():
    print(f"\n{'='*64}")
    print(f"  AFFINITY COMPETITIVE INTEL — FINAL SCRAPER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {len(CT_CITIES)} CT cities | Active products only")
    print(f"{'='*64}\n")

    token = authenticate()

    all_items = []
    city_stats = {}

    for i, city in enumerate(CT_CITIES):
        print(f"\n  [{i+1}/{len(CT_CITIES)}] {city}")
        items = fetch_city(token, city)
        all_items.extend(items)
        city_stats[city] = len(items)
        print(f"    → {len(items)} active products")

    # Build dashboard
    dashboard = build_dashboard(all_items)
    s = dashboard["stats"]

    print(f"\n{'='*64}")
    print(f"  RESULTS")
    print(f"  Active products: {s['total_active']:,}")
    print(f"  Dispensaries:    {s['dispensary_count']}")
    print(f"  Comparable:      {s['comparable']:,}")
    print(f"  Unique products: {s['unique_products']:,}")
    print(f"  Deals:           {s['deals']}")
    print(f"\n  Top dispensaries:")
    for dn, count in list(s["dispensary_counts"].items())[:25]:
        flag = " ◀ YOU" if "affinity" in dn.lower() else ""
        print(f"    {dn}: {count}{flag}")
    print(f"{'='*64}\n")

    # Save
    os.makedirs("data", exist_ok=True)
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out_path, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    print(f"  ✓ Saved dashboard_data.json")
    print(f"  DONE\n")


if __name__ == "__main__":
    main()
