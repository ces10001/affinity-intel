#!/usr/bin/env python3
"""
ATHENA — Affinity Competitive Intelligence Scraper
Pulls ALL active products from EVERY CT dispensary via Hoodie Analytics API.
Bulletproof: queries every CT city, paginates fully, filters active-only.
"""

import json, os, sys, time, requests
from datetime import datetime, date

AUTH0_DOMAIN    = "dev-cfqdc946.us.auth0.com"
AUTH0_CLIENT_ID = "3lL2GMZKQYHw0en00bS4okH5wf02nRDu"
HOODIE_API      = "https://app.hoodieanalytics.com/api"
REFRESH_TOKEN   = os.environ.get("HOODIE_REFRESH_TOKEN", "")

# EVERY town in CT that could have a dispensary — comprehensive coverage
CT_CITIES = [
    "Bethel","Branford","Bridgeport","Bristol","Canton","Clinton",
    "Colchester","Danbury","Derby","East Hartford","East Haven",
    "Enfield","Fairfield","Glastonbury","Greenwich","Groton",
    "Hamden","Hartford","Killingly","Ledyard","Manchester",
    "Meriden","Middletown","Milford","Monroe","Montville",
    "Naugatuck","New Britain","New Haven","New London","Newington",
    "North Haven","Norwalk","Norwich","Old Saybrook","Orange",
    "Plainville","Plymouth","Portland","Seymour","Shelton",
    "Simsbury","South Norwalk","Southington","Stamford","Stratford",
    "Torrington","Vernon","Wallingford","Waterbury","West Hartford",
    "West Haven","Westport","Willimantic","Windham","Windsor",
]

PAGE_SIZE = 50
MAX_PAGES = 200  # 200 * 50 = 10,000 per city (API hard limit)


def authenticate():
    if not REFRESH_TOKEN:
        print("  FATAL: HOODIE_REFRESH_TOKEN not set"); sys.exit(1)
    print("  Authenticating...")
    try:
        resp = requests.post(f"https://{AUTH0_DOMAIN}/oauth/token", json={
            "grant_type": "refresh_token",
            "client_id": AUTH0_CLIENT_ID,
            "refresh_token": REFRESH_TOKEN,
        }, timeout=30)
    except Exception as e:
        print(f"  Network error: {e}"); sys.exit(1)

    if resp.status_code != 200:
        print(f"  Auth failed ({resp.status_code})")
        print("  >>> Token expired. Get a new one from Hoodie Analytics:")
        print("  >>> DevTools Console → paste:")
        print("  >>> JSON.parse(localStorage.getItem('@@auth0spajs@@::3lL2GMZKQYHw0en00bS4okH5wf02nRDu::default::openid profile email offline_access')).body.refresh_token")
        print("  >>> Then update HOODIE_REFRESH_TOKEN in GitHub Secrets")
        sys.exit(1)

    data = resp.json()
    new_rt = data.get("refresh_token")
    if new_rt and new_rt != REFRESH_TOKEN:
        os.makedirs("data", exist_ok=True)
        with open("data/new_refresh_token.txt", "w") as f:
            f.write(new_rt)
        print("  !! Token rotated — saved to data/new_refresh_token.txt")
        print("  !! UPDATE HOODIE_REFRESH_TOKEN in GitHub Secrets!")

    token = data.get("id_token")
    if not token:
        print("  No id_token returned"); sys.exit(1)
    print(f"  Auth OK (token length: {len(token)})")

    # Step 2: Authorize with Hoodie app (establishes session)
    print("  Authorizing with Hoodie app...")
    try:
        auth_resp = requests.get(
            f"{HOODIE_API}/account.authorize",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if auth_resp.status_code == 200:
            print("  Hoodie session OK")
        else:
            print(f"  Hoodie session warning: {auth_resp.status_code} (continuing anyway)")
    except Exception as e:
        print(f"  Hoodie session error: {e} (continuing anyway)")

    return token


def fetch_city(token, city, debug=False):
    """Fetch ALL products from a city, paginate fully, filter active client-side."""
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    active = []
    total_seen = 0

    for page in range(MAX_PAGES):
        try:
            resp = requests.post(f"{HOODIE_API}/openSearch.getDispensarySKUs", json={
                "variables": {
                    "date": date.today().isoformat(),
                    "sort": [{"field": "UNITS_7_ROLLING", "order": "desc"}],
                    "search": "", "size": PAGE_SIZE, "from": page * PAGE_SIZE,
                },
                "filterset": {
                    "filterBy": {
                        "states": ["Connecticut"], "cities": [city],
                        "dispensaries": [], "brands": [], "categories": [],
                    }
                },
            }, headers=headers, timeout=60)
        except Exception as e:
            print(f"      Error pg {page+1}: {e}"); break

        if debug and page == 0:
            print(f"      DEBUG {city}: HTTP {resp.status_code}")
            print(f"      DEBUG response: {resp.text[:500]}")

        if resp.status_code != 200:
            if page == 0:
                print(f"      {city}: HTTP {resp.status_code} — {resp.text[:200]}")
            break

        try:
            body = resp.json()
            result = body.get("result", {}).get("data", {})
        except Exception as e:
            if page == 0:
                print(f"      {city}: JSON parse error: {e}")
            break

        items = result.get("page", [])
        total = result.get("totalSKUs", 0)
        if not items:
            break

        batch_active = [i for i in items if i.get("IS_ACTIVE") is True]
        active.extend(batch_active)
        total_seen += len(items)

        if (page + 1) * PAGE_SIZE >= total:
            break
        time.sleep(0.15)

    return active, total_seen


def build_dashboard(all_items):
    by_disp = {}
    for item in all_items:
        dn = item.get("DISPENSARY_NAME", "Unknown")
        if dn not in by_disp:
            by_disp[dn] = {"city": item.get("CITY", ""), "items": []}
        by_disp[dn]["items"].append(item)

    product_map = {}
    for dn, info in by_disp.items():
        for item in info["items"]:
            name = (item.get("NAME") or "").strip()
            price = item.get("ACTUAL_PRICE")
            if not name or not price or price <= 0:
                continue
            key = name.lower()
            if key not in product_map:
                product_map[key] = {
                    "name": name, "brand": item.get("BRAND", "Unknown"),
                    "category": item.get("CATEGORY", "Other"),
                    "cannabis_type": item.get("CANNABIS_TYPE", ""),
                    "dispensaries": {},
                }
            product_map[key]["dispensaries"][dn] = round(float(price), 2)

    comparable = sorted(
        [v for v in product_map.values() if len(v["dispensaries"]) >= 2],
        key=lambda x: (x["category"], x["name"]),
    )
    all_prods = sorted(product_map.values(), key=lambda x: (x["category"], x["name"]))

    dispensaries_meta = {}
    for dn, info in by_disp.items():
        dispensaries_meta[dn] = {
            "city": info["city"],
            "product_count": len(info["items"]),
            "is_affinity": "affinity" in dn.lower(),
        }

    deals = []
    seen = set()
    for item in all_items:
        if item.get("IS_ON_PROMOTION") and item.get("ORIGINAL_PRICE") and item.get("DISCOUNTED_PRICE"):
            orig, disc = item["ORIGINAL_PRICE"], item["DISCOUNTED_PRICE"]
            if orig > 0 and disc > 0 and orig > disc:
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
                        "pct_off": round((1 - disc / orig) * 100),
                    })
    deals.sort(key=lambda x: -x["pct_off"])

    return {
        "scraped_at": datetime.now().isoformat(),
        "source": "hoodie_live",
        "products": comparable if len(comparable) >= 20 else all_prods,
        "deals": deals[:300],
        "dispensaries": dispensaries_meta,
        "stats": {
            "total_active": len(all_items),
            "dispensary_count": len(by_disp),
            "comparable": len(comparable),
            "unique_products": len(all_prods),
            "deals": len(deals),
            "dispensary_counts": {
                k: len(v["items"])
                for k, v in sorted(by_disp.items(), key=lambda x: -len(x[1]["items"]))
            },
        },
    }


def main():
    print(f"\n{'='*64}")
    print(f"  ATHENA — Affinity Competitive Intel")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Scanning {len(CT_CITIES)} CT towns for active products")
    print(f"{'='*64}\n")

    token = authenticate()
    all_items = []
    cities_with_data = 0

    for i, city in enumerate(CT_CITIES):
        debug = (i == 0)  # Debug output for first city
        active, total_seen = fetch_city(token, city, debug=debug)
        if active:
            all_items.extend(active)
            cities_with_data += 1
            print(f"  [{i+1}/{len(CT_CITIES)}] {city}: {len(active)} active / {total_seen} total")
        # Skip printing empty cities to keep logs clean

    dashboard = build_dashboard(all_items)
    s = dashboard["stats"]

    print(f"\n{'='*64}")
    print(f"  RESULTS")
    print(f"  Active products:  {s['total_active']:,}")
    print(f"  Dispensaries:     {s['dispensary_count']}")
    print(f"  Comparable:       {s['comparable']:,}")
    print(f"  Unique products:  {s['unique_products']:,}")
    print(f"  Deals:            {s['deals']}")
    print(f"  Cities with data: {cities_with_data}")
    print(f"\n  Dispensary breakdown:")
    for dn, count in list(s["dispensary_counts"].items())[:30]:
        flag = " ◀ YOU" if "affinity" in dn.lower() else ""
        print(f"    {dn}: {count:,}{flag}")
    print(f"{'='*64}\n")

    os.makedirs("data", exist_ok=True)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    print(f"  Saved dashboard_data.json\n  DONE\n")


if __name__ == "__main__":
    main()
