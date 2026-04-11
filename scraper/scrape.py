#!/usr/bin/env python3
"""
AFFINITY SCRAPER v11 — Apify (Live Dutchie + Weedmaps menus)
Scrapes the actual live dispensary websites via Apify actors.
Only returns products currently on the menu — what customers see right now.
"""

import json, os, sys, time, requests
from datetime import datetime

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
APIFY_API = "https://api.apify.com/v2"

# Dutchie actor: tfmcg3/dutchie-dispensary-scraper
DUTCHIE_ACTOR = "tfmcg3~dutchie-dispensary-scraper"
# Weedmaps actor: kinaesthetic_millionaire/weedmaps-dispensaries-products
WEEDMAPS_ACTOR = "kinaesthetic_millionaire~weedmaps-dispensaries-products"

DISPENSARIES = [
    # Affinity
    {"name": "Affinity NH (Rec)", "slug": "affinity-health-and-wellness-rec", "type": "dutchie"},
    {"name": "Affinity NH (Med)", "slug": "affinity-health-and-wellness", "type": "dutchie"},
    {"name": "Affinity BP (Rec)", "slug": "affinity-dispensary-bridgeport", "type": "dutchie"},
    {"name": "Affinity BP (Med)", "slug": "affinity-dispensary-bridgeport-med", "type": "dutchie"},
    # Competitors — Dutchie
    {"name": "INSA New Haven", "slug": "insa-new-haven-sargent-drive", "type": "dutchie"},
    {"name": "RISE Orange", "slug": "rise-dispensaries-orange", "type": "dutchie"},
    {"name": "RISE Branford", "slug": "rise-dispensaries-branford", "type": "dutchie"},
    {"name": "Zen Leaf Meriden", "slug": "zen-leaf-meriden", "type": "dutchie"},
    {"name": "Zen Leaf Naugatuck", "slug": "zen-leaf-naugatuck", "type": "dutchie"},
    {"name": "Fine Fettle Newington", "slug": "fine-fettle-dispensary-newington", "type": "dutchie"},
    {"name": "Curaleaf Stamford", "slug": "curaleaf-ct-stamford", "type": "dutchie"},
    # Competitors — Weedmaps
    {"name": "High Profile Hamden", "url": "https://weedmaps.com/dispensaries/high-profile-hamden", "type": "weedmaps"},
    {"name": "Lit New Haven", "url": "https://weedmaps.com/dispensaries/lit-new-haven-cannabis", "type": "weedmaps"},
    {"name": "Higher Collective Bridgeport", "url": "https://weedmaps.com/dispensaries/higher-collective-bridgeport", "type": "weedmaps"},
    {"name": "Budr Stratford", "url": "https://weedmaps.com/dispensaries/budr-cannabis", "type": "weedmaps"},
]


def run_dutchie_actor(slugs):
    """Run the Dutchie scraper for multiple dispensary slugs."""
    print(f"  Running Dutchie actor for {len(slugs)} dispensaries...")
    urls = [f"https://dutchie.com/dispensary/{s}" for s in slugs]

    resp = requests.post(
        f"{APIFY_API}/acts/{DUTCHIE_ACTOR}/runs?token={APIFY_TOKEN}&waitForFinish=300",
        json={"dispensaryUrls": urls},
        timeout=360,
    )

    if resp.status_code not in (200, 201):
        print(f"  Dutchie actor failed: {resp.status_code} — {resp.text[:300]}")
        return []

    run = resp.json().get("data", {})
    run_id = run.get("id")
    status = run.get("status")
    dataset_id = run.get("defaultDatasetId")

    # If still running, poll
    if status not in ("SUCCEEDED", "FAILED"):
        print(f"  Waiting for Dutchie run {run_id}...")
        for _ in range(60):
            time.sleep(10)
            r = requests.get(f"{APIFY_API}/actor-runs/{run_id}?token={APIFY_TOKEN}", timeout=30)
            status = r.json().get("data", {}).get("status")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
            print(f"    Status: {status}")

    if status != "SUCCEEDED":
        print(f"  Dutchie run ended with: {status}")
        return []

    # Fetch results
    items = []
    offset = 0
    while True:
        r = requests.get(
            f"{APIFY_API}/datasets/{dataset_id}/items?token={APIFY_TOKEN}&offset={offset}&limit=1000",
            timeout=60,
        )
        batch = r.json()
        if not batch:
            break
        items.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000

    print(f"  Dutchie: {len(items)} products scraped")
    return items


def run_weedmaps_actor(urls):
    """Run the Weedmaps scraper for multiple dispensary URLs."""
    print(f"  Running Weedmaps actor for {len(urls)} dispensaries...")

    resp = requests.post(
        f"{APIFY_API}/acts/{WEEDMAPS_ACTOR}/runs?token={APIFY_TOKEN}&waitForFinish=300",
        json={"startUrls": [{"url": u} for u in urls]},
        timeout=360,
    )

    if resp.status_code not in (200, 201):
        print(f"  Weedmaps actor failed: {resp.status_code} — {resp.text[:300]}")
        return []

    run = resp.json().get("data", {})
    run_id = run.get("id")
    status = run.get("status")
    dataset_id = run.get("defaultDatasetId")

    if status not in ("SUCCEEDED", "FAILED"):
        print(f"  Waiting for Weedmaps run {run_id}...")
        for _ in range(60):
            time.sleep(10)
            r = requests.get(f"{APIFY_API}/actor-runs/{run_id}?token={APIFY_TOKEN}", timeout=30)
            status = r.json().get("data", {}).get("status")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
            print(f"    Status: {status}")

    if status != "SUCCEEDED":
        print(f"  Weedmaps run ended with: {status}")
        return []

    items = []
    offset = 0
    while True:
        r = requests.get(
            f"{APIFY_API}/datasets/{dataset_id}/items?token={APIFY_TOKEN}&offset={offset}&limit=1000",
            timeout=60,
        )
        batch = r.json()
        if not batch:
            break
        items.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000

    print(f"  Weedmaps: {len(items)} products scraped")
    return items


def normalize_dutchie(items, slug_to_name):
    """Normalize Dutchie results to common format."""
    products = []
    for item in items:
        # Map slug back to our friendly name
        url = item.get("dispensaryUrl", item.get("url", ""))
        slug = url.rstrip("/").split("/")[-1] if url else ""
        disp_name = slug_to_name.get(slug, item.get("dispensaryName", slug))

        price = item.get("price") or item.get("prices", [{}])[0].get("price") if isinstance(item.get("prices"), list) else item.get("price")
        if not price:
            # Try other price fields
            for key in ["recPrice", "medPrice", "specialPrice"]:
                if item.get(key):
                    price = item[key]
                    break

        products.append({
            "name": item.get("name", item.get("productName", "")),
            "brand": item.get("brand", item.get("brandName", "Unknown")),
            "category": item.get("category", item.get("type", "Other")),
            "price": price,
            "weight": item.get("weight", item.get("size", "")),
            "thc": item.get("thc", item.get("thcContent", "")),
            "cbd": item.get("cbd", item.get("cbdContent", "")),
            "dispensary": disp_name,
            "cannabis_type": item.get("strainType", item.get("cannabisType", "")),
        })
    return products


def normalize_weedmaps(items, url_to_name):
    """Normalize Weedmaps results to common format."""
    products = []
    for item in items:
        url = item.get("dispensaryUrl", item.get("url", ""))
        disp_name = url_to_name.get(url, item.get("dispensaryName", "Unknown"))
        # Try to match by partial URL
        if disp_name == "Unknown":
            for u, n in url_to_name.items():
                if u in url or url in u:
                    disp_name = n
                    break

        products.append({
            "name": item.get("name", item.get("productName", "")),
            "brand": item.get("brand", item.get("brandName", "Unknown")),
            "category": item.get("category", item.get("type", "Other")),
            "price": item.get("price", item.get("Price")),
            "weight": item.get("weight", item.get("size", "")),
            "thc": item.get("thc", ""),
            "cbd": item.get("cbd", ""),
            "dispensary": disp_name,
            "cannabis_type": item.get("strainType", ""),
        })
    return products


def build_dashboard(all_products):
    """Build the dashboard JSON."""
    # Product comparison map
    product_map = {}
    for p in all_products:
        name = (p.get("name") or "").strip()
        price = p.get("price")
        disp = p.get("dispensary", "Unknown")
        if not name or not price:
            continue
        try:
            price = float(price)
        except (ValueError, TypeError):
            continue
        if price <= 0:
            continue

        key = name.lower()
        if key not in product_map:
            product_map[key] = {
                "name": name,
                "brand": p.get("brand", "Unknown"),
                "category": p.get("category", "Other"),
                "weight": p.get("weight", ""),
                "dispensaries": {},
            }
        product_map[key]["dispensaries"][disp] = round(price, 2)

    comparable = sorted(
        [v for v in product_map.values() if len(v["dispensaries"]) >= 2],
        key=lambda x: (x["category"], x["name"])
    )
    all_prods = sorted(product_map.values(), key=lambda x: (x["category"], x["name"]))

    # Count by dispensary
    by_disp = {}
    for p in all_products:
        d = p.get("dispensary", "Unknown")
        if d not in by_disp:
            by_disp[d] = 0
        by_disp[d] += 1

    return {
        "scraped_at": datetime.now().isoformat(),
        "source": "apify_live",
        "products": comparable if len(comparable) >= 10 else all_prods,
        "deals": [],
        "stats": {
            "total_products": len(all_products),
            "dispensaries": len(by_disp),
            "comparable": len(comparable),
            "all_products": len(all_prods),
            "deals": 0,
            "dispensary_counts": dict(sorted(by_disp.items(), key=lambda x: -x[1])),
        },
    }


def main():
    if not APIFY_TOKEN:
        print("ERROR: APIFY_TOKEN not set")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER v11 (Apify — Live Menus)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(DISPENSARIES)} dispensaries")
    print(f"{'='*60}\n")

    # Split by type
    dutchie_disps = [d for d in DISPENSARIES if d["type"] == "dutchie"]
    weedmaps_disps = [d for d in DISPENSARIES if d["type"] == "weedmaps"]

    slug_to_name = {d["slug"]: d["name"] for d in dutchie_disps}
    url_to_name = {d["url"]: d["name"] for d in weedmaps_disps}

    all_products = []

    # Run Dutchie
    if dutchie_disps:
        dutchie_slugs = [d["slug"] for d in dutchie_disps]
        raw = run_dutchie_actor(dutchie_slugs)
        products = normalize_dutchie(raw, slug_to_name)
        all_products.extend(products)
        print(f"  Dutchie normalized: {len(products)}")

    # Run Weedmaps
    if weedmaps_disps:
        weedmaps_urls = [d["url"] for d in weedmaps_disps]
        raw = run_weedmaps_actor(weedmaps_urls)
        products = normalize_weedmaps(raw, url_to_name)
        all_products.extend(products)
        print(f"  Weedmaps normalized: {len(products)}")

    # Build dashboard
    dashboard = build_dashboard(all_products)

    print(f"\n{'='*60}")
    print(f"  Total products: {dashboard['stats']['total_products']}")
    print(f"  Dispensaries: {dashboard['stats']['dispensaries']}")
    print(f"  Comparable: {dashboard['stats']['comparable']}")
    print(f"\n  Breakdown:")
    for disp, count in list(dashboard['stats']['dispensary_counts'].items())[:20]:
        marker = " <-- YOU" if "Affinity" in disp else ""
        print(f"    {disp}: {count}{marker}")
    print(f"{'='*60}\n")

    # Save
    os.makedirs("data", exist_ok=True)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)

    print(f"  Dashboard saved to dashboard_data.json")
    print(f"  DONE\n")


if __name__ == "__main__":
    main()
