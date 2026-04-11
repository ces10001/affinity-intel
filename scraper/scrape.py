#!/usr/bin/env python3
"""
AFFINITY SCRAPER v13 — Apify Only (Web Scraper for Dutchie + Weedmaps actor)
No Hoodie. Scrapes live menus directly.
"""

import json, os, sys, time, requests
from datetime import datetime

APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
APIFY_API = "https://api.apify.com/v2"

# Apify actors
WEB_SCRAPER = "apify~web-scraper"  # Generic JS-rendering scraper for Dutchie
WM_ACTOR = "kinaesthetic_millionaire~weedmaps-dispensaries-products"

# All dispensaries to scrape
DUTCHIE_STORES = [
    {"name": "Affinity NH (Rec)", "url": "https://dutchie.com/dispensary/affinity-health-and-wellness-rec/products/flower"},
    {"name": "Affinity NH (Med)", "url": "https://dutchie.com/dispensary/affinity-health-and-wellness/products/flower"},
    {"name": "Affinity BP (Rec)", "url": "https://dutchie.com/dispensary/affinity-dispensary-bridgeport/products/flower"},
    {"name": "Affinity BP (Med)", "url": "https://dutchie.com/dispensary/affinity-dispensary-bridgeport-med/products/flower"},
    {"name": "INSA New Haven", "url": "https://dutchie.com/dispensary/insa-new-haven-sargent-drive/products/flower"},
    {"name": "RISE Orange", "url": "https://dutchie.com/dispensary/rise-dispensaries-orange/products/flower"},
    {"name": "RISE Branford", "url": "https://dutchie.com/dispensary/rise-dispensaries-branford/products/flower"},
    {"name": "Zen Leaf Meriden", "url": "https://dutchie.com/dispensary/zen-leaf-meriden/products/flower"},
    {"name": "Zen Leaf Naugatuck", "url": "https://dutchie.com/dispensary/zen-leaf-naugatuck/products/flower"},
    {"name": "Fine Fettle Newington", "url": "https://dutchie.com/dispensary/fine-fettle-dispensary-newington/products/flower"},
    {"name": "Curaleaf Stamford", "url": "https://dutchie.com/dispensary/curaleaf-ct-stamford/products/flower"},
]

WEEDMAPS_URLS = [
    "https://weedmaps.com/dispensaries/high-profile-hamden",
    "https://weedmaps.com/dispensaries/lit-new-haven-cannabis",
    "https://weedmaps.com/dispensaries/higher-collective-bridgeport",
    "https://weedmaps.com/dispensaries/budr-cannabis",
]

# Page function that runs inside the browser on each Dutchie page
DUTCHIE_PAGE_FUNCTION = """
async function pageFunction(context) {
    const { page, request } = context;
    
    // Wait for products to load
    await page.waitForSelector('body', { timeout: 15000 });
    await new Promise(r => setTimeout(r, 5000)); // Wait for JS to render
    
    // Extract dispensary name from URL
    const urlParts = request.url.split('/dispensary/');
    const slug = urlParts[1] ? urlParts[1].split('/')[0] : 'unknown';
    
    // Scroll to load all products
    for (let i = 0; i < 10; i++) {
        await page.evaluate(() => window.scrollBy(0, 1000));
        await new Promise(r => setTimeout(r, 500));
    }
    
    // Extract product data from rendered DOM
    const products = await page.evaluate((dispSlug) => {
        const text = document.body.innerText;
        const results = [];
        
        // Parse products from page text
        // Dutchie renders products in blocks with name, brand, type, THC, price
        const lines = text.split('\\n').map(l => l.trim()).filter(l => l);
        
        let current = {};
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            
            // Price pattern
            if (line.match(/^\\$\\d+\\.\\d{2}$/)) {
                if (!current.price) {
                    current.price = parseFloat(line.replace('$', ''));
                } else if (!current.originalPrice) {
                    current.originalPrice = parseFloat(line.replace('$', ''));
                }
            }
            
            // THC pattern
            if (line.match(/^THC:\\s*[\\d.]+%$/)) {
                current.thc = line;
            }
            
            // Type pattern
            if (['Indica', 'Sativa', 'Hybrid', 'Indica-Hybrid', 'Sativa-Hybrid'].includes(line)) {
                current.type = line;
            }
            
            // Weight pattern
            if (line.match(/^(1\\/8|1\\/4|1\\/2|1|3\\.5g|7g|14g|28g|1g|0\\.5g|\\d+\\s*(g|mg|oz|pk|ct))/) || 
                line.match(/^\\d+\\/\\d+\\s*oz$/)) {
                current.weight = line;
            }
            
            // "Add to cart" signals end of a product
            if (line.includes('Add') && line.includes('cart') && current.price) {
                if (Object.keys(current).length >= 2) {
                    // Look back for product name and brand
                    // Name is typically 2-5 lines before the brand
                    for (let j = i - 1; j >= Math.max(0, i - 15); j--) {
                        const prev = lines[j];
                        if (prev.length > 10 && !prev.match(/^\\$/) && !prev.match(/^THC:/) && 
                            !prev.match(/^CBD:/) && !prev.startsWith('Add') && !prev.startsWith('BUY') &&
                            !['Indica','Sativa','Hybrid','Indica-Hybrid','Sativa-Hybrid'].includes(prev) &&
                            !prev.match(/^\\d+\\/\\d+\\s*oz$/) && !prev.match(/^\\d+%\\s*off$/)) {
                            if (!current.brand) {
                                current.brand = prev;
                            } else if (!current.name) {
                                current.name = prev;
                            }
                            if (current.name && current.brand) break;
                        }
                    }
                    
                    if (current.name || current.brand) {
                        current.dispensary = dispSlug;
                        results.push({...current});
                    }
                }
                current = {};
            }
        }
        
        return results;
    }, slug);
    
    return products;
}
"""

def run_apify_actor(actor_id, input_data, label="Actor", max_wait=1500):
    """Run an Apify actor and wait for results."""
    print(f"  Starting {label}...")
    resp = requests.post(
        f"{APIFY_API}/acts/{actor_id}/runs?token={APIFY_TOKEN}",
        json=input_data, timeout=60,
    )
    if resp.status_code not in (200, 201):
        print(f"  {label} start failed: {resp.status_code} — {resp.text[:200]}")
        return []

    run = resp.json().get("data", {})
    run_id = run.get("id")
    dataset_id = run.get("defaultDatasetId")
    status = run.get("status")
    print(f"  {label} run {run_id} started")

    for i in range(max_wait // 10):
        time.sleep(10)
        r = requests.get(f"{APIFY_API}/actor-runs/{run_id}?token={APIFY_TOKEN}", timeout=30)
        status = r.json().get("data", {}).get("status")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
        if i % 6 == 0:
            print(f"    {i*10}s: {status}")

    print(f"  {label} ended: {status}")
    if status != "SUCCEEDED":
        return []

    items = []
    offset = 0
    while True:
        r = requests.get(
            f"{APIFY_API}/datasets/{dataset_id}/items?token={APIFY_TOKEN}&offset={offset}&limit=1000",
            timeout=60)
        batch = r.json()
        if not batch:
            break
        items.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    print(f"  {label}: {len(items)} items")
    return items


def scrape_dutchie():
    """Scrape Dutchie stores using Apify Web Scraper."""
    urls = [{"url": s["url"]} for s in DUTCHIE_STORES]
    slug_to_name = {}
    for s in DUTCHIE_STORES:
        slug = s["url"].split("/dispensary/")[1].split("/")[0]
        slug_to_name[slug] = s["name"]

    input_data = {
        "startUrls": urls,
        "pageFunction": DUTCHIE_PAGE_FUNCTION,
        "proxyConfiguration": {"useApifyProxy": True},
        "maxRequestRetries": 2,
        "maxConcurrency": 3,
        "maxPagesPerCrawl": len(urls) + 5,
    }

    raw = run_apify_actor(WEB_SCRAPER, input_data, "Dutchie Web Scraper", max_wait=600)

    # Normalize — web scraper returns arrays of products per page
    products = []
    for item in raw:
        if isinstance(item, list):
            for p in item:
                name = p.get("name", "")
                brand = p.get("brand", "")
                price = p.get("price")
                slug = p.get("dispensary", "")
                disp = slug_to_name.get(slug, slug.replace("-", " ").title())
                if name and price:
                    products.append({
                        "name": name, "brand": brand,
                        "category": "Flower",  # We're scraping /products/flower
                        "price": float(price), "dispensary": disp,
                        "weight": p.get("weight", ""),
                        "cannabis_type": p.get("type", ""),
                    })
        elif isinstance(item, dict):
            name = item.get("name", "")
            price = item.get("price")
            slug = item.get("dispensary", "")
            disp = slug_to_name.get(slug, slug.replace("-", " ").title())
            if name and price:
                products.append({
                    "name": name, "brand": item.get("brand", ""),
                    "category": "Flower", "price": float(price),
                    "dispensary": disp, "weight": item.get("weight", ""),
                    "cannabis_type": item.get("type", ""),
                })
    return products


def scrape_weedmaps():
    """Scrape Weedmaps stores using dedicated actor."""
    raw = run_apify_actor(
        WM_ACTOR,
        {"startUrls": [{"url": u} for u in WEEDMAPS_URLS]},
        "Weedmaps", max_wait=1500
    )

    products = []
    for item in raw:
        price = item.get("price") or item.get("Price")
        name = item.get("name") or item.get("title") or item.get("productName", "")
        if not name or not price:
            continue
        try:
            price = float(str(price).replace("$", "").replace(",", "").strip())
        except (ValueError, TypeError):
            continue
        if price <= 0:
            continue

        url = item.get("url", "")
        disp = item.get("dispensaryName", "")
        if not disp and url:
            parts = url.split("/dispensaries/")
            if len(parts) > 1:
                slug = parts[1].split("/")[0]
                disp = slug.replace("-", " ").title()

        products.append({
            "name": name, "brand": item.get("brand", item.get("Brand", "Unknown")),
            "category": item.get("category", item.get("Category", "Other")),
            "price": round(price, 2), "dispensary": disp,
            "weight": item.get("weight", ""),
            "cannabis_type": item.get("strainType", ""),
        })
    return products


def build_dashboard(all_products):
    product_map = {}
    for p in all_products:
        name = (p.get("name") or "").strip()
        if not name or not p.get("price"):
            continue
        key = name.lower()
        if key not in product_map:
            product_map[key] = {
                "name": name, "brand": p.get("brand", "Unknown"),
                "category": p.get("category", "Other"),
                "weight": p.get("weight", ""), "dispensaries": {},
            }
        product_map[key]["dispensaries"][p["dispensary"]] = p["price"]

    comparable = sorted([v for v in product_map.values() if len(v["dispensaries"]) >= 2],
                        key=lambda x: (x["category"], x["name"]))
    all_prods = sorted(product_map.values(), key=lambda x: (x["category"], x["name"]))

    by_disp = {}
    for p in all_products:
        d = p.get("dispensary", "Unknown")
        by_disp[d] = by_disp.get(d, 0) + 1

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
    print(f"  AFFINITY SCRAPER v13 (Apify Live Menus)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(DUTCHIE_STORES)} Dutchie + {len(WEEDMAPS_URLS)} Weedmaps")
    print(f"{'='*60}\n")

    all_products = []

    # Dutchie via Web Scraper
    print("  === DUTCHIE ===")
    dutchie_prods = scrape_dutchie()
    all_products.extend(dutchie_prods)
    print(f"  Dutchie: {len(dutchie_prods)} products\n")

    # Weedmaps via dedicated actor
    print("  === WEEDMAPS ===")
    wm_prods = scrape_weedmaps()
    all_products.extend(wm_prods)
    print(f"  Weedmaps: {len(wm_prods)} products\n")

    dashboard = build_dashboard(all_products)

    print(f"\n{'='*60}")
    print(f"  Total: {dashboard['stats']['total_products']}")
    print(f"  Dispensaries: {dashboard['stats']['dispensaries']}")
    print(f"  Comparable: {dashboard['stats']['comparable']}")
    print(f"\n  Breakdown:")
    for disp, count in list(dashboard['stats']['dispensary_counts'].items())[:20]:
        marker = " <-- YOU" if "Affinity" in disp else ""
        print(f"    {disp}: {count}{marker}")
    print(f"{'='*60}\n")

    os.makedirs("data", exist_ok=True)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(out, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    print(f"  Dashboard saved\n  DONE\n")


if __name__ == "__main__":
    main()
