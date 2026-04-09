#!/usr/bin/env python3
"""
DISPENSARY SCRAPER — Playwright Edition
Loads actual dispensary web pages in a headless browser and extracts product data.
Works with Dutchie and Weedmaps by intercepting their API responses.
"""

import json
import asyncio
import os
import sys
import time
from datetime import datetime

# Playwright imports
from playwright.async_api import async_playwright

# ─── DUTCHIE DISPENSARY SLUGS ────────────────────────────────
# Format: dutchie.com/dispensary/{slug}
DUTCHIE_DISPENSARIES = {
    "RISE Dispensary Orange": "rise-orange",
    "RISE Dispensary Branford": "rise-branford",
    "Fine Fettle Newington": "fine-fettle-newington",
    "Fine Fettle Manchester": "fine-fettle-manchester",
    "Fine Fettle Willimantic": "fine-fettle-willimantic",
    "Fine Fettle Stamford": "fine-fettle-stamford",
    "Fine Fettle Norwalk": "fine-fettle-norwalk",
    "Fine Fettle Bristol": "fine-fettle-bristol",
    "Fine Fettle Waterbury": "fine-fettle-waterbury",
    "Curaleaf Stamford": "curaleaf-ct-stamford",
    "Curaleaf Manchester": "curaleaf-ct-manchester",
    "Curaleaf Groton": "curaleaf-ct-groton",
    "Sweetspot West Hartford": "sweetspot-west-hartford",
    "Sweetspot Stamford": "sweetspot-cannabis-dispensary-stamford",
    "Insa New Haven": "insa-new-haven",
    "Insa Hartford": "insa-hartford",
    "Crisp Cannabis Cromwell": "crisp-cannabis-cromwell",
    "Crisp Cannabis East Hartford": "crisp-cannabis-east-hartford",
    "Venu Flower Collective": "venu-middletown",
    "Trulieve Bristol": "trulieve-bristol",
    "High Profile Hamden": "high-profile-hamden",
    "Still River Wellness": "still-river-wellness-torrington",
    "Nova Farms New Britain": "nova-farms-new-britain",
}

# ─── WEEDMAPS DISPENSARY SLUGS ───────────────────────────────
# Format: weedmaps.com/dispensaries/{slug}
WEEDMAPS_DISPENSARIES = {
    "Zen Leaf Meriden": "zen-leaf-cannabis-dispensary-meriden-2",
    "Zen Leaf Waterbury": "zen-leaf-cannabis-dispensary-waterbury",
    "Zen Leaf Norwich": "zen-leaf-cannabis-dispensary-norwich",
    "Zen Leaf Naugatuck": "zen-leaf-cannabis-dispensary-naugatuck",
    "Zen Leaf Enfield": "zen-leaf-cannabis-dispensary-enfield",
    "Zen Leaf Newington": "zen-leaf-cannabis-dispensary-newington",
    "Zen Leaf Ashford": "zen-leaf-cannabis-dispensary-ashford",
    "Budr Cannabis Stratford": "budr-cannabis-stratford",
    "Budr Cannabis Danbury": "budr-cannabis-danbury-2",
    "Budr Cannabis Vernon": "budr-cannabis-vernon",
    "Budr Cannabis Montville": "budr-cannabis-montville",
    "Budr Cannabis Tolland": "budr-cannabis-tolland",
    "Shangri-La Norwalk": "shangri-la-dispensary-norwalk-3",
    "Shangri-La Waterbury": "shangri-la-dispensary-waterbury",
    "Shangri-La Plainville": "shangri-la-dispensary-plainville",
    "Higher Collective Bridgeport": "higher-collective-3",
    "Higher Collective New London": "higher-collective-new-london",
    "Lit New Haven Cannabis": "lit-new-haven-cannabis",
    "Hi! People Derby": "hi-people-derby",
    "Rejoice Meriden": "rejoice-dispensary-meriden",
    "Nightjar East Lyme": "nightjar-east-lyme-dispensary",
    "Octane Enfield": "octane-cannabis-dispensary-enfield",
}


async def scrape_dutchie_dispensary(page, name, slug):
    """Load a Dutchie dispensary page and extract product data."""
    url = f"https://dutchie.com/dispensary/{slug}"
    print(f"  → Loading {name}: {url}")

    products = []
    api_responses = []

    # Intercept API responses that contain product data
    async def capture_response(response):
        try:
            if "graphql" in response.url or "api" in response.url:
                if response.status == 200:
                    try:
                        body = await response.json()
                        api_responses.append(body)
                    except:
                        pass
        except:
            pass

    page.on("response", capture_response)

    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        # Give extra time for dynamic content
        await page.wait_for_timeout(3000)

        # Try to extract products from the page DOM
        products = await page.evaluate("""() => {
            const items = [];

            // Try multiple selectors that Dutchie uses
            const selectors = [
                '[data-testid="product-card"]',
                '.product-card',
                '[class*="ProductCard"]',
                '[class*="product-card"]',
                '[class*="MenuProduct"]',
                'article[class*="product"]',
                '[data-test="product"]',
            ];

            let cards = [];
            for (const sel of selectors) {
                cards = document.querySelectorAll(sel);
                if (cards.length > 0) break;
            }

            cards.forEach(card => {
                try {
                    // Extract text content and look for price patterns
                    const text = card.innerText || '';
                    const nameEl = card.querySelector('h2, h3, [class*="name"], [class*="Name"], [class*="title"], [class*="Title"]');
                    const priceEl = card.querySelector('[class*="price"], [class*="Price"], [class*="cost"]');
                    const brandEl = card.querySelector('[class*="brand"], [class*="Brand"]');
                    const categoryEl = card.querySelector('[class*="category"], [class*="Category"]');

                    const name = nameEl ? nameEl.innerText.trim() : '';
                    const priceText = priceEl ? priceEl.innerText.trim() : '';
                    const brand = brandEl ? brandEl.innerText.trim() : '';
                    const category = categoryEl ? categoryEl.innerText.trim() : '';

                    // Extract price number
                    const priceMatch = priceText.match(/\\$([\\d.]+)/);
                    const price = priceMatch ? parseFloat(priceMatch[1]) : null;

                    if (name && price) {
                        items.push({ name, price, brand, category });
                    }
                } catch(e) {}
            });

            // If no cards found, try extracting from any price-like elements
            if (items.length === 0) {
                const allText = document.body.innerText;
                // Look for product-price patterns in the page text
                const lines = allText.split('\\n').filter(l => l.includes('$'));
                // Return raw text for debugging
                return { raw_lines: lines.slice(0, 50), items: [] };
            }

            return { items, raw_lines: [] };
        }""")

    except Exception as e:
        print(f"  ✗ Error loading {name}: {e}")
        return []
    finally:
        page.remove_listener("response", capture_response)

    # Try to extract products from captured API responses
    for resp in api_responses:
        try:
            extracted = extract_products_from_api(resp)
            if extracted:
                products = extracted
                break
        except:
            pass

    # Use DOM extraction if available
    if isinstance(products, dict):
        if products.get("items"):
            return products["items"]
        elif products.get("raw_lines"):
            print(f"  ⚠ Found price text but no structured products for {name}")
            return []
    elif isinstance(products, list):
        return products

    return []


def extract_products_from_api(data):
    """Try to extract product data from a Dutchie API response."""
    products = []

    # Navigate through common Dutchie response structures
    if isinstance(data, dict):
        # Check for filteredProducts (GraphQL response)
        fp = (data.get("data", {}).get("filteredProducts", {}) or {}).get("products", [])
        if fp:
            for p in fp:
                pricing = p.get("pricing", {}) or {}
                products.append({
                    "name": p.get("name", ""),
                    "brand": (p.get("brand", {}) or {}).get("name", "Unknown"),
                    "category": p.get("category", ""),
                    "price": pricing.get("recPrice") or pricing.get("price") or pricing.get("medPrice"),
                })
            return products

        # Check for menu items
        mi = data.get("data", {}).get("menu", {}).get("items", [])
        if mi:
            for p in mi:
                products.append({
                    "name": p.get("name", ""),
                    "brand": p.get("brand", "Unknown"),
                    "category": p.get("category", ""),
                    "price": p.get("price") or p.get("recPrice"),
                })
            return products

    return products


async def scrape_weedmaps_dispensary(page, name, slug):
    """Load a Weedmaps dispensary menu page and extract product data."""
    url = f"https://weedmaps.com/dispensaries/{slug}/menu"
    print(f"  → Loading {name}: {url}")

    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)

        # Check for age gate and click through
        try:
            age_btn = await page.query_selector('button:has-text("Yes"), button:has-text("Enter"), button:has-text("I am")')
            if age_btn:
                await age_btn.click()
                await page.wait_for_timeout(2000)
        except:
            pass

        products = await page.evaluate("""() => {
            const items = [];

            // Weedmaps product card selectors
            const selectors = [
                '[data-testid="menu-item"]',
                '[class*="MenuCard"]',
                '[class*="menu-item"]',
                '[class*="ProductCard"]',
                '[class*="product-card"]',
                'a[href*="/menu/"]',
            ];

            let cards = [];
            for (const sel of selectors) {
                cards = document.querySelectorAll(sel);
                if (cards.length > 0) break;
            }

            cards.forEach(card => {
                try {
                    const text = card.innerText || '';
                    const nameEl = card.querySelector('h2, h3, h4, [class*="name"], [class*="Name"], [class*="title"]');
                    const priceEl = card.querySelector('[class*="price"], [class*="Price"]');
                    const brandEl = card.querySelector('[class*="brand"], [class*="Brand"]');

                    const name = nameEl ? nameEl.innerText.trim() : '';
                    const priceText = priceEl ? priceEl.innerText.trim() : text;
                    const brand = brandEl ? brandEl.innerText.trim() : '';

                    const priceMatch = priceText.match(/\\$([\\d.]+)/);
                    const price = priceMatch ? parseFloat(priceMatch[1]) : null;

                    if (name && price) {
                        items.push({ name, price, brand, category: '' });
                    }
                } catch(e) {}
            });

            return items;
        }""")

        return products or []

    except Exception as e:
        print(f"  ✗ Error loading {name}: {e}")
        return []


async def run_scraper():
    """Main scraper entry point."""
    print(f"\n{'='*60}")
    print(f"  🌿 AFFINITY SCRAPER (Playwright)")
    print(f"  📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  📍 {len(DUTCHIE_DISPENSARIES) + len(WEEDMAPS_DISPENSARIES)} dispensaries")
    print(f"{'='*60}\n")

    all_products = {}  # dispensary_name -> [products]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        page = await context.new_page()

        # Scrape Dutchie dispensaries
        print("▸ Phase 1: Dutchie dispensaries\n")
        for name, slug in DUTCHIE_DISPENSARIES.items():
            try:
                products = await scrape_dutchie_dispensary(page, name, slug)
                all_products[name] = products
                print(f"  ✓ {name}: {len(products)} products\n")
            except Exception as e:
                print(f"  ✗ {name}: {e}\n")
                all_products[name] = []
            await asyncio.sleep(2)

        # Scrape Weedmaps dispensaries
        print("\n▸ Phase 2: Weedmaps dispensaries\n")
        for name, slug in WEEDMAPS_DISPENSARIES.items():
            try:
                products = await scrape_weedmaps_dispensary(page, name, slug)
                all_products[name] = products
                print(f"  ✓ {name}: {len(products)} products\n")
            except Exception as e:
                print(f"  ✗ {name}: {e}\n")
                all_products[name] = []
            await asyncio.sleep(2)

        await browser.close()

    return all_products


def build_dashboard_data(all_products):
    """Convert scraped data into dashboard format."""
    # Group products by name+brand across dispensaries
    product_map = {}

    for disp_name, products in all_products.items():
        for p in products:
            if not p.get("name") or not p.get("price"):
                continue
            key = f"{p.get('brand', 'Unknown')}::{p['name']}".lower().strip()
            if key not in product_map:
                product_map[key] = {
                    "name": p["name"],
                    "brand": p.get("brand", "Unknown"),
                    "category": p.get("category", "Other") or "Other",
                    "weight": p.get("weight", ""),
                    "dispensaries": {},
                }
            product_map[key]["dispensaries"][disp_name] = round(p["price"], 2)

    # Only keep products found at 2+ dispensaries
    comparable = [v for v in product_map.values() if len(v["dispensaries"]) >= 2]
    comparable.sort(key=lambda x: (x["category"], x["name"]))

    return {
        "scraped_at": datetime.now().isoformat(),
        "products": comparable,
        "deals": [],
        "stats": {
            "total_products_raw": sum(len(v) for v in all_products.values()),
            "comparable_products": len(comparable),
            "dispensaries_with_data": len([k for k, v in all_products.items() if v]),
        },
    }


async def main():
    all_products = await run_scraper()

    total = sum(len(v) for v in all_products.values())
    with_data = len([k for k, v in all_products.items() if v])
    print(f"\n📦 Total products scraped: {total}")
    print(f"📍 Dispensaries with data: {with_data}")

    # Save raw data
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"data/raw_scrape_{ts}.json", "w") as f:
        json.dump(all_products, f, indent=2, default=str)

    # Build and save dashboard data
    dashboard = build_dashboard_data(all_products)
    print(f"📊 Comparable products: {dashboard['stats']['comparable_products']}")

    # Save to both data/ and parent (for GitHub Actions)
    with open(f"data/dashboard_data_{ts}.json", "w") as f:
        json.dump(dashboard, f, indent=2, default=str)

    # Also save to parent directory for GitHub Actions workflow
    parent_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(parent_path, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)

    print(f"\n✅ Dashboard data saved")


if __name__ == "__main__":
    asyncio.run(main())
