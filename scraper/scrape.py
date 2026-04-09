#!/usr/bin/env python3
"""
AFFINITY SCRAPER (Firecrawl v4) — Uses verified Leafly URLs as data source.
Leafly has consistent menu pages with product names and prices for every CT dispensary.
"""

import json, os, sys, time, re, requests
from datetime import datetime

EXTRACT_URL = "https://api.firecrawl.dev/v2/extract"
SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

# Verified Leafly URLs — these all exist and show menu data
STORES = [
    {"name": "Affinity - New Haven", "url": "https://www.leafly.com/dispensary-info/affinity-dispensary-new-haven"},
    {"name": "Affinity - Bridgeport", "url": "https://www.leafly.com/dispensary-info/affinity-dispensary-bridgeport"},
    {"name": "Higher Collective Bridgeport", "url": "https://www.leafly.com/dispensary-info/higher-collective-bridgeport"},
    {"name": "Lit New Haven Cannabis", "url": "https://www.leafly.com/dispensary-info/lit-new-haven"},
    {"name": "Insa New Haven", "url": "https://www.leafly.com/dispensary-info/insa-cannabis-dispensary-new-haven"},
    {"name": "RISE Dispensary Orange", "url": "https://www.leafly.com/dispensary-info/rise-cannabis-dispensary-orange"},
    {"name": "High Profile Hamden", "url": "https://www.leafly.com/dispensary-info/high-profile-hamden"},
    {"name": "Hi! People Derby", "url": "https://www.leafly.com/dispensary-info/hi-people-derby"},
    {"name": "Budr Cannabis Stratford", "url": "https://www.leafly.com/dispensary-info/budr-cannabis-stratford"},
    {"name": "RISE Dispensary Branford", "url": "https://www.leafly.com/dispensary-info/rise-cannabis-dispensary-branford"},
    {"name": "Zen Leaf Naugatuck", "url": "https://www.leafly.com/dispensary-info/zen-leaf-naugatuck"},
    {"name": "Zen Leaf Meriden", "url": "https://www.leafly.com/dispensary-info/zen-leaf-meriden"},
    {"name": "Curaleaf Stamford", "url": "https://www.leafly.com/dispensary-info/curaleaf-ct-stamford"},
    {"name": "Sweetspot Stamford", "url": "https://www.leafly.com/dispensary-info/sweetspot-cannabis-dispensary-stamford"},
    {"name": "Fine Fettle Newington", "url": "https://www.leafly.com/dispensary-info/fine-fettle-newington"},
    {"name": "Shangri-La Waterbury", "url": "https://www.leafly.com/dispensary-info/shangri-la-cannabis-adult-use-retailer-waterbury"},
    {"name": "Rejoice Seymour", "url": "https://www.leafly.com/dispensary-info/rejoice-seymour"},
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
    "Extract ALL cannabis products listed on this dispensary page. "
    "For each product: name, brand, category (Flower/Pre-Rolls/Vaporizers/Edibles/Concentrates/Tinctures), "
    "price in dollars, weight/size. Also extract any deals or specials."
)

HEADERS = {}

def init():
    global HEADERS
    HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def try_extract(name, url):
    payload = {"urls": [url], "prompt": PROMPT, "schema": SCHEMA}
    try:
        resp = requests.post(EXTRACT_URL, json=payload, headers=HEADERS, timeout=120)
        if resp.status_code == 429:
            wait = 20
            print(f"    rate limited, waiting {wait}s...")
            time.sleep(wait)
            resp = requests.post(EXTRACT_URL, json=payload, headers=HEADERS, timeout=120)
        if resp.status_code != 200:
            try: msg = resp.json().get("error","")[:150]
            except: msg = resp.text[:150]
            print(f"    extract HTTP {resp.status_code}: {msg}")
            return None
        data = resp.json()
        if not data.get("success"):
            job_id = data.get("id")
            if job_id:
                return poll_job(name, job_id)
            print(f"    extract failed: {data.get('error','?')[:100]}")
            return None
        result = data.get("data", {})
        products = [p for p in result.get("products", []) if p.get("name") and p.get("price") and p["price"] > 0]
        deals = result.get("deals", []) or []
        return {"products": products, "deals": deals}
    except Exception as e:
        print(f"    extract error: {e}")
        return None

def poll_job(name, job_id):
    url = f"{EXTRACT_URL}/{job_id}"
    for _ in range(24):
        time.sleep(5)
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200: continue
            d = r.json()
            if d.get("status") == "completed":
                result = d.get("data", {})
                products = [p for p in result.get("products", []) if p.get("name") and p.get("price") and p["price"] > 0]
                return {"products": products, "deals": result.get("deals", []) or []}
            elif d.get("status") == "failed":
                print(f"    job failed")
                return None
        except: continue
    print(f"    job timed out")
    return None

def try_markdown(name, url):
    try:
        resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code == 429:
            time.sleep(20)
            resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code != 200:
            print(f"    markdown HTTP {resp.status_code}")
            return None
        data = resp.json()
        if not data.get("success"): return None
        md = data.get("data", {}).get("markdown", "")
        if not md: return None
        products = parse_md(md)
        return {"products": products, "deals": []}
    except Exception as e:
        print(f"    markdown error: {e}")
        return None

def parse_md(md):
    products = []
    pp = re.compile(r'\$(\d+\.?\d*)')
    cat = "Other"
    lines = md.split("\n")
    for i, line in enumerate(lines):
        line = line.strip()
        if not line: continue
        lo = line.lower()
        for c, lb in [("flower","Flower"),("pre-roll","Pre-Rolls"),("vape","Vaporizers"),("cartridge","Vaporizers"),("edible","Edibles"),("gumm","Edibles"),("concentrate","Concentrates"),("tincture","Tinctures")]:
            if c in lo and len(line) < 50:
                cat = lb; break
        prices = pp.findall(line)
        if not prices: continue
        price = float(prices[0])
        if price < 5 or price > 500: continue
        nm = re.sub(r'\$[\d.]+','',line).strip()
        nm = re.sub(r'[#*_\[\]()>|]','',nm).strip()
        nm = re.sub(r'\s+',' ',nm).strip()
        if len(nm) < 3 and i > 0:
            nm = re.sub(r'[#*_\[\]()>|]','',lines[i-1]).strip()
        if not nm or len(nm) < 3: continue
        pts = re.split(r'\s*[-–|]\s*', nm, 1)
        brand = pts[0].strip()[:40] if len(pts) > 1 else "Unknown"
        pname = pts[1].strip()[:80] if len(pts) > 1 else nm[:80]
        products.append({"name": pname, "brand": brand, "category": cat, "price": price, "weight": ""})
    return products

def scrape(name, url):
    print(f"  -> {name}")
    result = try_extract(name, url)
    if result and result["products"]:
        print(f"     OK: {len(result['products'])} products (extract)")
        return result
    print(f"     trying markdown...")
    result = try_markdown(name, url)
    if result and result["products"]:
        print(f"     OK: {len(result['products'])} products (markdown)")
        return result
    print(f"     FAILED")
    return None

def build_dash(results):
    pm = {}
    for disp, data in results.items():
        if not data: continue
        for p in data.get("products", []):
            nm = (p.get("name") or "").strip()
            br = (p.get("brand") or "Unknown").strip()
            ct = (p.get("category") or "Other").strip()
            pr = p.get("price")
            wt = (p.get("weight") or "").strip()
            if not nm or not pr or pr <= 0: continue
            key = f"{br}::{nm}".lower()
            if key not in pm:
                pm[key] = {"name":nm,"brand":br,"category":ct,"weight":wt,"dispensaries":{}}
            pm[key]["dispensaries"][disp] = round(float(pr), 2)
    comp = sorted([v for v in pm.values() if len(v["dispensaries"]) >= 2], key=lambda x:(x["category"],x["name"]))
    allp = sorted(pm.values(), key=lambda x:(x["category"],x["name"]))
    out = comp if len(comp) >= 5 else allp
    deals = []
    for disp, data in results.items():
        if not data: continue
        for d in data.get("deals", []):
            if d.get("title"):
                deals.append({"dispensary":disp,"title":d["title"],"type":d.get("discount_type","other"),"category":"All","expires":None})
    return {
        "scraped_at": datetime.now().isoformat(),
        "products": out, "deals": deals,
        "stats": {
            "total": sum(len(d["products"]) for d in results.values() if d),
            "comparable": len(comp), "all": len(allp),
            "success": len([v for v in results.values() if v]),
            "failed": len([v for v in results.values() if not v]),
            "deals": len(deals),
        },
    }

def main():
    if not API_KEY:
        print("ERROR: FIRECRAWL_API_KEY not set"); sys.exit(1)
    init()
    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER v4 (Firecrawl + Leafly)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(STORES)} stores")
    print(f"{'='*60}\n")
    results = {}
    for s in STORES:
        results[s["name"]] = scrape(s["name"], s["url"])
        time.sleep(6)  # 6s between requests to avoid rate limits
    dash = build_dash(results)
    print(f"\n{'='*60}")
    for k, v in dash["stats"].items(): print(f"  {k}: {v}")
    print(f"{'='*60}\n")
    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"data/raw_{ts}.json","w") as f: json.dump(results,f,indent=2,default=str)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..","dashboard_data.json")
    with open(out,"w") as f: json.dump(dash,f,indent=2,default=str)
    print("  DONE\n")

if __name__ == "__main__":
    main()
