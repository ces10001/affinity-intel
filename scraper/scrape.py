#!/usr/bin/env python3
"""AFFINITY SCRAPER v6 — Mix of direct sites + Weedmaps (verified URLs)"""

import json, os, sys, time, re, requests
from datetime import datetime

EXTRACT_URL = "https://api.firecrawl.dev/v2/extract"
SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

# All URLs verified — Weedmaps slugs confirmed from weedmaps.com/dispensaries/in/connecticut
STORES = [
    # Affinity — direct site (rec + med)
    {"name": "Affinity NH (Rec)", "url": "https://www.affinityct.com/menu-adult"},
    {"name": "Affinity NH (Med)", "url": "https://www.affinityct.com/menu"},
    {"name": "Affinity BP (Rec)", "url": "https://www.affinityct.com/bridgeport-menu-adult"},
    {"name": "Affinity BP (Med)", "url": "https://www.affinityct.com/bridgeport-menu"},
    # Higher Collective — direct site (PROVEN: 26 products)
    {"name": "Higher Collective Bridgeport", "url": "https://highercollective.com/locations/bridgeport/menu/"},
    # Competitors — Weedmaps pages (renders menu in HTML, bypasses Dutchie iframes)
    {"name": "Lit New Haven Cannabis", "url": "https://weedmaps.com/dispensaries/lit-new-haven"},
    {"name": "Insa New Haven", "url": "https://weedmaps.com/dispensaries/insa-new-haven"},
    {"name": "RISE Dispensary Orange", "url": "https://weedmaps.com/dispensaries/southern-ct-wellness-and-healing"},
    {"name": "High Profile Hamden", "url": "https://weedmaps.com/dispensaries/high-profile-hamden"},
    {"name": "Hi! People Derby", "url": "https://weedmaps.com/dispensaries/hi-people-derby"},
    {"name": "Budr Cannabis Stratford", "url": "https://weedmaps.com/dispensaries/budr-cannabis-stratford"},
    {"name": "RISE Dispensary Branford", "url": "https://weedmaps.com/dispensaries/bluepoint-wellness-of-branford"},
    {"name": "Zen Leaf Naugatuck", "url": "https://weedmaps.com/dispensaries/zen-leaf-naugatuck"},
    {"name": "Zen Leaf Meriden", "url": "https://weedmaps.com/dispensaries/zen-leaf-meriden"},
    {"name": "Curaleaf Stamford", "url": "https://weedmaps.com/dispensaries/curaleaf-ct-stamford"},
    {"name": "Sweetspot Stamford", "url": "https://weedmaps.com/dispensaries/sweetspot-stamford"},
    {"name": "Fine Fettle Newington", "url": "https://weedmaps.com/dispensaries/fine-fettle-newington"},
    {"name": "Shangri-La Waterbury", "url": "https://weedmaps.com/dispensaries/shangri-la-waterbury"},
    {"name": "Rejoice Seymour", "url": "https://weedmaps.com/dispensaries/rejoice-seymour"},
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
    "Extract ALL cannabis products from this dispensary menu page. "
    "If there is an age verification gate, assume yes/21+. "
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
        if resp.status_code == 402:
            print(f"    OUT OF CREDITS"); return "no_credits"
        if resp.status_code == 429:
            print(f"    rate limited, waiting 25s..."); time.sleep(25)
            resp = requests.post(EXTRACT_URL, json=payload, headers=HEADERS, timeout=120)
        if resp.status_code != 200:
            try: msg = resp.json().get("error","")[:200]
            except: msg = resp.text[:200]
            print(f"    extract HTTP {resp.status_code}: {msg}")
            return None
        data = resp.json()
        if not data.get("success"):
            job_id = data.get("id")
            if job_id: return poll_job(name, job_id)
            print(f"    extract: {data.get('error','?')[:100]}")
            return None
        result = data.get("data", {})
        products = [p for p in result.get("products", []) if p.get("name") and p.get("price") and p["price"] > 0]
        deals = result.get("deals", []) or []
        return {"products": products, "deals": deals}
    except Exception as e:
        print(f"    extract error: {e}"); return None

def poll_job(name, job_id):
    url = f"{EXTRACT_URL}/{job_id}"
    for _ in range(30):
        time.sleep(5)
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200: continue
            d = r.json()
            s = d.get("status", "")
            if s == "completed":
                result = d.get("data", {})
                products = [p for p in result.get("products", []) if p.get("name") and p.get("price") and p["price"] > 0]
                return {"products": products, "deals": result.get("deals", []) or []}
            elif s == "failed":
                print(f"    job failed"); return None
            print(f"    polling... ({s})")
        except: continue
    print(f"    job timed out"); return None

def try_markdown(name, url):
    try:
        resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code == 402:
            print(f"    OUT OF CREDITS"); return "no_credits"
        if resp.status_code == 429:
            time.sleep(25)
            resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code != 200:
            print(f"    markdown HTTP {resp.status_code}"); return None
        data = resp.json()
        if not data.get("success"): return None
        md = data.get("data", {}).get("markdown", "")
        if not md: return None
        products = parse_md(md)
        return {"products": products, "deals": []}
    except Exception as e:
        print(f"    markdown error: {e}"); return None

def parse_md(md):
    products = []
    pp = re.compile(r'\$(\d+\.?\d*)')
    cat = "Other"
    lines = md.split("\n")
    for i, line in enumerate(lines):
        line = line.strip()
        if not line: continue
        lo = line.lower()
        for c, lb in [("flower","Flower"),("pre-roll","Pre-Rolls"),("preroll","Pre-Rolls"),
                       ("vape","Vaporizers"),("cartridge","Vaporizers"),("edible","Edibles"),
                       ("gumm","Edibles"),("concentrate","Concentrates"),("tincture","Tinctures")]:
            if c in lo and len(line) < 50: cat = lb; break
        prices = pp.findall(line)
        if not prices: continue
        price = float(prices[0])
        if price < 5 or price > 500: continue
        nm = re.sub(r'\$[\d.]+','',line).strip()
        nm = re.sub(r'[#*_\[\]()>|]','',nm).strip()
        nm = re.sub(r'\s+',' ',nm).strip()
        if len(nm) < 3 and i > 0: nm = re.sub(r'[#*_\[\]()>|]','',lines[i-1]).strip()
        if not nm or len(nm) < 3: continue
        pts = re.split(r'\s*[-\u2013|]\s*', nm, 1)
        brand = pts[0].strip()[:40] if len(pts) > 1 else "Unknown"
        pname = pts[1].strip()[:80] if len(pts) > 1 else nm[:80]
        products.append({"name":pname,"brand":brand,"category":cat,"price":price,"weight":""})
    return products

def scrape(name, url):
    print(f"  -> {name}")
    print(f"     {url}")
    result = try_extract(name, url)
    if result == "no_credits":
        return None
    if result and isinstance(result, dict) and result.get("products"):
        print(f"     OK: {len(result['products'])} products (extract)")
        return result
    print(f"     trying markdown...")
    result = try_markdown(name, url)
    if result == "no_credits":
        return None
    if result and isinstance(result, dict) and result.get("products"):
        print(f"     OK: {len(result['products'])} products (markdown)")
        return result
    print(f"     FAILED: 0 products")
    return None

def build_dash(results):
    pm = {}
    for disp, data in results.items():
        if not data: continue
        display = disp
        if "Affinity NH" in disp: display = "Affinity - New Haven"
        elif "Affinity BP" in disp: display = "Affinity - Bridgeport"
        mt = " (Rec)" if "(Rec)" in disp else " (Med)" if "(Med)" in disp else ""
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
            label = f"{display}{mt}".strip()
            if label in pm[key]["dispensaries"]:
                pm[key]["dispensaries"][label] = min(pm[key]["dispensaries"][label], round(float(pr),2))
            else:
                pm[key]["dispensaries"][label] = round(float(pr),2)
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
    print(f"  AFFINITY SCRAPER v6")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(STORES)} pages")
    print(f"{'='*60}\n")
    results = {}
    out_of_credits = False
    for s in STORES:
        if out_of_credits:
            print(f"  -> {s['name']}: SKIPPED (no credits)")
            results[s["name"]] = None
            continue
        results[s["name"]] = scrape(s["name"], s["url"])
        if results[s["name"]] is None:
            # Check if it was a credits issue by looking at last output
            pass
        time.sleep(6)
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
