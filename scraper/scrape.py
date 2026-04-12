#!/usr/bin/env python3
"""
ATHENA v16 — Affinity Competitive Intelligence Scraper (FINAL)

Architecture:
  1. Auth: Self-sustaining token chain (file → env fallback)
  2. Session: account.authorize call before data queries
  3. Date: Smart fallback (today → yesterday → day-before)
  4. Data: All CT cities, full pagination, active-only, deduped
  5. Output: dashboard_data.json with products, deals, dispensary metadata

Reviewed by: Architecture Agent, Data Integrity Agent, Edge Case Agent
"""

import json, os, sys, time, hashlib, requests
from datetime import datetime, date, timedelta

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUTH0_DOMAIN    = "dev-cfqdc946.us.auth0.com"
AUTH0_CLIENT_ID = "3lL2GMZKQYHw0en00bS4okH5wf02nRDu"
HOODIE_API      = "https://app.hoodieanalytics.com/api"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT  = os.path.join(SCRIPT_DIR, "..")
DATA_DIR   = os.path.join(REPO_ROOT, "data")
TOKEN_FILE = os.path.join(DATA_DIR, "new_refresh_token.txt")
OUTPUT_FILE = os.path.join(REPO_ROOT, "dashboard_data.json")

PAGE_SIZE  = 50
MAX_PAGES  = 200   # 200 × 50 = 10,000 (API hard cap per query)
RATE_DELAY = 0.15  # seconds between API calls

# Every CT town that could host a dispensary
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AUTH — Self-sustaining token chain
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_token_sources():
    """
    Return all available refresh tokens to try, in priority order:
      1. data/new_refresh_token.txt (rotated from previous run)
      2. HOODIE_REFRESH_TOKEN env var (GitHub secret, initial seed)
    """
    sources = []
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tok = f.read().strip()
        if tok and len(tok) > 20:
            sources.append(("committed file", tok))
    env = os.environ.get("HOODIE_REFRESH_TOKEN", "").strip()
    if env:
        sources.append(("GitHub secret", env))
    return sources


def try_auth(token_in):
    """Attempt Auth0 authentication. Returns (id_token, new_refresh_token) or (None, None)."""
    try:
        r = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/token",
            json={
                "grant_type": "refresh_token",
                "client_id": AUTH0_CLIENT_ID,
                "refresh_token": token_in,
            },
            timeout=30,
        )
    except requests.RequestException as e:
        print(f"    Network error: {e}")
        return None, None

    if r.status_code != 200:
        print(f"    Failed (HTTP {r.status_code}): {r.text[:120]}")
        return None, None

    body = r.json()
    return body.get("id_token"), body.get("refresh_token", "")


def authenticate():
    """Try all available tokens with automatic fallback."""
    sources = get_token_sources()
    if not sources:
        print("  FATAL: No refresh token in file or env")
        sys.exit(1)

    id_token = None
    new_rt = None

    for label, tok in sources:
        print(f"  Trying {label}...")
        id_token, new_rt = try_auth(tok)
        if id_token:
            print(f"  Auth OK via {label} (id_token: {len(id_token)} chars)")
            break
        else:
            print(f"  {label} token expired, trying next...")

    if not id_token:
        print()
        print("  FATAL: All tokens expired.")
        print("  To fix: get a fresh token from Hoodie Analytics")
        print("  DevTools Console → paste:")
        print("  JSON.parse(localStorage.getItem(")
        print("    '@@auth0spajs@@::3lL2GMZKQYHw0en00bS4okH5wf02nRDu::default::openid profile email offline_access'")
        print("  )).body.refresh_token")
        print("  Then update HOODIE_REFRESH_TOKEN in GitHub Secrets")
        sys.exit(1)

    # Save rotated refresh token for next run
    if new_rt:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(TOKEN_FILE, "w") as f:
            f.write(new_rt)
        print("  Refresh token rotated → saved for next run")

    # Establish Hoodie session
    try:
        sr = requests.get(
            f"{HOODIE_API}/account.authorize",
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=30,
        )
        print(f"  Hoodie session: {'OK' if sr.status_code == 200 else f'HTTP {sr.status_code} (continuing)'}")
    except Exception as e:
        print(f"  Hoodie session: error ({e}), continuing")

    return id_token


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATE — Smart fallback for Hoodie's daily index
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def find_valid_date(token):
    """
    Hoodie indexes data daily (hoodie-YYYY-MM-DD).
    Today's index may not exist yet if run before ~8AM ET.
    Try today → yesterday → 2 days ago.
    """
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    for days_back in [0, 1, 2]:
        d = (date.today() - timedelta(days=days_back)).isoformat()
        try:
            r = requests.post(
                f"{HOODIE_API}/openSearch.getDispensarySKUs",
                json={
                    "variables": {"date": d, "sort": [{"field": "UNITS_7_ROLLING", "order": "desc"}], "search": "", "size": 1, "from": 0},
                    "filterset": {"filterBy": {"states": ["Connecticut"], "cities": ["New Haven"],
                                               "dispensaries": [], "brands": [], "categories": []}},
                },
                headers=headers, timeout=30,
            )
            if r.status_code == 200:
                total = r.json().get("result", {}).get("data", {}).get("totalSKUs", 0)
                if total > 0:
                    print(f"  Using date: {d} ({total:,} SKUs available)")
                    return d
                else:
                    print(f"  Date {d}: 0 SKUs, trying older")
            else:
                err = r.text[:120]
                print(f"  Date {d}: HTTP {r.status_code} — {err}")
        except Exception as e:
            print(f"  Date {d}: error — {e}")

    # Last resort
    fallback = (date.today() - timedelta(days=1)).isoformat()
    print(f"  WARNING: No valid date found, using {fallback}")
    return fallback


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA FETCHING — Full pagination per city
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def fetch_city(token, city, query_date):
    """Fetch all products for a city, filter active client-side."""
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    active = []
    errors = 0

    for pg in range(MAX_PAGES):
        try:
            r = requests.post(
                f"{HOODIE_API}/openSearch.getDispensarySKUs",
                json={
                    "variables": {
                        "date": query_date,
                        "sort": [{"field": "UNITS_7_ROLLING", "order": "desc"}],
                        "search": "", "size": PAGE_SIZE, "from": pg * PAGE_SIZE,
                    },
                    "filterset": {
                        "filterBy": {
                            "states": ["Connecticut"], "cities": [city],
                            "dispensaries": [], "brands": [], "categories": [],
                        }
                    },
                },
                headers=headers, timeout=60,
            )
        except requests.RequestException as e:
            errors += 1
            if errors >= 3:
                break
            continue

        if r.status_code != 200:
            errors += 1
            if errors >= 3:
                break
            continue

        try:
            data = r.json().get("result", {}).get("data", {})
        except (ValueError, AttributeError):
            errors += 1
            break

        items = data.get("page", [])
        total = data.get("totalSKUs", 0)

        if not items:
            break

        batch = [i for i in items if i.get("IS_ACTIVE") is True]
        # Tag each item's dispensary name with Med/Rec menu type
        for item in batch:
            med = item.get("MEDICAL", False)
            dn = item.get("DISPENSARY_NAME", "Unknown")
            item["DISPENSARY_NAME"] = f"{dn} ({'Med' if med else 'Rec'})"
        active.extend(batch)

        if (pg + 1) * PAGE_SIZE >= total:
            break

        time.sleep(RATE_DELAY)

    return active


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA PROCESSING — SKU matching, dedup, compare, structure
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import re

def extract_sku(name):
    """
    Extract the product SKU ID from a Hoodie product name.
    
    Examples:
      "Gush Mintz Flower I 26712 (3.5g)"     → "26712"
      "M.O.B. Flower Minis T22.88% I 00880"  → "00880"
      "Rythm 3.5g Animal Face (I) 01304"      → "01304"
      "all:hours Mule Fuel 41623 (H) (3.5g)"  → "41623"
      "Sunshine Daydream - 0.5g Vape Cart"     → None (no SKU)
    
    Strategy: Find all standalone numbers with 4-6 digits.
    Exclude weights (3.5g, 14g, 28g), THC% (T22.88%), and pack counts.
    """
    if not name:
        return None
    
    # Find all standalone numbers (not part of decimals, not preceded by T)
    candidates = re.findall(r'(?<![.\d])\b(\d{4,6})\b(?!\.\d)', name)
    
    # Filter out common false positives
    false_pos = {'3500', '7000', '14000', '28000', '1000', '2000'}
    candidates = [c for c in candidates if c not in false_pos]
    
    if candidates:
        # Prefer 5-digit SKUs, then 4-digit, as primary product IDs
        fives = [c for c in candidates if len(c) == 5]
        fours = [c for c in candidates if len(c) == 4]
        if fives:
            return fives[0]
        if fours:
            return fours[0]
        return candidates[0]
    
    return None


def normalize_name(name):
    """
    Normalize a product name for fuzzy matching when no SKU is found.
    Strips weights, percentages, batch codes, and packaging info.
    """
    if not name:
        return ""
    n = name.lower().strip()
    # Remove parenthetical info: (3.5g), (H), (I), (S), (20pk), (AU), etc.
    n = re.sub(r'\([^)]*\)', '', n)
    # Remove THC/TC percentages: T22.88%, TC 30.24%
    n = re.sub(r'\b[tT][cC]?\s*\d+\.?\d*%?', '', n)
    # Remove weight specs: 3.5g, 0.5g, 1g, 14g, 28g, 7g
    n = re.sub(r'\b\d+\.?\d*\s*g\b', '', n)
    # Remove pack specs: 1pk, 5pk, 20pk
    n = re.sub(r'\b\d+pk\b', '', n)
    # Remove standalone numbers (SKUs, batch numbers)
    n = re.sub(r'\b\d{3,6}\b', '', n)
    # Remove extra whitespace and trailing dashes
    n = re.sub(r'[\s\-_]+', ' ', n).strip(' -|')
    return n


def make_product_key(item):
    """
    Create a comparison key for a product.
    Priority: SKU (most reliable) → normalized name + category
    """
    name = item.get("NAME", "")
    category = item.get("CATEGORY", "Other")
    
    sku = extract_sku(name)
    if sku:
        # SKU is the gold standard — same SKU = same product
        return f"sku:{sku}"
    
    # Fallback: normalized name + category
    norm = normalize_name(name)
    if norm:
        return f"name:{norm}|{category.lower()}"
    
    # Last resort: raw name
    return f"raw:{name.lower()}"


def dedup_items(all_items):
    """Remove duplicate products (same dispensary + same product key)."""
    seen = set()
    unique = []
    for item in all_items:
        key = f"{item.get('DISPENSARY_NAME','')}||{make_product_key(item)}"
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def build_dashboard(all_items):
    """Transform raw items into the dashboard JSON structure."""
    # Group by dispensary
    by_disp = {}
    for item in all_items:
        dn = item.get("DISPENSARY_NAME", "Unknown")
        if dn not in by_disp:
            by_disp[dn] = {"city": item.get("CITY", ""), "items": []}
        by_disp[dn]["items"].append(item)

    # Build product comparison map using SKU-based matching
    product_map = {}
    for dn, info in by_disp.items():
        for item in info["items"]:
            name = (item.get("NAME") or "").strip()
            price = item.get("ACTUAL_PRICE")
            if not name or not price or price <= 0:
                continue

            pkey = make_product_key(item)
            
            if pkey not in product_map:
                # Use the cleanest name as display name
                product_map[pkey] = {
                    "name": name,
                    "brand": (item.get("BRAND") or "Unknown").strip(),
                    "category": item.get("CATEGORY", "Other"),
                    "cannabis_type": item.get("CANNABIS_TYPE", ""),
                    "sku": extract_sku(name),
                    "dispensaries": {},
                }
            product_map[pkey]["dispensaries"][dn] = round(float(price), 2)

    comparable = sorted(
        [v for v in product_map.values() if len(v["dispensaries"]) >= 2],
        key=lambda x: (x["category"], x["name"]),
    )
    all_prods = sorted(product_map.values(), key=lambda x: (x["category"], x["name"]))

    # Dispensary metadata — include daily sales
    dispensaries_meta = {}
    for dn, info in by_disp.items():
        # Get dispensary-level daily sales from any item
        d_daily = 0
        d_addr = ""
        for item in info["items"]:
            ds = item.get("D_AVG_DAILY_SALES")
            if ds and ds > d_daily:
                d_daily = round(ds, 2)
            addr = item.get("D_FULL_ADDRESS", "")
            if addr and not d_addr:
                d_addr = addr
        dispensaries_meta[dn] = {
            "city": info["city"],
            "product_count": len(info["items"]),
            "is_affinity": "affinity" in dn.lower(),
            "daily_sales": d_daily,
            "address": d_addr,
        }

    # Active deals/promotions
    deals = []
    deal_seen = set()
    for item in all_items:
        orig = item.get("ORIGINAL_PRICE")
        disc = item.get("DISCOUNTED_PRICE")
        if item.get("IS_ON_PROMOTION") and orig and disc and orig > 0 and disc > 0 and orig > disc:
            dkey = f"{item.get('DISPENSARY_NAME')}:{item.get('NAME')}".lower()
            if dkey not in deal_seen:
                deal_seen.add(dkey)
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

    # ── SALES VELOCITY: Top 300 products by 7-day sales ──
    velocity = []
    for item in all_items:
        s7 = item.get("SALES_7_ROLLING") or 0
        if s7 > 0:
            velocity.append({
                "name": (item.get("NAME") or "").strip(),
                "brand": (item.get("BRAND") or "Unknown").strip(),
                "category": item.get("CATEGORY", "Other"),
                "dispensary": item.get("DISPENSARY_NAME", ""),
                "sales_7d": round(s7, 2),
                "sales_28d": round(item.get("SALES_28_ROLLING") or 0, 2),
                "sales_3m": round(item.get("SALES_3MTH_ROLLING") or 0, 2),
                "units_7d": round(item.get("UNITS_7_ROLLING") or 0, 1),
                "units_28d": round(item.get("UNITS_28_ROLLING") or 0, 1),
                "avg_daily_units": round(item.get("AVG_DAILY_UNITS_7DAYS") or 0, 1),
                "price": round(item.get("ACTUAL_PRICE") or 0, 2),
            })
    velocity.sort(key=lambda x: -x["sales_7d"])

    # ── STOCK ALERTS: Low stock + stockout risk ──
    stock_alerts = []
    for item in all_items:
        stock = item.get("STOCK_QTY")
        days_out = item.get("DAYS_UNTIL_STOCK_OUT")
        oos_28 = item.get("TOTAL_OOS_OVER_LAST_28_DAYS") or 0
        days_since = item.get("DAYS_SINCE_OOS") or 0
        if stock is not None or days_out is not None or oos_28 > 0:
            alert_level = "ok"
            if stock is not None and stock <= 5:
                alert_level = "critical"
            elif days_out is not None and days_out <= 3:
                alert_level = "critical"
            elif stock is not None and stock <= 15:
                alert_level = "warning"
            elif days_out is not None and days_out <= 7:
                alert_level = "warning"
            elif oos_28 > 5:
                alert_level = "warning"

            if alert_level != "ok":
                stock_alerts.append({
                    "name": (item.get("NAME") or "").strip(),
                    "brand": (item.get("BRAND") or "Unknown").strip(),
                    "category": item.get("CATEGORY", "Other"),
                    "dispensary": item.get("DISPENSARY_NAME", ""),
                    "stock_qty": stock,
                    "days_until_out": days_out,
                    "oos_last_28": oos_28,
                    "days_since_oos": days_since,
                    "alert": alert_level,
                })
    stock_alerts.sort(key=lambda x: (0 if x["alert"] == "critical" else 1, x.get("stock_qty") or 999))

    # ── CONSUMER INSIGHTS: Demographics per dispensary ──
    demo_by_disp = {}
    for dn, info in by_disp.items():
        segments = {}
        ages = {}
        affluency = {}
        for item in info["items"]:
            for seg in (item.get("HOODIE_SEGMENTS") or []):
                segments[seg] = segments.get(seg, 0) + 1
            for age in (item.get("AGE_SKEW") or []):
                ages[age] = ages.get(age, 0) + 1
            for afl in (item.get("AFFLUENCY") or []):
                affluency[afl] = affluency.get(afl, 0) + 1
        if segments or ages:
            demo_by_disp[dn] = {
                "segments": dict(sorted(segments.items(), key=lambda x: -x[1])[:8]),
                "ages": dict(sorted(ages.items(), key=lambda x: -x[1])),
                "affluency": dict(sorted(affluency.items(), key=lambda x: -x[1])),
                "urbanicity": info["items"][0].get("URBANICITY", "") if info["items"] else "",
            }

    # Use comparable products if enough exist, otherwise show all
    products_out = comparable if len(comparable) >= 10 else all_prods

    # Count matching method stats
    sku_matched = sum(1 for k in product_map if k.startswith("sku:"))
    name_matched = sum(1 for k in product_map if k.startswith("name:"))
    raw_matched = sum(1 for k in product_map if k.startswith("raw:"))

    return {
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "source": "hoodie_analytics",
        "products": products_out,
        "deals": deals[:300],
        "velocity": velocity[:300],
        "stock_alerts": stock_alerts[:300],
        "demographics": demo_by_disp,
        "dispensaries": dispensaries_meta,
        "stats": {
            "total_active": len(all_items),
            "dispensary_count": len(by_disp),
            "comparable": len(comparable),
            "unique_products": len(all_prods),
            "deals": len(deals),
            "match_by_sku": sku_matched,
            "match_by_name": name_matched,
            "match_by_raw": raw_matched,
            "stock_alerts_count": len(stock_alerts),
            "velocity_tracked": len(velocity),
            "dispensary_counts": {
                k: len(v["items"])
                for k, v in sorted(by_disp.items(), key=lambda x: -len(x[1]["items"]))
            },
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    t0 = time.time()
    print(f"\n{'='*64}")
    print(f"  ATHENA v16 — Affinity Competitive Intel")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  {len(CT_CITIES)} CT towns to scan")
    print(f"{'='*64}\n")

    token = authenticate()
    print()

    query_date = find_valid_date(token)
    print()

    # Fetch all cities
    all_items = []
    cities_hit = 0
    for i, city in enumerate(CT_CITIES):
        items = fetch_city(token, city, query_date)
        if items:
            all_items.extend(items)
            cities_hit += 1
            print(f"  [{i+1:2d}/{len(CT_CITIES)}] {city}: {len(items):,} active")

    # Deduplicate
    before = len(all_items)
    all_items = dedup_items(all_items)
    dupes = before - len(all_items)

    # Build output
    dashboard = build_dashboard(all_items)
    s = dashboard["stats"]

    elapsed = round(time.time() - t0)
    print(f"\n{'='*64}")
    print(f"  RESULTS ({elapsed}s)")
    print(f"  Active products:  {s['total_active']:,}")
    print(f"  Dispensaries:     {s['dispensary_count']}")
    print(f"  Comparable:       {s['comparable']:,}")
    print(f"  Unique products:  {s['unique_products']:,}")
    print(f"  Deals:            {s['deals']}")
    print(f"  Cities with data: {cities_hit}")
    print(f"  Matching: {s.get('match_by_sku',0)} by SKU, {s.get('match_by_name',0)} by name, {s.get('match_by_raw',0)} raw")
    if dupes:
        print(f"  Duplicates removed: {dupes}")
    print(f"\n  Top dispensaries:")
    for dn, count in list(s["dispensary_counts"].items())[:25]:
        flag = " ◀ YOU" if "affinity" in dn.lower() else ""
        print(f"    {dn}: {count:,}{flag}")
    print(f"{'='*64}\n")

    # Save main dashboard
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(dashboard, f, separators=(",", ":"), default=str)
    print(f"  ✓ Saved {OUTPUT_FILE}")

    # Save daily history snapshot for price tracking
    history_dir = os.path.join(DATA_DIR, "history")
    os.makedirs(history_dir, exist_ok=True)
    today_file = os.path.join(history_dir, f"{date.today().isoformat()}.json")

    # Build lightweight history record (just prices + dispensary)
    history_record = {
        "date": date.today().isoformat(),
        "prices": {},  # {product_key: {dispensary: price}}
        "new_products": [],  # products not seen yesterday
    }

    # Current product prices
    for p in dashboard.get("products", []):
        key = (p.get("name") or "").lower().strip()
        if key:
            history_record["prices"][key] = p.get("dispensaries", {})

    # Compare with yesterday to find new products
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    yesterday_file = os.path.join(history_dir, f"{yesterday}.json")
    if os.path.exists(yesterday_file):
        try:
            with open(yesterday_file) as f:
                prev = json.load(f)
            prev_keys = set(prev.get("prices", {}).keys())
            curr_keys = set(history_record["prices"].keys())
            new_keys = curr_keys - prev_keys
            dropped_keys = prev_keys - curr_keys

            # Build new product alerts
            new_alerts = []
            for p in dashboard.get("products", []):
                pkey = (p.get("name") or "").lower().strip()
                if pkey in new_keys:
                    new_alerts.append({
                        "name": p.get("name", ""),
                        "brand": p.get("brand", ""),
                        "category": p.get("category", ""),
                        "dispensaries": list(p.get("dispensaries", {}).keys()),
                    })

            dashboard["new_products"] = new_alerts[:200]
            dashboard["dropped_products"] = len(dropped_keys)
            print(f"  ✓ {len(new_alerts)} new products since yesterday, {len(dropped_keys)} dropped")

            # Re-save with new product alerts
            with open(OUTPUT_FILE, "w") as f:
                json.dump(dashboard, f, separators=(",", ":"), default=str)
        except Exception as e:
            print(f"  History comparison failed: {e}")
    else:
        print(f"  No previous data for comparison (first run or new day)")

    # Save today's snapshot
    with open(today_file, "w") as f:
        json.dump(history_record, f, separators=(",", ":"), default=str)
    print(f"  ✓ Saved history snapshot: {today_file}")

    # Clean up old history (keep last 90 days)
    for hf in sorted(os.listdir(history_dir)):
        if hf.endswith(".json"):
            fdate = hf.replace(".json", "")
            try:
                if (date.today() - date.fromisoformat(fdate)).days > 90:
                    os.remove(os.path.join(history_dir, hf))
            except ValueError:
                pass

    print(f"  DONE\n")


if __name__ == "__main__":
    main()
