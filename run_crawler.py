# -*- coding: utf-8 -*-
"""
🚀 TIKI CRAWLER - Crawl dữ liệu sản phẩm từ Tiki.vn
Cấu hình trong file: input_config.py
Chạy: python run_crawler.py
"""

import requests
import pandas as pd
import sqlite3
import time
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# Import cấu hình
from input_config import (
    MODE, CATEGORIES, URL, SEARCH_KEYWORDS, SORT_BY,
    PRODUCT_IDS, MAX_PAGES, SAVE_DATA, ANALYZE_DATA, OUTPUT_DIR
)

# ============================================================
# CẤU HÌNH
# ============================================================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://tiki.vn/',
    'Origin': 'https://tiki.vn'
}

LISTINGS_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
PRODUCT_URL = "https://tiki.vn/api/v2/products"

ELECTRONICS_CATEGORIES = {
    'dien_thoai': 1789,
    'laptop': 8095,
    'may_tinh_bang': 1794,
    'am_thanh': 4221,
    'phu_kien_dien_thoai': 1801,
    'thiet_bi_deo': 8132,
    'camera': 1805,
    'may_choi_game': 8060,
    'linh_kien_may_tinh': 1846,
    'thiet_bi_van_phong': 1883,
}


# ============================================================
# API FUNCTIONS
# ============================================================
def get_products_by_category(category_id, category_name, page=1, limit=40):
    """Lấy sản phẩm theo category"""
    params = {
        'limit': limit, 'include': 'advertisement', 'aggregations': 2,
        'category': category_id, 'page': page, 'urlKey': category_name
    }
    try:
        resp = requests.get(LISTINGS_URL, headers=HEADERS, params=params, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


def get_products_by_url(url, page=1, limit=40):
    """Lấy sản phẩm từ URL"""
    parsed = urlparse(url)
    params = {'limit': limit, 'include': 'advertisement', 'aggregations': 2, 'page': page}
    
    # Lấy category từ URL
    match = re.search(r'/c(\d+)', parsed.path)
    if match:
        params['category'] = int(match.group(1))
        params['urlKey'] = parsed.path.split('/')[1] if len(parsed.path.split('/')) > 1 else ''
    
    # Lấy keyword tìm kiếm
    query = parse_qs(parsed.query)
    if 'q' in query:
        params['q'] = query['q'][0]
    
    try:
        resp = requests.get(LISTINGS_URL, headers=HEADERS, params=params, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


def search_products(keyword, page=1, limit=40, sort=None):
    """Tìm kiếm sản phẩm"""
    params = {'limit': limit, 'include': 'advertisement', 'aggregations': 2, 'q': keyword, 'page': page}
    if sort:
        params['sort'] = sort
    try:
        resp = requests.get(LISTINGS_URL, headers=HEADERS, params=params, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


def get_product_detail(product_id):
    """Lấy chi tiết sản phẩm"""
    try:
        resp = requests.get(f"{PRODUCT_URL}/{product_id}", headers=HEADERS, 
                           params={'platform': 'web', 'spid': product_id}, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    except:
        return None


# ============================================================
# PARSER
# ============================================================
def parse_product(product):
    """Parse thông tin sản phẩm"""
    try:
        return {
            'id': product.get('id', ''),
            'name': product.get('name', ''),
            'price': product.get('price', 0),
            'original_price': product.get('original_price', 0),
            'discount_rate': product.get('discount_rate', 0),
            'rating_average': product.get('rating_average', 0),
            'review_count': product.get('review_count', 0),
            'quantity_sold': product.get('quantity_sold', {}).get('value', 0) if product.get('quantity_sold') else 0,
            'brand_name': product.get('brand_name', ''),
            'seller_name': product.get('seller_name', '') or (product.get('current_seller', {}).get('name', '') if product.get('current_seller') else ''),
            'thumbnail_url': product.get('thumbnail_url', ''),
            'url_key': product.get('url_key', ''),
        }
    except:
        return None


def extract_product_id(url):
    """Trích xuất product ID từ URL"""
    match = re.search(r'-p(\d+)', url)
    return int(match.group(1)) if match else None


# ============================================================
# STORAGE
# ============================================================
def save_to_db(products, filename, output_dir=None):
    """Lưu vào SQLite database"""
    if not products:
        return None
    
    import os
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
    else:
        filepath = filename
    
    df = pd.DataFrame(products)
    conn = sqlite3.connect(filepath)
    df.to_sql('products', conn, if_exists='replace', index=False)
    conn.close()
    print(f"✅ Đã lưu {len(products)} sản phẩm vào: {filepath}")
    return filepath


# ============================================================
# CRAWLER FUNCTIONS
# ============================================================
def crawl_category(category_id, category_name, max_pages):
    """Crawl một category"""
    products = []
    for page in range(1, max_pages + 1):
        print(f"  Trang {page}/{max_pages}...", end=" ")
        result = get_products_by_category(category_id, category_name, page)
        if result and 'data' in result and result['data']:
            for p in result['data']:
                parsed = parse_product(p)
                if parsed:
                    parsed['category'] = category_name
                    products.append(parsed)
            print(f"✓ {len(result['data'])} SP")
            time.sleep(1)
        else:
            print("Hết dữ liệu")
            break
    return products


def crawl_url(url, max_pages):
    """Crawl từ URL"""
    products = []
    for page in range(1, max_pages + 1):
        print(f"  Trang {page}/{max_pages}...", end=" ")
        result = get_products_by_url(url, page)
        if result and 'data' in result and result['data']:
            for p in result['data']:
                parsed = parse_product(p)
                if parsed:
                    products.append(parsed)
            print(f"✓ {len(result['data'])} SP")
            time.sleep(1)
        else:
            print("Hết dữ liệu")
            break
    return products


def crawl_search(keyword, max_pages, sort=None):
    """Crawl từ tìm kiếm"""
    products = []
    for page in range(1, max_pages + 1):
        print(f"  Trang {page}/{max_pages}...", end=" ")
        result = search_products(keyword, page, sort=sort)
        if result and 'data' in result and result['data']:
            for p in result['data']:
                parsed = parse_product(p)
                if parsed:
                    parsed['search_keyword'] = keyword
                    products.append(parsed)
            print(f"✓ {len(result['data'])} SP")
            time.sleep(1)
        else:
            print("Hết dữ liệu")
            break
    return products


# ============================================================
# ANALYZER
# ============================================================
def analyze(df):
    """Phân tích dữ liệu"""
    print("\n" + "="*50)
    print("📊 THỐNG KÊ")
    print("="*50)
    print(f"📦 Tổng SP: {len(df)}")
    if 'category' in df.columns:
        print(f"📁 Categories: {df['category'].nunique()}")
    print(f"🏷️ Brands: {df['brand_name'].nunique()}")
    print(f"\n💰 Giá: {df['price'].min():,.0f} - {df['price'].max():,.0f} VNĐ (TB: {df['price'].mean():,.0f})")
    
    print("\n⭐ Top 5 đánh giá cao:")
    top = df[df['rating_average'] > 0].nlargest(5, 'rating_average')[['name', 'rating_average', 'price']]
    for _, r in top.iterrows():
        print(f"  • {r['name'][:40]}... - {r['rating_average']}/5 - {r['price']:,.0f}đ")
    
    print("\n🔥 Top 5 bán chạy:")
    top = df.nlargest(5, 'quantity_sold')[['name', 'quantity_sold', 'price']]
    for _, r in top.iterrows():
        print(f"  • {r['name'][:40]}... - {r['quantity_sold']} đã bán - {r['price']:,.0f}đ")


# ============================================================
# MAIN
# ============================================================
def main():
    print("="*50)
    print("🕷️ TIKI CRAWLER")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)
    
    products = []
    
    # MODE: category
    if MODE == 'category':
        print(f"\n📁 MODE: Crawl theo danh mục")
        cats = CATEGORIES if CATEGORIES else list(ELECTRONICS_CATEGORIES.keys())
        for i, cat in enumerate(cats, 1):
            if cat in ELECTRONICS_CATEGORIES:
                print(f"\n[{i}/{len(cats)}] {cat}")
                products.extend(crawl_category(ELECTRONICS_CATEGORIES[cat], cat, MAX_PAGES))
                time.sleep(2)
    
    # MODE: url
    elif MODE == 'url':
        print(f"\n🔗 MODE: Crawl từ URL")
        print(f"URL: {URL}")
        products = crawl_url(URL, MAX_PAGES)
    
    # MODE: search
    elif MODE == 'search':
        print(f"\n🔍 MODE: Tìm kiếm")
        keywords = SEARCH_KEYWORDS if isinstance(SEARCH_KEYWORDS, list) else [SEARCH_KEYWORDS]
        for kw in keywords:
            print(f"\nTừ khóa: '{kw}'")
            products.extend(crawl_search(kw, MAX_PAGES, SORT_BY))
            time.sleep(2)
    
    # MODE: product
    elif MODE == 'product':
        print(f"\n📦 MODE: Chi tiết sản phẩm")
        for item in PRODUCT_IDS:
            pid = extract_product_id(item) if isinstance(item, str) and 'tiki.vn' in item else item
            if pid:
                detail = get_product_detail(pid)
                if detail:
                    products.append(parse_product(detail))
                    print(f"✓ {detail.get('name', '')[:50]}...")
    
    # Kết quả
    if products:
        print(f"\n📦 TỔNG: {len(products)} sản phẩm")
        
        if SAVE_DATA:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            save_to_db(products, f'tiki_{timestamp}.db', OUTPUT_DIR)
        
        if ANALYZE_DATA:
            analyze(pd.DataFrame(products))
        
        print("\n✅ HOÀN THÀNH!")
    else:
        print("\n❌ Không có dữ liệu!")


if __name__ == "__main__":
    main()
