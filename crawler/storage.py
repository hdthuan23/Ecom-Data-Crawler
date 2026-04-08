"""CSV-first storage: only in-memory processing + CSV export."""

import csv
import logging
import os
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVStorage:
    """
    Luu du lieu trong RAM trong suot mot lan chay crawl,
    sau do export truc tiep ra CSV.
    """

    def __init__(self):
        self.categories = {}
        self.products = {}
        self.crawl_runs = {}
        logger.info("CSV-only storage initialized (in-memory mode).")

    def _now(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def save_categories(self, categories, parent_id):
        for cat in categories:
            cid = cat["id"]
            if cid not in self.categories:
                self.categories[cid] = {
                    "category_id": cid,
                    "category_name": cat["name"],
                    "parent_id": parent_id,
                    "discovered_at": self._now(),
                }
        logger.debug("Saved %d categories (parent: %s)", len(categories), parent_id)

    def upsert_products(self, products, run_id):
        new_count = 0
        updated_count = 0
        now = self._now()

        for p in products:
            pid = p["product_id"]
            existing = self.products.get(pid)
            if existing:
                updated_count += 1
                first_crawled_at = existing["first_crawled_at"]
            else:
                new_count += 1
                first_crawled_at = now

            self.products[pid] = {
                "product_id": p.get("product_id"),
                "product_name": p.get("product_name"),
                "category_id": p.get("category_id"),
                "category_name": p.get("category_name"),
                "brand_name": p.get("brand_name"),
                "brand_type": p.get("brand_type"),
                "price": p.get("price"),
                "original_price": p.get("original_price"),
                "discount_rate": p.get("discount_rate"),
                "rating_average": p.get("rating_average"),
                "review_count": p.get("review_count"),
                "quantity_sold": p.get("quantity_sold"),
                "seller_name": p.get("seller_name"),
                "is_tiki_trading": p.get("is_tiki_trading", 0),
                "first_crawled_at": first_crawled_at,
                "last_crawled_at": now,
                "crawl_run_id": run_id,
            }

        logger.info(
            "Upserted %d products (New: %d, Updated: %d)",
            len(products),
            new_count,
            updated_count,
        )
        return new_count, updated_count

    def start_run(self, run_id):
        self.crawl_runs[run_id] = {
            "run_id": run_id,
            "start_time": self._now(),
            "end_time": None,
            "categories_crawled": 0,
            "products_found": 0,
            "products_new": 0,
            "products_updated": 0,
            "status": "running",
        }
        logger.info("Crawl run started: %s", run_id)

    def finish_run(self, run_id, categories_crawled, products_found, products_new, products_updated):
        run = self.crawl_runs.get(run_id)
        if not run:
            return
        run["end_time"] = self._now()
        run["categories_crawled"] = categories_crawled
        run["products_found"] = products_found
        run["products_new"] = products_new
        run["products_updated"] = products_updated
        run["status"] = "completed"
        logger.info("Crawl run completed: %s", run_id)

    def export_to_csv(self, csv_path):
        csv_dir = os.path.dirname(csv_path)
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)

        columns = [
            "product_id",
            "product_name",
            "category_id",
            "category_name",
            "brand_name",
            "brand_type",
            "brand_group",
            "price",
            "original_price",
            "discount_rate",
            "rating_average",
            "review_count",
            "quantity_sold",
            "seller_name",
            "is_tiki_trading",
            "first_crawled_at",
            "last_crawled_at",
        ]

        def brand_group(bt):
            return "Global Brand" if bt == "Global_Brand" else "Local/OEM Generic"

        rows = []
        for p in self.products.values():
            rows.append([
                p.get("product_id"),
                p.get("product_name"),
                p.get("category_id"),
                p.get("category_name"),
                p.get("brand_name"),
                p.get("brand_type"),
                brand_group(p.get("brand_type")),
                p.get("price"),
                p.get("original_price"),
                p.get("discount_rate"),
                p.get("rating_average"),
                p.get("review_count"),
                p.get("quantity_sold"),
                p.get("seller_name"),
                p.get("is_tiki_trading"),
                p.get("first_crawled_at"),
                p.get("last_crawled_at"),
            ])

        rows.sort(key=lambda r: (r[6], r[2], -(r[7] or 0)))

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info("Exported %d products -> %s", len(rows), csv_path)
        return len(rows)

    def get_stats(self):
        products = list(self.products.values())
        total_products = len(products)
        by_brand_type = Counter(p.get("brand_type") for p in products if p.get("brand_type"))
        unique_categories = len({p.get("category_id") for p in products if p.get("category_id") is not None})
        unique_brands = len({p.get("brand_name") for p in products if p.get("brand_name")})

        avg_price_by_type = {}
        avg_rating_by_type = {}
        for bt in by_brand_type:
            sub = [p for p in products if p.get("brand_type") == bt]
            prices = [p.get("price") for p in sub if isinstance(p.get("price"), (int, float))]
            ratings = [p.get("rating_average") for p in sub if isinstance(p.get("rating_average"), (int, float)) and p.get("rating_average") > 0]
            avg_price_by_type[bt] = round(sum(prices) / len(prices), 0) if prices else 0
            avg_rating_by_type[bt] = round(sum(ratings) / len(ratings), 2) if ratings else 0

        return {
            "total_products": total_products,
            "by_brand_type": dict(by_brand_type),
            "unique_categories": unique_categories,
            "unique_brands": unique_brands,
            "total_runs": len(self.crawl_runs),
            "avg_price_by_type": avg_price_by_type,
            "avg_rating_by_type": avg_rating_by_type,
        }

    def get_run_history(self):
        runs = list(self.crawl_runs.values())
        runs.sort(key=lambda r: r.get("start_time") or "", reverse=True)
        return runs

    def close(self):
        logger.info("CSV-only storage closed.")
