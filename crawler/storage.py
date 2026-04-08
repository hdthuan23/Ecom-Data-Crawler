"""
CSV-first Storage - in-memory snapshot for crawl and SQL sync.
"""

import logging
import os
import csv
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVStorage:
    """Store unique products by product_id and export raw snapshot CSV."""

    def __init__(self, db_path):
        self.db_path = db_path  # Giu lai de tuong thich config va log
        self.products = {}
        logger.info(
            "CSV-only storage initialized (in-memory mode, no .db file): %s",
            db_path,
        )

    def _now(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def save_categories(self, categories, parent_id):
        # Category persistence is not required. Categories are derived from products for SQL sync.
        _ = categories, parent_id

    def upsert_products(self, products, run_id):
        new_count = 0
        updated_count = 0
        now = self._now()

        for p in products:
            pid = p["product_id"]
            existing = self.products.get(pid)
            if existing:
                updated_count += 1
            else:
                new_count += 1

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
            "price",
            "original_price",
            "discount_rate",
            "rating_average",
            "review_count",
            "quantity_sold",
            "seller_name",
            "is_tiki_trading",
            "last_crawled_at",
        ]

        rows = []
        for p in self.products.values():
            rows.append([
                p.get("product_id"),
                p.get("product_name"),
                p.get("category_id"),
                p.get("category_name"),
                p.get("brand_name"),
                p.get("brand_type"),
                p.get("price"),
                p.get("original_price"),
                p.get("discount_rate"),
                p.get("rating_average"),
                p.get("review_count"),
                p.get("quantity_sold"),
                p.get("seller_name"),
                p.get("is_tiki_trading"),
                p.get("last_crawled_at"),
            ])

        rows.sort(key=lambda r: (r[5], r[2], -(r[6] or 0)))

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info("Exported %d products -> %s", len(rows), csv_path)
        return len(rows)

    def get_all_products(self):
        """Return unique product snapshot by product_id."""
        return list(self.products.values())

    def close(self):
        logger.info("CSV-only storage closed.")

