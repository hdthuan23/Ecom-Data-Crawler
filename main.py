"""
Tiki Electronics Data Crawler - Main Orchestrator
==================================================

Entry point điều phối toàn bộ quy trình crawl:

  PHASE 1: Deep Category Discovery
    -> Đọc target_parents từ config.json
    -> Đệ quy tìm tất cả leaf categories (sub-category sâu nhất)

  PHASE 2: Crawl Products & Real-time Classification
    -> Duyệt từng leaf category
    -> Gọi API products + pagination
    -> Phân loại brand_type ngay (Global/OEM/Local)
    -> UPSERT vào SQLite (chống trùng lặp)

  PHASE 3: Finalize & Export
    -> Export CSV cho PowerBI/Tableau
    -> In thống kê tổng hợp

Cách sử dụng:
  python main.py                    # Dùng config.json mặc định
  python main.py custom_config.json # Dùng config tùy chỉnh
  python main.py --export-only      # Chỉ export CSV (không crawl)
  python main.py --stats            # Chỉ xem thống kê
"""

import json
import logging
import os
import sys
from datetime import datetime

from crawler.category_mapper import discover_leaf_categories
from crawler.brand_classifier import BrandClassifier
from crawler.scraper import TikiScraper, DEFAULT_HEADERS
from crawler.storage import SQLiteStorage


def setup_logging():
    """
    Cấu hình logging: ghi ra cả console và file log.
    File log lưu theo timestamp để không bị ghi đè.
    """
    os.makedirs("logs", exist_ok=True)

    log_filename = f"logs/crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    return log_filename


def load_config(filepath="config.json"):
    """
    Load file cấu hình JSON.

    Args:
        filepath (str): Đường dẫn đến file config

    Returns:
        dict: Nội dung config đã parse
    """
    if not os.path.exists(filepath):
        print(f"ERROR: Không tìm thấy file config: {filepath}")
        print("Hãy tạo file config.json theo hướng dẫn trong README.md")
        sys.exit(1)

    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def show_stats(config):
    """Hiển thị thống kê database hiện tại (không crawl)."""
    output = config["output"]
    db_path = output["sqlite_db"]

    if not os.path.exists(db_path):
        print(f"Database chưa tồn tại: {db_path}")
        print("Hãy chạy crawl trước: python main.py")
        return

    storage = SQLiteStorage(db_path)
    stats = storage.get_stats()

    print("\n" + "=" * 60)
    print("THỐNG KÊ DATABASE HIỆN TẠI")
    print("=" * 60)
    print(f"Tổng sản phẩm: {stats['total_products']:,}")
    print(f"Danh mục: {stats['unique_categories']}")
    print(f"Thương hiệu: {stats['unique_brands']}")
    print(f"Số lần crawl: {stats['total_runs']}")
    print()
    print("Phân bố Brand Type:")
    for bt, count in stats.get("by_brand_type", {}).items():
        print(f"  {bt}: {count:,}")
    print()
    print("Giá trung bình theo Brand Type:")
    for bt, avg in stats.get("avg_price_by_type", {}).items():
        print(f"  {bt}: {int(avg):,}đ")
    print()
    print("Rating trung bình theo Brand Type:")
    for bt, avg in stats.get("avg_rating_by_type", {}).items():
        print(f"  {bt}: {avg}")

    # Hiển thị lịch sử crawl
    runs = storage.get_run_history()
    if runs:
        print()
        print("Lịch sử Crawl Runs:")
        for run in runs[:5]:  # Hiập 5 lần gần nhất
            print(
                f"  [{run['run_id']}] "
                f"{run['status']} | "
                f"Found: {run['products_found']} | "
                f"New: {run['products_new']} | "
                f"Updated: {run['products_updated']}"
            )

    print("=" * 60)
    storage.close()


def export_only(config):
    """Chỉ export CSV từ database hiện tại (không crawl)."""
    output = config["output"]
    db_path = output["sqlite_db"]
    csv_path = output["csv_export"]

    if not os.path.exists(db_path):
        print(f"Database chưa tồn tại: {db_path}")
        return

    storage = SQLiteStorage(db_path)
    count = storage.export_to_csv(csv_path)
    print(f"Đã export {count:,} sản phẩm -> {csv_path}")
    storage.close()


def main():
    """Main entry point - Điều phối toàn bộ quy trình."""
    log_file = setup_logging()
    logger = logging.getLogger("main")

    # ========== PARSE ARGUMENTS ==========
    args = sys.argv[1:]

    # Xác định file config
    config_path = "config.json"
    for arg in args:
        if not arg.startswith("--") and arg.endswith(".json"):
            config_path = arg
            break

    logger.info(f"Loading config: {config_path}")
    config = load_config(config_path)

    # Xử lý các mode đặc biệt
    if "--stats" in args:
        show_stats(config)
        return

    if "--export-only" in args:
        export_only(config)
        return

    # ========== KHỞI TẠO CÁC COMPONENT ==========
    settings = config["scraping_settings"]
    output = config["output"]

    # Unique run ID (timestamp-based)
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logger.info("=" * 60)
    logger.info(f"PROJECT: {config.get('project', {}).get('name', 'Tiki Crawler')}")
    logger.info(f"RUN ID:  {run_id}")
    logger.info(f"LOG:     {log_file}")
    logger.info("=" * 60)

    # Khởi tạo Brand Classifier
    classifier = BrandClassifier(
        global_brands_list=config["global_brands_dict"],
        oem_indicators=config.get("oem_indicators"),
    )

    # Khởi tạo Scraper
    scraper = TikiScraper(settings, classifier)

    # Khởi tạo Storage (SQLite)
    storage = SQLiteStorage(output["sqlite_db"])
    storage.start_run(run_id)

    try:
        # ==========================================
        # PHASE 1: DEEP CATEGORY DISCOVERY
        # ==========================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 1: Dò tìm tất cả Leaf Categories (Sub-category sâu nhất)")
        logger.info("=" * 60)

        all_leaf_categories = []

        for parent in config["target_parents"]:
            parent_id = parent["id"]
            parent_name = parent["name"]
            parent_desc = parent.get("description", "")

            logger.info(f"")
            logger.info(
                f"Scanning: {parent_name} (ID: {parent_id}) - {parent_desc}"
            )

            leaves = discover_leaf_categories(
                parent_id=parent_id,
                headers=DEFAULT_HEADERS,
                delay=settings.get("delay_between_requests_sec", 2.0),
            )

            # Lưu metadata categories vào DB
            storage.save_categories(leaves, parent_id)
            all_leaf_categories.extend(leaves)

        # De-duplicate categories (loại bỏ ID trùng)
        seen_ids = set()
        unique_categories = []
        for cat in all_leaf_categories:
            if cat["id"] not in seen_ids:
                seen_ids.add(cat["id"])
                unique_categories.append(cat)

        logger.info("")
        logger.info(
            f"TỔNG CỘNG: {len(unique_categories)} leaf categories "
            f"(đã loại trùng từ {len(all_leaf_categories)})"
        )
        for cat in unique_categories:
            logger.info(f"  [{cat['id']:>6}] {cat['name']}")

        # ==========================================
        # PHASE 2: CRAWL & CLASSIFY REAL-TIME
        # ==========================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 2: Crawl sản phẩm + Phân loại Brand Type real-time")
        logger.info("=" * 60)

        total_found = 0
        total_new = 0
        total_updated = 0

        for i, cat in enumerate(unique_categories, 1):
            logger.info("")
            logger.info(
                f"[{i}/{len(unique_categories)}] "
                f"Crawling: {cat['name']} (ID: {cat['id']})"
            )

            # Crawl tất cả trang của category này
            products = scraper.scrape_category(cat["id"], cat["name"])

            if products:
                # UPSERT vào database (chống trùng lặp tuyệt đối)
                new_count, updated_count = storage.upsert_products(
                    products, run_id
                )
                total_found += len(products)
                total_new += new_count
                total_updated += updated_count

                logger.info(
                    f"  -> Tìm thấy: {len(products)} | "
                    f"Mới: {new_count} | Cập nhật: {updated_count}"
                )
            else:
                logger.info("  -> Không có sản phẩm")

        # ==========================================
        # PHASE 3: FINALIZE & EXPORT
        # ==========================================
        logger.info("")
        logger.info("=" * 60)
        logger.info("PHASE 3: Lưu trữ & Export dữ liệu")
        logger.info("=" * 60)

        # Cập nhật trạng thái phiên crawl
        storage.finish_run(
            run_id, len(unique_categories),
            total_found, total_new, total_updated
        )

        # Export ra CSV
        csv_count = storage.export_to_csv(output["csv_export"])

        # ========== IN BÁO CÁO TỔNG HỢP ==========
        stats = storage.get_stats()

        logger.info("")
        logger.info("=" * 60)
        logger.info("CRAWL HOÀN THÀNH!")
        logger.info("=" * 60)
        logger.info(f"Run ID:             {run_id}")
        logger.info(f"Categories crawled: {len(unique_categories)}")
        logger.info(
            f"Sản phẩm lần này:   {total_found:,} "
            f"(Mới: {total_new:,} | Cập nhật: {total_updated:,})"
        )
        logger.info(f"Tổng trong DB:      {stats['total_products']:,}")
        logger.info(f"Thương hiệu:        {stats['unique_brands']}")
        logger.info(f"")
        logger.info("Phân bố Brand Type:")
        for bt, count in stats.get("by_brand_type", {}).items():
            pct = (count / stats["total_products"] * 100
                   if stats["total_products"] > 0 else 0)
            logger.info(f"  {bt:20s}: {count:>6,} ({pct:.1f}%)")

        logger.info(f"")
        logger.info(f"Output files:")
        logger.info(f"  SQLite: {output['sqlite_db']}")
        logger.info(f"  CSV:    {output['csv_export']} ({csv_count:,} records)")
        logger.info(f"  Log:    {log_file}")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Crawl bị dừng bởi người dùng (Ctrl+C)")
        storage.finish_run(
            run_id, 0, total_found, total_new, total_updated
        )
    except Exception as e:
        logger.error(f"Crawl thất bại: {e}", exc_info=True)
        raise
    finally:
        storage.close()


if __name__ == "__main__":
    main()
