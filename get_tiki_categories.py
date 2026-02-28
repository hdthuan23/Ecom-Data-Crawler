import requests
import json

def fetch_tiki_categories():
    # 1846: Laptop - Máy vi tính - Linh kiện | 1789: Điện thoại - Máy tính bảng
    root_ids = [1846, 1789]
    
    # Bổ sung Headers chi tiết hơn để "ngụy trang" tốt hơn
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://tiki.vn/"
    }
    
    target_parents = []

    print("Đang kết nối đến API Tiki...")
    for root_id in root_ids:
        # Thay đổi Endpoint: Dùng ?parent_id= thay vì gọi trực tiếp ID
        url = f"https://tiki.vn/api/v2/categories?parent_id={root_id}"
        print(f"-> Đang quét URL: {url}")
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # API trả về list trực tiếp hoặc bọc trong 'data'
            children = []
            if isinstance(data, list):
                children = data
            elif isinstance(data, dict) and "data" in data:
                children = data["data"]
            elif isinstance(data, dict) and "children" in data:
                children = data["children"]
            else:
                print(f"⚠️ Cảnh báo: Cấu trúc JSON lạ ở ID {root_id}. Xem dữ liệu thô:")
                print(json.dumps(data, indent=2, ensure_ascii=False)[:500]) # In ra 500 ký tự đầu để bắt bệnh
                
            for child in children:
                target_parents.append({
                    "id": child.get("id"),
                    "name": child.get("name"),
                    "description": f"Nhóm con của ID gốc: {root_id}"
                })
        else:
            print(f"❌ Lỗi khi lấy ID {root_id}: Mã lỗi {response.status_code}")

    if len(target_parents) > 0:
        # Ghi ra file JSON
        with open("tiki_categories_output.json", "w", encoding="utf-8") as f:
            json.dump(target_parents, f, ensure_ascii=False, indent=2)
        print(f"✅ Xong! Đã quét được {len(target_parents)} danh mục. Mở file 'tiki_categories_output.json' để kiểm tra.")
    else:
        print("❌ Vẫn không lấy được danh mục nào. Hãy kiểm tra lại dữ liệu thô (raw data) in ra ở trên.")

# Chạy tool
fetch_tiki_categories()