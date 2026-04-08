# Ecom-Data-Crawler

Pipeline toi gian:

1. Crawl du lieu tu Tiki
2. Tao snapshot CSV raw
3. Dong bo vao SQL Server (schema Products/Categories/Brands/Sellers)

## Cai dat

```bash
pip install -r requirements.txt
```

## Cach chay

### 1) Crawl du lieu

```bash
python main.py
```

Output:
- `data/tiki_electronics_2026.csv`

## Luong du lieu

`crawl -> raw csv -> sql server snapshot`

## Ghi chu

- SQL Server schema muc tieu:
  - `dbo.Categories`
  - `dbo.Brands`
  - `dbo.Sellers`
  - `dbo.Products`