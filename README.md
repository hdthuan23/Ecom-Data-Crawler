# Ecom-Data-Crawler

Pipeline toi gian:

1. Crawl
2. Hieu chinh du lieu va tao CSV processed
3. Phan tich bang 1 file

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

### 2) Hieu chinh du lieu

```bash
python preprocessing/verify_pipeline.py
```

Output:
- `data/tiki_electronics_2026_processed.csv`
- `data/preprocessing_report.json`

### 3) Phan tich (1 file)

```bash
python analysis/report.py
```

Output:
- Bieu do luu trong `analysis/charts/`
- `report.py` tu dong chay tiep cac muc con lai

## Luong du lieu

`raw csv -> processed csv -> analysis`

## Ghi chu

- Nhom brand su dung trong phan tich:
  - `Global_Brand`
  - `Local/OEM Generic`
