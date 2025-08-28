# ETL Workshop-1 — Data Engineer (Python + SQLite)

> **Goal:** Build an ETL pipeline that reads a 50k-row CSV, applies the **HIRED** rule (both scores ≥ 7), loads into a **Data Warehouse** with a **star schema**, and produces **KPIs + charts** querying the **DW (not the CSV)**.

## Project Layout

```
etl_workshop/
├── data/candidates.csv
├── dw/schema.sql
├── dw/dw_hiring.db
├── etl.py
├── kpi/queries.sql
├── kpi/visualizations.py (optional)
├── requirements.txt
└── .gitignore
```

## Requirements

- Python 3.10+
- `pip install -r requirements.txt`
- Note: Jupyter is **not required**. This repo uses **scripts**.

## How to Run

1. Place the CSV at `data/candidates.csv` (`;` separator).
2. Build the DW and load data:
   ```bash
   python etl.py --csv data/candidates.csv --db dw/dw_hiring.db --schema dw/schema.sql
   ```
3. Run KPIs (against the DW):
   ```bash
   sqlite3 dw/dw_hiring.db < kpi/queries.sql
   ```
4. (Optional) Generate charts:
   ```bash
   python kpi/visualizations.py
   ```

## Star Schema

Dimensions: `DimDate`, `DimTechnology`, `DimSeniority`, `DimCountry`, `DimCandidate`  
Fact: `FactHiring(hired, scores, yoe, FKs)`

## Quick Validation

- `sqlite3 dw/dw_hiring.db "SELECT COUNT(*) FROM FactHiring;"`
- `sqlite3 dw/dw_hiring.db "SELECT hired, COUNT(*) FROM FactHiring GROUP BY hired;"`
