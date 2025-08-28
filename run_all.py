#!/usr/bin/env python3
"""
Run-all pipeline:
- (Optional) Rebuild DW by running etl.py if --rebuild is passed
- Execute ALL KPI queries (defined here) against the DW
- Save each KPI result as CSV (and one consolidated Excel) under kpi/out/
- Generate charts (PNG + HTML) by calling kpi/visualizations.py

Usage:
  python run_all.py --rebuild
"""

import argparse, sqlite3, subprocess, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent
DB_DEFAULT = ROOT / "dw" / "dw_hiring.db"
CSV_DEFAULT = ROOT / "data" / "candidates.csv"
SCHEMA_DEFAULT = ROOT / "dw" / "schema.sql"

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true", help="Run etl.py before KPIs")
    ap.add_argument("--csv", default=str(CSV_DEFAULT))
    ap.add_argument("--db", default=str(DB_DEFAULT))
    ap.add_argument("--schema", default=str(SCHEMA_DEFAULT))
    return ap.parse_args()

# KPI SQL (names → queries)
KPI_SQL = {
    "hires_by_technology": """
SELECT t.technology,
       SUM(f.hired) AS hires,
       COUNT(*) AS total_candidates,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimTechnology t USING(technology_id)
GROUP BY t.technology
ORDER BY hires DESC;""",

    "hires_by_year": """
SELECT d.year,
       SUM(f.hired) AS hires,
       COUNT(*) AS total_candidates,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimDate d USING(date_id)
GROUP BY d.year
ORDER BY d.year;""",

    "hires_by_seniority": """
SELECT s.seniority,
       SUM(f.hired) AS hires,
       COUNT(*) AS total_candidates,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimSeniority s USING(seniority_id)
GROUP BY s.seniority
ORDER BY hires DESC;""",

    "hires_by_country_over_years_focus": """
SELECT d.year, c.country, SUM(f.hired) AS hires
FROM FactHiring f
JOIN DimDate d USING(date_id)
JOIN DimCountry c USING(country_id)
WHERE c.country IN ('United States','Brazil','Colombia','Ecuador')
GROUP BY d.year, c.country
ORDER BY d.year, hires DESC;""",

    "avg_scores_by_hired": """
SELECT hired,
       ROUND(AVG(code_challenge_score), 2) AS avg_code_challenge,
       ROUND(AVG(technical_interview_score), 2) AS avg_tech_interview
FROM FactHiring
GROUP BY hired
ORDER BY hired DESC;"""
}

def run_etl_if_requested(args):
    if not args.rebuild:
        return
    etl = ROOT / "etl.py"
    if not etl.exists():
        print("[WARN] etl.py not found; skipping rebuild.")
        return
    cmd = [sys.executable, str(etl), "--csv", args.csv, "--db", args.db, "--schema", args.schema]
    print("[RUN]", " ".join(str(x) for x in cmd))
    subprocess.run(cmd, check=True)

def run_kpis(db_path: Path) -> dict:
    out_dir = ROOT / "kpi" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    with sqlite3.connect(db_path) as con:
        for name, sql in KPI_SQL.items():
            df = pd.read_sql_query(sql, con)
            df.to_csv(out_dir / f"{name}.csv", index=False)
            results[name] = df
    # Save all as Excel too
    xlsx = out_dir / "kpis.xlsx"
    with pd.ExcelWriter(xlsx, engine="xlsxwriter") as writer:
        for name, df in results.items():
            sheet = name[:31]
            df.to_excel(writer, sheet_name=sheet, index=False)
    print(f"[OK] KPIs saved in {out_dir}")
    return results

def run_charts():
    viz = ROOT / "kpi" / "visualizations.py"
    if viz.exists():
        cmd = [sys.executable, str(viz)]
        print("[RUN]", " ".join(str(x) for x in cmd))
        subprocess.run(cmd, check=True)
    else:
        print("[WARN] visualizations.py not found; skipping charts.")

def main():
    args = parse_args()
    run_etl_if_requested(args)
    run_kpis(Path(args.db))
    run_charts()
    print("[DONE] All queries executed and charts saved.")

if __name__ == "__main__":
    main()
