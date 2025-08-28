#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL for Workshop-1 (enhanced with Rich logs and extra normalization)
- Extract: reads data/candidates.csv (';' separator)
- Transform: normalize columns, cast types, compute HIRED flag (>=7 on both scores), clean countries, add experience bands
- Load: create SQLite DW (star schema) and load dimensions + fact

Usage:
  python etl.py --csv data/candidates.csv --db dw/dw_hiring.db --schema dw/schema.sql
"""
from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.panel import Panel

console = Console()

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', default='data/candidates.csv', help='Path to source CSV')
    ap.add_argument('--db',  default='dw/dw_hiring.db',      help='Path to SQLite DW')
    ap.add_argument('--schema', default='dw/schema.sql',     help='Path to DW DDL file')
    return ap.parse_args()

def ensure_dirs(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

COUNTRY_FIX = {
    'usa': 'United States', 'us': 'United States', 'u.s.a.': 'United States',
    'united states of america': 'United States', 'ee.uu.': 'United States',
    'uk': 'United Kingdom', 'u.k.': 'United Kingdom',
    'brasil': 'Brazil'
}

HIRED_THRESHOLD = 7

def extract(csv_path: Path) -> pd.DataFrame:
    console.rule("[bold cyan]1/6 Extract")
    console.print(f"Reading CSV: [bold]{csv_path}[/bold]")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found at {csv_path}")
    df = pd.read_csv(csv_path, sep=';')
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    console.rule("[bold cyan]2/6 Transform")
    console.print("Casting types and creating derived columns…")

    # Types
    df['application_date'] = pd.to_datetime(df['application_date'], errors='coerce').dt.date.astype(str)
    df['yoe'] = pd.to_numeric(df['yoe'], errors='coerce').fillna(0).astype(int)
    df['code_challenge_score'] = pd.to_numeric(df['code_challenge_score'], errors='coerce')
    df['technical_interview_score'] = pd.to_numeric(df['technical_interview_score'], errors='coerce')

    # Basic cleanup
    for col in ['first_name', 'last_name', 'email', 'country', 'seniority', 'technology']:
        df[col] = df[col].astype(str).str.strip()

    # Country normalization
    df['country'] = df['country'].str.lower().map(COUNTRY_FIX).fillna(df['country'].str.title())

    # HIRED rule
    df['hired'] = ((df['code_challenge_score'] >= HIRED_THRESHOLD) &
                   (df['technical_interview_score'] >= HIRED_THRESHOLD)).astype(int)

    # Date key
    df['date_id'] = df['application_date'].replace('-', '', regex=True).astype(int)

    # Experience band (for analysis; not loaded into DW)
    bins = [-1, 2, 5, 10, 10**6]
    labels = ['0-2', '3-5', '6-10', '11+']
    df['yoe_band'] = pd.cut(df['yoe'], bins=bins, labels=labels)

    return df

def load_to_dw(df: pd.DataFrame, db_path: Path, schema_path: Path) -> None:
    console.rule("[bold cyan]3/6 Load → DW")
    console.print(f"Connecting to SQLite at [bold]{db_path}[/bold]")
    ensure_dirs(db_path)
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        # 3.1 Create schema
        console.print(f"Applying DDL: [italic]{schema_path}[/italic]")
        ddl = Path(schema_path).read_text(encoding='utf-8')
        cur.executescript(ddl)

        # 3.2 DimDate
        console.print("Loading DimDate / other dimensions…")
        dates = (
            df[['date_id', 'application_date']]
            .drop_duplicates()
            .rename(columns={'application_date': 'full_date'})
        )
        dates['year'] = dates['full_date'].str.slice(0, 4).astype(int)
        dates['month'] = dates['full_date'].str.slice(5, 7).astype(int)
        dates['day'] = dates['full_date'].str.slice(8, 10).astype(int)
        dates['quarter'] = ((dates['month'] - 1) // 3 + 1).astype(int)
        month_names = ['January','February','March','April','May','June','July','August','September','October','November','December']
        dates['month_name'] = dates['month'].apply(lambda m: month_names[m-1])
        cur.executemany(
            """            INSERT OR IGNORE INTO DimDate(date_id, full_date, day, month, month_name, quarter, year)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,            dates[['date_id','full_date','day','month','month_name','quarter','year']].itertuples(index=False)
        )

        # Other dimensions from unique values
        def insert_unique(table: str, column: str, values):
            cur.executemany(
                f"INSERT OR IGNORE INTO {table}({column}) VALUES (?)",
                [(str(v),) for v in sorted(pd.Series(values).dropna().unique().tolist())]
            )

        insert_unique('DimTechnology', 'technology', df['technology'])
        insert_unique('DimSeniority', 'seniority',  df['seniority'])
        insert_unique('DimCountry',    'country',    df['country'])

        # DimCandidate (unique by email)
        candidates = df[['first_name','last_name','email']].drop_duplicates('email')
        cur.executemany(
            """            INSERT OR IGNORE INTO DimCandidate(first_name, last_name, email)
            VALUES (?, ?, ?)
            """,            candidates.itertuples(index=False, name=None)
        )

        # ID maps
        def build_map(sql: str) -> dict:
            return {row[1]: row[0] for row in cur.execute(sql).fetchall()}

        map_tech = build_map("SELECT technology_id, technology FROM DimTechnology")
        map_sen  = build_map("SELECT seniority_id, seniority FROM DimSeniority")
        map_cty  = build_map("SELECT country_id, country FROM DimCountry")
        map_cnd  = build_map("SELECT candidate_id, email FROM DimCandidate")

        # Fact
        console.print("Inserting rows into FactHiring…")
        rows = []
        for r in df.itertuples(index=False):
            rows.append((
                map_cnd[r.email],
                map_tech[r.technology],
                map_sen[r.seniority],
                map_cty[r.country],
                r.date_id,
                int(r.yoe),
                float(r.code_challenge_score),
                float(r.technical_interview_score),
                int(r.hired)
            ))
        cur.executemany(
            """            INSERT INTO FactHiring(
                candidate_id, technology_id, seniority_id, country_id, date_id,
                yoe, code_challenge_score, technical_interview_score, hired)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,            rows
        )
        con.commit()
        console.print(Panel.fit(f"Load complete: {len(rows)} rows inserted into FactHiring"))

def main():
    args = parse_args()
    csv_path = Path(args.csv)
    db_path = Path(args.db)
    schema_path = Path(args.schema)

    df = extract(csv_path)
    df = transform(df)
    load_to_dw(df, db_path, schema_path)

if __name__ == '__main__':
    main()
