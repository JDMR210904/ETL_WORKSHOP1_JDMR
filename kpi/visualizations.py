#!/usr/bin/env python3
# Generates charts from the DW:
# - PNGs (matplotlib): technology, year, seniority, avg_scores_by_hired, yoe_band
# - HTML (plotly): country over years, avg_scores_by_hired, yoe_band
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt

DB = Path('dw/dw_hiring.db')
OUT_PNG = Path('visuals')
OUT_HTML = Path('docs')
OUT_PNG.mkdir(parents=True, exist_ok=True)
OUT_HTML.mkdir(parents=True, exist_ok=True)

SQL_TECH = """
SELECT t.technology AS label, SUM(f.hired) AS hires
FROM FactHiring f
JOIN DimTechnology t USING(technology_id)
GROUP BY t.technology
ORDER BY hires DESC
"""

SQL_YEAR = """
SELECT d.year AS label, SUM(f.hired) AS hires
FROM FactHiring f
JOIN DimDate d USING(date_id)
GROUP BY d.year
ORDER BY d.year
"""

SQL_SENIORITY = """
SELECT s.seniority AS label, SUM(f.hired) AS hires
FROM FactHiring f
JOIN DimSeniority s USING(seniority_id)
GROUP BY s.seniority
ORDER BY hires DESC
"""

SQL_COUNTRY_YEAR = """
SELECT d.year, c.country, SUM(f.hired) AS hires
FROM FactHiring f
JOIN DimDate d USING(date_id)
JOIN DimCountry c USING(country_id)
WHERE c.country IN ('United States','Brazil','Colombia','Ecuador')
GROUP BY d.year, c.country
ORDER BY d.year, hires DESC
"""

SQL_AVG_SCORES_BY_HIRED = """
SELECT CASE WHEN hired=1 THEN 'Hired' ELSE 'Not hired' END AS label,
       ROUND(AVG(code_challenge_score), 2) AS avg_code_challenge,
       ROUND(AVG(technical_interview_score), 2) AS avg_tech_interview
FROM FactHiring
GROUP BY hired
ORDER BY hired DESC
"""

# YOE band computed on the fly (same bins as ETL: 0-2, 3-5, 6-10, 11+)
SQL_HIRES_BY_YOE_BAND = """
SELECT CASE
         WHEN f.yoe < 3 THEN '0-2'
         WHEN f.yoe BETWEEN 3 AND 5 THEN '3-5'
         WHEN f.yoe BETWEEN 6 AND 10 THEN '6-10'
         ELSE '11+'
       END AS label,
       SUM(f.hired) AS hires
FROM FactHiring f
GROUP BY label
ORDER BY CASE label
           WHEN '0-2' THEN 1
           WHEN '3-5' THEN 2
           WHEN '6-10' THEN 3
           ELSE 4
         END
"""

def save_bar_png(labels, values, title, filename, xlabel=None, ylabel=None, rotate_x=True):
    plt.figure()
    plt.title(title)
    plt.bar(labels, values)
    if rotate_x:
        plt.xticks(rotation=45, ha='right')
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(OUT_PNG / filename)
    plt.close()

def save_line_png(x, y, title, filename, xlabel=None, ylabel=None):
    plt.figure()
    plt.title(title)
    plt.plot(x, y, marker='o')
    if xlabel:
        plt.xlabel(xlabel)
    if ylabel:
        plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(OUT_PNG / filename)
    plt.close()

with sqlite3.connect(DB) as con:
    # --- Hires by Technology (PNG)
    rows = con.execute(SQL_TECH).fetchall()
    if rows:
        labels, values = zip(*rows)
        save_bar_png(labels, values, 'Hires by Technology', 'hires_by_technology.png',
                     xlabel='Technology', ylabel='Hires')

    # --- Hires by Year (PNG)
    rows = con.execute(SQL_YEAR).fetchall()
    if rows:
        years, values = zip(*rows)
        save_line_png(years, values, 'Hires by Year', 'hires_by_year.png',
                      xlabel='Year', ylabel='Hires')

    # --- Hires by Seniority (PNG)
    rows = con.execute(SQL_SENIORITY).fetchall()
    if rows:
        labels, values = zip(*rows)
        save_bar_png(labels, values, 'Hires by Seniority', 'hires_by_seniority.png',
                     xlabel='Seniority', ylabel='Hires')

    # --- Country over Years (HTML, interactive)
    try:
        import pandas as pd
        import plotly.express as px
        df_country = pd.read_sql_query(SQL_COUNTRY_YEAR, con)
        if not df_country.empty:
            fig = px.line(df_country, x='year', y='hires', color='country', markers=True,
                          title='Hires by Country over Years')
            fig.write_html(OUT_HTML / 'hires_by_country_over_years.html', include_plotlyjs='cdn')
    except Exception:
        pass

    # --- Avg Scores by Hired (PNG + HTML)
    try:
        import pandas as pd
        import plotly.express as px
        df_scores = pd.read_sql_query(SQL_AVG_SCORES_BY_HIRED, con)
        if not df_scores.empty:
            # PNG (two bars per metric side by side)
            labels = df_scores['label'].tolist()
            plt.figure()
            plt.title('Average Scores by Hired Status')
            # simple grouped bars without choosing colors
            x = range(len(labels))
            width = 0.35
            plt.bar([i - width/2 for i in x], df_scores['avg_code_challenge'], width, label='Code Challenge')
            plt.bar([i + width/2 for i in x], df_scores['avg_tech_interview'], width, label='Technical Interview')
            plt.xticks(list(x), labels)
            plt.ylabel('Average Score')
            plt.legend()
            plt.tight_layout()
            plt.savefig(OUT_PNG / 'avg_scores_by_hired.png')
            plt.close()

            # HTML interactive
            df_melt = df_scores.melt(id_vars='label',
                                     value_vars=['avg_code_challenge','avg_tech_interview'],
                                     var_name='metric', value_name='avg_score')
            fig = px.bar(df_melt, x='label', y='avg_score', color='metric',
                         title='Average Scores by Hired Status', barmode='group')
            fig.write_html(OUT_HTML / 'avg_scores_by_hired.html', include_plotlyjs='cdn')
    except Exception:
        pass

    # --- Hires by YOE band (PNG + HTML)
    try:
        import pandas as pd
        import plotly.express as px
        df_yoe = pd.read_sql_query(SQL_HIRES_BY_YOE_BAND, con)
        if not df_yoe.empty:
            save_bar_png(df_yoe['label'].tolist(), df_yoe['hires'].tolist(),
                         'Hires by YOE Band', 'hires_by_yoe_band.png',
                         xlabel='YOE Band', ylabel='Hires', rotate_x=False)

            fig = px.bar(df_yoe, x='label', y='hires', title='Hires by YOE Band')
            fig.write_html(OUT_HTML / 'hires_by_yoe_band.html', include_plotlyjs='cdn')
    except Exception:
        pass

print(f"PNG → {OUT_PNG.resolve()} | HTML → {OUT_HTML.resolve()}")
