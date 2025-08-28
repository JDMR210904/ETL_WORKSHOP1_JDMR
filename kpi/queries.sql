-- =========================
-- Required KPIs
-- =========================
-- 1) Hires by Technology (+ hire rate)
SELECT t.technology,
       SUM(f.hired) AS hires,
       COUNT(*) AS total_candidates,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimTechnology t USING(technology_id)
GROUP BY t.technology
ORDER BY hires DESC;

-- 2) Hires by Year (+ hire rate)
SELECT d.year,
       SUM(f.hired) AS hires,
       COUNT(*) AS total_candidates,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimDate d USING(date_id)
GROUP BY d.year
ORDER BY d.year;

-- 3) Hires by Seniority
SELECT s.seniority, SUM(f.hired) AS hires, COUNT(*) AS total_candidates,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimSeniority s USING(seniority_id)
GROUP BY s.seniority
ORDER BY hires DESC;

-- 4) Hires by Country over Years (focus set)
SELECT d.year, c.country, SUM(f.hired) AS hires
FROM FactHiring f
JOIN DimDate d USING(date_id)
JOIN DimCountry c USING(country_id)
WHERE c.country IN ('United States','Brazil','Colombia','Ecuador')
GROUP BY d.year, c.country
ORDER BY d.year, hires DESC;

-- =========================
-- +2 Additional KPIs
-- =========================
-- A) Hire Rate by Technology (sorted by rate)
SELECT t.technology,
       COUNT(*) AS total_candidates,
       SUM(f.hired) AS hires,
       ROUND(100.0 * SUM(f.hired) / COUNT(*), 2) AS hire_rate_pct
FROM FactHiring f
JOIN DimTechnology t USING(technology_id)
GROUP BY t.technology
ORDER BY hire_rate_pct DESC;

-- B) Hires by YOE band (computed on the fly)
SELECT CASE
         WHEN f.yoe < 3 THEN '0-2'
         WHEN f.yoe BETWEEN 3 AND 5 THEN '3-5'
         WHEN f.yoe BETWEEN 6 AND 10 THEN '6-10'
         ELSE '11+'
       END AS experience_range,
       SUM(f.hired) AS hires
FROM FactHiring f
GROUP BY experience_range
ORDER BY CASE experience_range
           WHEN '0-2' THEN 1
           WHEN '3-5' THEN 2
           WHEN '6-10' THEN 3
           ELSE 4
         END;

-- C) (Extra) Average scores by hired vs not hired
SELECT f.hired,
       ROUND(AVG(f.code_challenge_score), 2) AS avg_code_challenge,
       ROUND(AVG(f.technical_interview_score), 2) AS avg_tech_interview
FROM FactHiring f
GROUP BY f.hired
ORDER BY f.hired DESC;
