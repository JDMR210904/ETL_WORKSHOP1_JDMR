PRAGMA foreign_keys = ON;

-- =========================
-- Dimensions
-- =========================
CREATE TABLE IF NOT EXISTS DimDate (
  date_id INTEGER PRIMARY KEY,              -- yyyymmdd
  full_date TEXT NOT NULL,                  -- ISO yyyy-mm-dd
  day INTEGER NOT NULL,
  month INTEGER NOT NULL,
  month_name TEXT NOT NULL,
  quarter INTEGER NOT NULL,
  year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS DimTechnology (
  technology_id INTEGER PRIMARY KEY AUTOINCREMENT,
  technology TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS DimSeniority (
  seniority_id INTEGER PRIMARY KEY AUTOINCREMENT,
  seniority TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS DimCountry (
  country_id INTEGER PRIMARY KEY AUTOINCREMENT,
  country TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS DimCandidate (
  candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT NOT NULL,
  last_name  TEXT NOT NULL,
  email      TEXT NOT NULL UNIQUE
);

-- =========================
-- Fact
-- =========================
CREATE TABLE IF NOT EXISTS FactHiring (
  fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
  candidate_id INTEGER NOT NULL,
  technology_id INTEGER NOT NULL,
  seniority_id INTEGER NOT NULL,
  country_id INTEGER NOT NULL,
  date_id INTEGER NOT NULL,
  yoe INTEGER NOT NULL,
  code_challenge_score REAL NOT NULL,
  technical_interview_score REAL NOT NULL,
  hired INTEGER NOT NULL CHECK (hired IN (0,1)),
  FOREIGN KEY(candidate_id) REFERENCES DimCandidate(candidate_id),
  FOREIGN KEY(technology_id) REFERENCES DimTechnology(technology_id),
  FOREIGN KEY(seniority_id) REFERENCES DimSeniority(seniority_id),
  FOREIGN KEY(country_id) REFERENCES DimCountry(country_id),
  FOREIGN KEY(date_id) REFERENCES DimDate(date_id)
);

-- =========================
-- Helpful indexes
-- =========================
CREATE INDEX IF NOT EXISTS idx_fact_date ON FactHiring(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_country ON FactHiring(country_id);
CREATE INDEX IF NOT EXISTS idx_fact_tech ON FactHiring(technology_id);
CREATE INDEX IF NOT EXISTS idx_fact_sen ON FactHiring(seniority_id);
