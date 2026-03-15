# Airline Operation Analysis

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Pandas](https://img.shields.io/badge/Pandas-1.6.2-blue?logo=pandas)
![License](https://img.shields.io/badge/license-MIT-green)

**Capstone Project — Operational Bottleneck Identification and Performance Optimization in the U.S. Airline Industry**

This repository contains an end-to-end **Airline Operation Analysis** pipeline that cleans, enriches, and analyzes U.S. domestic flight data (2015). It produces a validated, feature-rich dataset suitable for descriptive analytics, delay/cancellation analysis, and operational insights.

---

## Table of Contents

- [Overview](#overview)
- [Objectives](#objectives)
- [Repository Structure](#repository-structure)
- [Data Sources](#data-sources)
- [Pipeline Overview](#pipeline-overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Pipeline Steps (Detailed)](#pipeline-steps-detailed)
- [Outputs](#outputs)
- [Descriptive Analytics (Databricks)](#descriptive-analytics-databricks)
- [Notes & Troubleshooting](#notes--troubleshooting)
- [License](#license)

---

## Overview

The project processes raw flight records from the U.S. Bureau of Transportation Statistics (BTS), merges them with airline and airport lookup tables, validates and cleans time/date/code fields, removes invalid records, and enriches the data with airline/airport names and coordinates. A second stage adds engineered features (delay flags, season, congestion, etc.) for analytics. A third stage (Databricks notebook) performs descriptive analytics to identify airline-level and airport-level bottlenecks.

**Key capabilities:**

- Load and validate ~5.8M flight rows (31 raw columns).
- Standardize time columns (HHMM 0000–2359, 4-digit strings).
- Handle cancelled flights (N/A for timing/delay fields) and cancellation reasons.
- Validate airline/airport codes against IATA lookups and drop invalid rows.
- Join airlines and airports metadata (names, city, state, lat/lon).
- Fill missing coordinates for a few airports (ECP, PBG, UST).
- Clean delay columns and set cancellation reason `NC` for non-cancelled flights.
- Add features: delay flags, season, daypart, congestion, route, etc.
- Save a final cleaned & enriched dataset (~5.3M rows, 50+ columns) for analysis.

---

## Objectives

- **Data quality:** Produce a clean, consistent dataset with validated dates, times, and IATA codes.
- **Enrichment:** Attach airline full names and airport metadata (name, city, state, coordinates).
- **Feature engineering:** Add operational features (delay indicators, seasonality, congestion, route).
- **Analytics:** Support descriptive analytics to identify delay/cancellation patterns by airline, airport, and time.

---

## Repository Structure

```
AirlineOPerationAnalysis/
├── README.md                    # This file
├── requirements.txt             # Python dependencies (pandas, numpy, scipy)
├── run_output.txt               # Example log from running the cleaning pipeline
│
├── Cleaning_and_Merging.py      # Step 1: Load, validate, clean, join, save
├── 01_Feature_engineering.py    # Step 2: Add derived features to cleaned data
├── 02_descriptive_analytics.py  # Step 3: Databricks notebook for SQL analytics
│
├── DATA/                        # Input data and pipeline output
│   ├── flights.csv              # Raw flight records (BTS 2015)
│   ├── airlines.csv             # Airline IATA codes and full names
│   ├── airports.csv             # Airport IATA codes, name, city, state, lat, lon
│   └── flights_CLEANED_ENRICHED.csv   # Output: cleaned + enriched (+ optional features)
│
├── Cleaned_dataset/             # Optional; can hold copies of cleaned output
└── OUTPUT/                      # Optional; reports, exports, or model artifacts
```

---

## Data Sources

| File           | Description |
|----------------|-------------|
| `DATA/flights.csv`   | Raw flight data (year, month, day, airline, airports, times, delays, cancelled, etc.). |
| `DATA/airlines.csv` | Lookup: IATA code → airline full name. |
| `DATA/airports.csv` | Lookup: IATA code → airport name, city, state, latitude, longitude. |

The flight data is **2015 U.S. domestic flights** (BTS-style). Ensure all three CSVs are present in `DATA/` before running the pipeline.

---

## Pipeline Overview

| Step | Script / Notebook           | Purpose |
|------|-----------------------------|--------|
| 1    | `Cleaning_and_Merging.py`  | Load flights, validate dates/times, clean times (2400→0, 4-digit), set N/A for cancelled, validate/remove invalid airport codes, join airlines/airports, fill missing lat/lon, check duplicates/junk, save `flights_CLEANED_ENRICHED.csv` to `DATA/`. |
| 2    | `01_Feature_engineering.py`| Read cleaned CSV, add delay flags, season, daypart, route, congestion, etc., then overwrite or save to a new file. |
| 3    | `02_descriptive_analytics.py` | Databricks notebook: SQL queries on a table (e.g. `finally_cleaned_data`) for airline/airport performance, monthly trends, route-level bottlenecks. |

Run **Step 1**, then **Step 2**. Step 3 is intended for Databricks using a table created from the final CSV.

---

## Prerequisites

- **Python:** 3.11 (or compatible 3.x).
- **OS:** Windows, macOS, or Linux.
- **Disk:** Sufficient space for large CSV (~5M+ rows); output file can be on the order of hundreds of MB to over 1 GB uncompressed.
- **Memory:** Recommended 4 GB+ RAM for loading the full flights CSV.

---

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd AirlineOPerationAnalysis
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv .venv
```

### 3. Activate the virtual environment

- **Windows (PowerShell):**
  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```

- **macOS / Linux:**
  ```bash
  source .venv/bin/activate
  ```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

If you prefer to install manually:

```bash
pip install pandas numpy scipy
```

---

## Usage

### Run the full pipeline (cleaning + feature engineering)

**Step 1 — Cleaning and merging (required first):**

```bash
python Cleaning_and_Merging.py
```

- Reads `DATA/flights.csv`, `DATA/airlines.csv`, `DATA/airports.csv`.
- Writes `DATA/flights_CLEANED_ENRICHED.csv`.
- Console output includes validation summaries and row counts.

**Step 2 — Feature engineering (after Step 1):**

```bash
python 01_Feature_engineering.py
```

- Reads `DATA/flights_CLEANED_ENRICHED.csv` by default.
- Overwrites the same file with added feature columns (or use `--output` to write elsewhere).

Optional: specify input/output paths:

```bash
python 01_Feature_engineering.py --input DATA/flights_CLEANED_ENRICHED.csv --output DATA/flights_FINAL.csv
```

**Step 3 — Descriptive analytics:**

- Use `02_descriptive_analytics.py` in **Databricks**.
- Create a table (e.g. `finally_cleaned_data`) from the final CSV (e.g. from Step 2).
- Run the notebook cells to get airline-level, airport-level, monthly, and route-level metrics.

---

## Pipeline Steps (Detailed)

### Step 1: `Cleaning_and_Merging.py`

1. **Load** `DATA/flights.csv` (and list contents of `DATA/`).
2. **Validate date columns:** YEAR (2015), MONTH (1–12), DAY (1–31), DAY_OF_WEEK (1–7).
3. **Validate time columns:** SCHEDULED_DEPARTURE, DEPARTURE_TIME, WHEELS_OFF, WHEELS_ON, SCHEDULED_ARRIVAL, ARRIVAL_TIME (expected 0000–2359).
4. **Clean times:** Replace 2400 → 0; convert to 4-digit zero-padded strings (`*_STR` columns).
5. **Cancelled flights:** Set N/A (NaN/None) for departure/arrival times and delay fields where `CANCELLED == 1`.
6. **Validate codes:** Check AIRLINE against `airlines.csv`, ORIGIN_AIRPORT and DESTINATION_AIRPORT against `airports.csv`.
7. **Remove invalid rows:** Drop rows where either airport code is not in `airports.csv`.
8. **Cancellation reason:** Set `CANCELLATION_REASON = 'NC'` for non-cancelled flights.
9. **Delay columns:** Clean AIR_SYSTEM_DELAY, SECURITY_DELAY, AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY (numeric, fill missing with 0).
10. **Join lookups:** Merge airlines (add AIRLINE_FULL_NAME) and airports (add name, city, state, lat, lon for origin and destination).
11. **Fill missing coordinates:** ECP, PBG, UST (from FAA/official sources).
12. **Duplicate & junk check:** Report full duplicate rows and basic text validation on key columns.
13. **Save:** Write `DATA/flights_CLEANED_ENRICHED.csv`.

### Step 2: `01_Feature_engineering.py`

Adds (among others):

- **ROUTE:** `ORIGIN_AIRPORT-DESTINATION_AIRPORT`
- **DEPARTURE_HOUR,** **DAYPART** (Night/Morning/Afternoon/Evening), **IS_PEAK_HOUR,** **IS_WEEKEND**
- **SEASON** (Winter/Spring/Summer/Fall from MONTH)
- **IS_DELAYED_DEPARTURE / IS_DELAYED_ARRIVAL** (>15 min), **TOTAL_DELAY,** **HAS_WEATHER_DELAY**
- **IS_CANCELLED,** **IS_DIVERTED**
- **DISTANCE_CATEGORY** (short/medium/long haul)
- **FLIGHT_DATE,** **ISO_YEAR,** **ISO_WEEK**
- **FLIGHTS_PER_TAIL_NUMBER_PER_DAY,** **FLIGHTS_PER_TAIL_NUMBER_PER_WEEK**
- **ORIGIN_CONGESTION** (departures per origin per hour)

Reads from and (by default) overwrites the cleaned-enriched CSV.

### Step 3: `02_descriptive_analytics.py`

Databricks notebook that runs SQL on a table (e.g. `finally_cleaned_data`) to produce:

- Executive snapshot (total flights, delay rate, cancellation rate, avg arrival delay).
- Airline-level performance (delay rates, cancellation rate, avg delay by carrier).
- Airport congestion (delay rate, cancellation rate, avg congestion by origin).
- Monthly delay and cancellation trends.
- Route-level or other operational views.

---

## Outputs

| Output | Location | Description |
|--------|----------|-------------|
| Cleaned & enriched CSV | `DATA/flights_CLEANED_ENRICHED.csv` | After Step 1: validated rows, joined airline/airport metadata, cleaned times and delays. |
| Feature-engineered CSV | Same file (or path given by `--output`) | After Step 2: same as above plus ROUTE, SEASON, delay flags, congestion, etc. |
| Run log (example) | `run_output.txt` | Sample console output from a pipeline run. |

Approximate size: hundreds of MB to over 1 GB for the full CSV (uncompressed). Row count after cleaning is roughly ~5.33M (invalid airport rows removed).

---

## Descriptive Analytics (Databricks)

The file `02_descriptive_analytics.py` is a **Databricks notebook** (with `# MAGIC %md` and `# MAGIC %sql`). To use it:

1. Import or paste the notebook into a Databricks workspace.
2. Create a table (e.g. in a catalog/schema such as `tables.default`) from the final CSV produced by Step 2, and name it e.g. `finally_cleaned_data`.
3. Run the notebook; it will query this table for industry snapshot, airline performance, airport congestion, and monthly trends.

---

## Notes & Troubleshooting

- **CSV encoding:** The main script sets UTF-8 for stdout/stderr on Windows to avoid `charmap` errors when printing.
- **Mixed types warning:** If pandas warns about mixed types in columns (e.g. 7, 8), you can load with `dtype` specified or `low_memory=False` in `read_csv` if needed.
- **DATA folder:** Ensure `DATA/` exists and contains `flights.csv`, `airlines.csv`, and `airports.csv` before running Step 1.
- **Order of execution:** Always run `Cleaning_and_Merging.py` before `01_Feature_engineering.py`; the feature script expects the cleaned-enriched CSV.
- **Memory:** For very large machines, the full dataset loads in memory; if needed, consider chunked reading or sampling for experimentation.

---

## License

This project is provided under the **MIT License**. See the `LICENSE` file in the repository for full text.
