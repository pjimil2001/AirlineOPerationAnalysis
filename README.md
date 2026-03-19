# Airline Operation Analysis (2015)

<p align="center">
  <img src="https://img.icons8.com/external-flaticons-lineal-color-flat-icons/344/external-airplane-airport-flaticons-lineal-color-flat-icons.png" alt="Airline Operation Analysis Logo" width="120" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python" alt="Python" />
  <img src="https://img.shields.io/badge/Pandas-Data%20Wrangling-blue?logo=pandas" alt="Pandas" />
  <img src="https://img.shields.io/badge/Databricks-Notebooks-critical?logo=databricks" alt="Databricks" />
  <img src="https://img.shields.io/badge/Tableau-Analytics-orange?logo=tableau" alt="Tableau" />
  <img src="https://img.shields.io/badge/SQL-Queries-2f6fdb?logo=sqlite" alt="SQL" />
</p>

<p align="center">
  <strong>Capstone Project — Operational Bottleneck Identification and Performance Optimization in the U.S. Airline Industry</strong><br/>
  <sub>Cleaning • Feature engineering • Descriptive analytics • Statistical analysis • Machine learning • Prescriptive analytics</sub>
</p>

This repository contains an end-to-end pipeline that cleans, enriches, and analyzes **U.S. domestic flight operations data (2015)** to identify operational bottlenecks (airlines, airports, and time-of-day effects) and support decision-making.

---

## Table of Contents

- [Overview](#overview)
- [Key Results (Highlights)](#key-results-highlights)
- [Objectives](#objectives)
- [Repository Structure](#repository-structure)
- [Data Sources](#data-sources)
- [Pipeline Overview](#pipeline-overview)
- [How to Run](#how-to-run)
- [Outputs](#outputs)
- [Analytics Notebooks](#analytics-notebooks)
- [Notes & Troubleshooting](#notes--troubleshooting)
- [License](#license)

---

## Overview

The project:

- Validates and cleans dates/times and operational fields (delays, cancellations)
- Validates airline and airport codes against lookup tables and removes invalid rows
- Enriches raw flight records with airline and airport metadata (names, city/state, coordinates)
- Builds engineered features (route, congestion, seasonality, delay flags, etc.)
- Produces analysis notebooks (Databricks exports) for descriptive + statistical insights, ML modeling, and prescriptive recommendations

**Raw dataset scale (from `run_output.txt`):**

- **Rows**: **5,819,079**
- **Columns**: **31**

---

## Key Results (Highlights)

The following results are taken directly from notebook outputs in this repo (Databricks export tables).

### Airline-level performance (sample from `02_descriptive_analytics.ipynb`)

| Airline | Total flights | Arrival delay rate | Cancel rate | Avg arrival delay (min) |
|--------:|--------------:|-------------------:|------------:|-------------------------:|
| NK | 117,379 | 0.4846 | 0.0171 | 14.20 |
| F9 | 90,834 | 0.4539 | 0.0065 | 12.38 |
| WN | 1,261,855 | 0.3731 | 0.0127 | 4.31 |
| UA | 515,707 | 0.3611 | 0.0127 | 5.31 |
| AA | 725,828 | 0.3472 | 0.0150 | 3.13 |
| AS | 172,521 | 0.3301 | 0.0039 | -0.97 |
| DL | 875,814 | 0.2863 | 0.0044 | 0.11 |

**Highlights from the same output table:**

- **Highest arrival delay rate (shown)**: `NK` (~48.5%), `F9` (~45.4%)
- **Lowest arrival delay rate (shown)**: `DL` (~28.6%)
- **Highest cancellation rate (in the table output)**: `MQ` (~5.10%)
- **Lowest cancellation rate (in the table output)**: `HA` (~0.22%)

### Time-of-day effect (from `06_summary&visuals.ipynb`)

Arrival delay rate by **departure hour** shows clear propagation into evening operations:

- **Best hour (lowest delay rate)**: **05:00** (~0.2056)
- **Worst hour (highest delay rate)**: **19:00** (~0.4364)

### Congestion insight (from `06_summary&visuals.ipynb`)

- “**High traffic airports show higher delay rates, indicating congestion-driven inefficiencies.**”

---

## Objectives

- **Data quality**: Produce a clean, consistent dataset with validated dates/times and IATA codes
- **Enrichment**: Attach airline names and airport metadata (city/state, lat/lon)
- **Feature engineering**: Add operational features (delay flags, seasonality, route, congestion)
- **Analytics**: Identify delay/cancellation patterns by airline, airport, and time-of-day

---

## Repository Structure

```
AirlineOPerationAnalysis/
├── README.md
├── requirements.txt
├── run_output.txt
├── Airline_Sql_Query.sql
│
├── Cleaning_and_Merging.py
├── 01_Feature_engineering.py
│
├── 02_descriptive_analytics.ipynb
├── 03_statistical_analysis.ipynb
├── 04_machine_learing.ipynb
├── 05_prescriptive_analytics.ipynb
├── 06_summary&visuals.ipynb
│
└── DATA/
    ├── flights.csv
    ├── airlines.csv
    ├── airports.csv
    └── flights_CLEANED_ENRICHED.csv
```

---

## Data Sources

| File | Description |
|------|-------------|
| `DATA/flights.csv` | Raw 2015 flight records (dates, carrier, airports, times, delays, cancellations, etc.) |
| `DATA/airlines.csv` | Airline lookup (IATA code → airline full name) |
| `DATA/airports.csv` | Airport lookup (IATA code → airport metadata and coordinates) |

---

## Pipeline Overview

| Step | Component | What it does |
|------|----------|--------------|
| 1 | `Cleaning_and_Merging.py` | Loads data, validates dates/times, fixes `2400` time edge cases, handles cancelled flights, validates codes, merges airline/airport lookups, fills missing coordinates (ECP/PBG/UST), saves cleaned/enriched CSV |
| 2 | `01_Feature_engineering.py` | Adds features such as `ROUTE`, `DEPARTURE_HOUR`, `DAYPART`, `SEASON`, delay flags, distance category, tail-number utilization, and `ORIGIN_CONGESTION` |
| 3+ | Notebooks (`02_*` → `06_*`) | Descriptive analytics, statistics, ML, prescriptive analysis, and a final executive summary with visuals |

---

## How to Run

### Prerequisites

- **Python**: 3.11+ recommended
- **RAM**: dataset is large (raw load reported ~1.3 GB in `run_output.txt`)
- **Input files present**: ensure `DATA/` has `flights.csv`, `airlines.csv`, `airports.csv`

### Install

```bash
python -m venv .venv
```

Windows (PowerShell):

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### Run cleaning + merge (Step 1)

```bash
python Cleaning_and_Merging.py
```

### Run feature engineering (Step 2)

```bash
python 01_Feature_engineering.py
```

Optional I/O overrides:

```bash
python 01_Feature_engineering.py --input DATA/flights_CLEANED_ENRICHED.csv --output DATA/flights_FINAL.csv
```

---

## Outputs

| Output | Location | Description |
|--------|----------|-------------|
| Cleaned & enriched dataset | `DATA/flights_CLEANED_ENRICHED.csv` | Produced by Step 1; cleaned and joined with airline/airport metadata |
| Feature-engineered dataset | Same file (or `--output`) | Produced by Step 2; adds route, seasonality, delay flags, congestion features, etc. |
| Example run log | `run_output.txt` | Console log captured from running Step 1 |

---

## Analytics Notebooks

These notebooks are Databricks exports included in the repo:

- **`02_descriptive_analytics.ipynb`**: airline/airport performance, delay & cancellation patterns, congestion views
- **`03_statistical_analysis.ipynb`**: statistical exploration and tests
- **`04_machine_learing.ipynb`**: predictive modeling workflow (file name kept as-is)
- **`05_prescriptive_analytics.ipynb`**: prescriptive recommendations
- **`06_summary&visuals.ipynb`**: executive summary + visuals and consolidated insights

---

## Notes & Troubleshooting

- **`run_output.txt` encoding**: the file is UTF-16 (so it may look “weird” in some editors); open with UTF-16/Unicode encoding.
- **Mixed dtypes warning**: you may see pandas `DtypeWarning` for airport code columns; setting `low_memory=False` or explicit `dtype=` can help.
- **Order of execution**: run `Cleaning_and_Merging.py` before `01_Feature_engineering.py`.

---

## License

MIT

