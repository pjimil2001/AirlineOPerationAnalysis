# Airline Operation Analysis 🚀

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Pandas](https://img.shields.io/badge/Pandas-1.6.2-blue?logo=pandas)
![MIT License](https://img.shields.io/badge/license-MIT-green)

## 🧭 Project Overview

This repository contains the **Airline Operation Analysis** capstone project, which processes and merges airline flight datasets to produce a clean, enriched output that can be used for analytics and visualization.

## 📁 Repository Structure

- `Cleaning_and_Merging.py` – Main script to clean and merge flight data.
- `DATA/` – Source datasets (raw CSVs).
  - `airlines.csv` – Airlines base information.
  - `airports.csv` – Airports metadata.
  - `flights.csv` – Raw flight records.
- `Cleaned_dataset/` – Output dataset(s) produced by the cleaning pipeline.
- `OUTPUT/` – Any generated reports, analysis outputs, or model artifacts.
- `run_output.txt` – Example output log from a pipeline run.

## 🚀 Getting Started

### 1) Create a Python virtual environment

```bash
python -m venv .venv
.
```

### 2) Activate the virtual environment

- **Windows (PowerShell)**
  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```

- **macOS / Linux**
  ```bash
  source .venv/bin/activate
  ```

### 3) Install dependencies

If you have a `requirements.txt` file, run:

```bash
pip install -r requirements.txt
```

If a requirements file does not exist, install at least the packages below:

```bash
pip install pandas numpy
```

## ▶️ Running the Pipeline

Run the main cleaning script:

```bash
python Cleaning_and_Merging.py
```

After the script completes, check the `Cleaned_dataset/` folder for the processed output.

## 📌 Notes

- Ensure the CSV files in `DATA/` are present and not corrupted.
- This project is structured to support further analysis (e.g., visualization, modeling, dashboards).

---

## 🏷️ License

This project is provided under the **MIT License**. See `LICENSE` for details.
