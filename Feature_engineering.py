"""Feature engineering for Airline Operation Analysis dataset.

This script reads the cleaned/enriched dataset produced by Cleaning_and_Merging.py
(`DATA/flights_CLEANED_ENRICHED.csv`), computes additional operational features,
and overwrites the file (or writes to a new file if requested).

New features added:
- Delay flags (departure/arrival > 15 min)
- Total delay (departure + arrival)
- Distance category (short/medium/long haul)
- Season (Winter/Spring/Summer/Fall)
- Daypart, Departure hour, Weekend flag
- Route (ORIGIN -> DESTINATION)
- Flights per tail number per day/week
- Origin congestion (departures per origin per hour)
- Weather impact flag
- Cancellation / diversion flags

Usage:
    python Feature_engineering.py

Optionally specify an output file path:
    python Feature_engineering.py --output DATA/flights_CLEANED_ENRICHED.csv
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd


def safe_int_hour_from_hhmm(hhmm: pd.Series, default: int | None = None) -> pd.Series:
    """Extract hour portion from HHMM-style string/int.

    Works for strings like '0530', '730', '0000' and for numeric values.
    Returns a series of integers 0-23 or NaN for invalids.
    """
    # Ensure string
    s = hhmm.astype(str).fillna("")

    # Remove decimal if present (e.g., '615.0')
    s = s.str.replace(r"\.0+$", "", regex=True)

    # Zero-pad to 4 characters where possible (e.g. '615' -> '0615')
    s = s.str.zfill(4)

    # Keep only 4-digit numeric values
    valid = s.str.match(r"^\d{4}$")
    s_valid = s.where(valid)

    # Extract hour
    hour = pd.to_numeric(s_valid.str[:2], errors="coerce")

    # Clip to 0-23
    hour = hour.where(hour.between(0, 23))

    if default is not None:
        hour = hour.fillna(default)

    return hour.astype('Int64')


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add engineering features to the flights DataFrame."""

    df = df.copy()

    # ------------------------------
    # Basic derived fields
    # ------------------------------

    # 1) Route string
    df['ROUTE'] = df['ORIGIN_AIRPORT'].astype(str) + "-" + df['DESTINATION_AIRPORT'].astype(str)

    # 2) Departure hour + daypart + peak hour
    # Use scheduled departure to avoid sleeping/early morning flights being treated incorrectly.
    # We rely on the standardized _STR columns produced earlier by Cleaning_and_Merging.py.
    if 'SCHEDULED_DEPARTURE_STR' in df.columns:
        departure_hhmm = df['SCHEDULED_DEPARTURE_STR']
    else:
        departure_hhmm = df['SCHEDULED_DEPARTURE']

    df['DEPARTURE_HOUR'] = safe_int_hour_from_hhmm(departure_hhmm)

    def compute_daypart(hour: pd.Series) -> pd.Series:
        # NA-safe mapping
        part = pd.Series([pd.NA] * len(hour), index=hour.index, dtype="string")
        part = part.where(hour.isna())

        # Define dayparts
        part = part.mask(hour.between(0, 5), "Night")
        part = part.mask(hour.between(6, 11), "Morning")
        part = part.mask(hour.between(12, 17), "Afternoon")
        part = part.mask(hour.between(18, 23), "Evening")
        return part

    df['DAYPART'] = compute_daypart(df['DEPARTURE_HOUR'])

    # Peak hours: typically 6-9 and 16-19
    df['IS_PEAK_HOUR'] = df['DEPARTURE_HOUR'].isin(range(6, 10)) | df['DEPARTURE_HOUR'].isin(range(16, 20))

    # Weekend indicator (1=Monday): Saturday=6, Sunday=7
    if 'DAY_OF_WEEK' in df.columns:
        df['IS_WEEKEND'] = df['DAY_OF_WEEK'].isin([6, 7])
    else:
        df['IS_WEEKEND'] = pd.NA

    # ------------------------------
    # Seasonality
    # ------------------------------
    if 'MONTH' in df.columns:
        season_map = {
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        }
        df['SEASON'] = df['MONTH'].map(season_map).astype('string')
    else:
        df['SEASON'] = pd.NA

    # ------------------------------
    # Delay features
    # ------------------------------
    df['IS_DELAYED_DEPARTURE'] = (df['DEPARTURE_DELAY'] > 15).fillna(False)
    df['IS_DELAYED_ARRIVAL'] = (df['ARRIVAL_DELAY'] > 15).fillna(False)
    df['TOTAL_DELAY'] = (df['DEPARTURE_DELAY'].fillna(0) + df['ARRIVAL_DELAY'].fillna(0))

    # Weather impact
    if 'WEATHER_DELAY' in df.columns:
        df['HAS_WEATHER_DELAY'] = (df['WEATHER_DELAY'] > 0).fillna(False)
    else:
        df['HAS_WEATHER_DELAY'] = False

    # Cancellation / diversion flags
    df['IS_CANCELLED'] = (df.get('CANCELLED', 0) == 1)
    df['IS_DIVERTED'] = (df.get('DIVERTED', 0) == 1)

    # ------------------------------
    # Distance category
    # ------------------------------
    if 'DISTANCE' in df.columns:
        bins = [0, 500, 1500, float('inf')]
        labels = ['short_haul', 'medium_haul', 'long_haul']
        df['DISTANCE_CATEGORY'] = pd.cut(df['DISTANCE'], bins=bins, labels=labels, right=False)
    else:
        df['DISTANCE_CATEGORY'] = pd.NA

    # ------------------------------
    # FlightDate + flights per tail number
    # ------------------------------
    # Create a proper date column for grouping
    if {'YEAR', 'MONTH', 'DAY'}.issubset(df.columns):
        df['FLIGHT_DATE'] = pd.to_datetime(df[['YEAR', 'MONTH', 'DAY']], errors='coerce')
    else:
        df['FLIGHT_DATE'] = pd.NaT

    # Flights per tail number per day
    if 'TAIL_NUMBER' in df.columns:
        df['FLIGHTS_PER_TAIL_NUMBER_PER_DAY'] = (
            df.groupby(['TAIL_NUMBER', 'FLIGHT_DATE'])['TAIL_NUMBER']
              .transform('count')
        )

        # Week-based grouping (ISO year/week)
        # Pandas 1.3+ returns DataFrame/Series with isocalendar columns.
        iso = df['FLIGHT_DATE'].dt.isocalendar()
        df['ISO_YEAR'] = iso['year'].astype('Int64')
        df['ISO_WEEK'] = iso['week'].astype('Int64')

        df['FLIGHTS_PER_TAIL_NUMBER_PER_WEEK'] = (
            df.groupby(['TAIL_NUMBER', 'ISO_YEAR', 'ISO_WEEK'])['TAIL_NUMBER']
              .transform('count')
        )
    else:
        df['FLIGHTS_PER_TAIL_NUMBER_PER_DAY'] = pd.NA
        df['FLIGHTS_PER_TAIL_NUMBER_PER_WEEK'] = pd.NA
        df['ISO_YEAR'] = pd.NA
        df['ISO_WEEK'] = pd.NA

    # ------------------------------
    # Airport congestion (departures per origin per hour)
    # ------------------------------
    # Use scheduled departure hour and date to count departures in the same hour
    if {'ORIGIN_AIRPORT', 'FLIGHT_DATE', 'DEPARTURE_HOUR'}.issubset(df.columns):
        df['ORIGIN_CONGESTION'] = (
            df.groupby(['ORIGIN_AIRPORT', 'FLIGHT_DATE', 'DEPARTURE_HOUR'])['ORIGIN_AIRPORT']
              .transform('count')
        )
    else:
        df['ORIGIN_CONGESTION'] = pd.NA

    # ------------------------------
    # Cleanup helper columns (optional)
    # ------------------------------
    # We keep FLIGHT_DATE, ISO_YEAR, ISO_WEEK by default; can be dropped later if desired.

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute feature-engineered columns for flights dataset.")
    parser.add_argument(
        "--input",
        default=os.path.join(Path(__file__).resolve().parent, "DATA", "flights_CLEANED_ENRICHED.csv"),
        help="Path to the cleaned/enriched CSV file to read.")
    parser.add_argument(
        "--output",
        default=None,
        help="Path to write the feature-engineered CSV file. If omitted, overwrites the input file.")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    print(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df):,} rows, {len(df.columns):,} columns")

    df = add_features(df)

    output_path = Path(args.output) if args.output else input_path
    print(f"Saving feature-engineered data to: {output_path}")
    df.to_csv(output_path, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
