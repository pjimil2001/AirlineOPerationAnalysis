import pandas as pd
import numpy as np

# -----------------------------
# Load Data
# -----------------------------
flights = pd.read_csv("DATA/flights.csv")
airlines = pd.read_csv("DATA/airlines.csv")

print(flights.info())
print(flights.head())

# -----------------------------
# Data Description
# -----------------------------
print(flights.describe(include="all"))

# Unique values count
print(flights.nunique())

# Null count
print(flights.isnull().sum())

# -----------------------------
# Missing Value Treatment
# -----------------------------

# Remove cancelled/diverted flights
flights_cleaned = flights[
    (flights["CANCELLED"] == 0) &
    (flights["DIVERTED"] == 0) &
    (flights["ARRIVAL_DELAY"].notna())
]

delay_columns = [
    "WEATHER_DELAY",
    "LATE_AIRCRAFT_DELAY",
    "AIR_SYSTEM_DELAY",
    "SECURITY_DELAY",
    "AIRLINE_DELAY",
    "CANCELLED",
    "DIVERTED"
]

# Replace null delays with 0
for col in delay_columns:
    flights_cleaned[col] = flights_cleaned[col].fillna(0)

# Save intermediate dataset
# flights_cleaned.to_csv(
#     "Cleaned_Dataset_flights_cleaned_delays.csv",
#     index=False
# )

# -----------------------------
# Mean Imputation
# -----------------------------

impute_columns = [
    "ARRIVAL_DELAY","ARRIVAL_TIME","DEPARTURE_TIME",
    "DEPARTURE_DELAY","TAXI_OUT","WHEELS_OFF",
    "SCHEDULED_TIME","ELAPSED_TIME","AIR_TIME",
    "DISTANCE","WHEELS_ON","TAXI_IN",
    "SCHEDULED_ARRIVAL","SCHEDULED_DEPARTURE"
]

for col in impute_columns:
    flights_cleaned.loc[:, col] = flights_cleaned[col].fillna(
        flights_cleaned[col].mean()
    )

# Remove null flight numbers
df = flights_cleaned.dropna(subset=["FLIGHT_NUMBER"])

# Fill airport nulls with most frequent value
df["ORIGIN_AIRPORT"] = df["ORIGIN_AIRPORT"].fillna(df["ORIGIN_AIRPORT"].mode()[0])
df["DESTINATION_AIRPORT"] = df["DESTINATION_AIRPORT"].fillna(df["DESTINATION_AIRPORT"].mode()[0])

# df.to_csv("Clean_Dataset.csv", index=False)

# -----------------------------
# Feature Engineering
# -----------------------------

# Departure Hour
df["DEPARTURE_HOUR"] = (df["SCHEDULED_DEPARTURE"] // 100).astype(int)

# Peak Hour Indicator
df["PEAK_HOUR_INDICATOR"] = np.where(
    ((df["DEPARTURE_HOUR"].between(7,9)) |
     (df["DEPARTURE_HOUR"].between(16,19))),1,0
)

# Weekend Indicator
df["WEEKEND_INDICATOR"] = np.where(
    df["DAY_OF_WEEK"].isin([6,7]),1,0
)

# Route Feature
df["ROUTE"] = df["ORIGIN_AIRPORT"].astype(str) + "-" + df["DESTINATION_AIRPORT"].astype(str)

# Delay Flags
df["IS_DELAYED_DEPARTURE"] = np.where(df["DEPARTURE_DELAY"] > 15,1,0)
df["IS_DELAYED_ARRIVAL"] = np.where(df["ARRIVAL_DELAY"] > 15,1,0)

# Total Delay
df["TOTAL_DELAY"] = df["DEPARTURE_DELAY"] + df["ARRIVAL_DELAY"]

# Distance Category
df["DISTANCE_CATEGORY"] = np.select(
    [
        df["DISTANCE"] < 1000,
        (df["DISTANCE"] >= 1000) & (df["DISTANCE"] < 2000)
    ],
    ["Short","Medium"],
    default="Long"
)

# Season
df["SEASON"] = np.select(
    [
        df["MONTH"].isin([12,1,2]),
        df["MONTH"].isin([3,4,5]),
        df["MONTH"].isin([6,7,8]),
        df["MONTH"].isin([9,10,11])
    ],
    ["Winter","Spring","Summer","Fall"],
    default="Unknown"
)

# Weather Impact
df["HAS_WEATHER_DELAY"] = np.where(df["WEATHER_DELAY"] > 0,1,0)

# -----------------------------
# Aircraft Utilization
# -----------------------------

tail_day = (
    df.groupby(["TAIL_NUMBER","YEAR","MONTH","DAY"])
    .size()
    .reset_index(name="FLIGHTS_PER_TAIL_NUMBER_DAY")
)

tail_week = (
    df.groupby(["TAIL_NUMBER","YEAR","MONTH","DAY_OF_WEEK"])
    .size()
    .reset_index(name="FLIGHTS_PER_TAIL_NUMBER_WEEK")
)

# Airport Congestion
origin_congestion = (
    df.groupby(
        ["ORIGIN_AIRPORT","YEAR","MONTH","DAY","DEPARTURE_HOUR"]
    )
    .size()
    .reset_index(name="ORIGIN_CONGESTION")
)

# Merge features
df = df.merge(tail_day, how="left")
df = df.merge(tail_week, how="left")
df = df.merge(origin_congestion, how="left")

# -----------------------------
# Save Final Dataset
# -----------------------------
df.to_csv("Clean_Dataset_with_features.csv", index=False)

print("Data cleaning and feature engineering completed!")
