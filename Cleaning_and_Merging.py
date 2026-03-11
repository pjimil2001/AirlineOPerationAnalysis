import pandas as pd
import os
import sys
from pathlib import Path

# Ensure stdout/stderr can safely print UTF-8 on Windows consoles
# (avoids "charmap" encoding errors when printing unicode)
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except AttributeError:
    # Older Python versions may not have reconfigure
    pass

# Define the folder and try to find the file automatically
base_dir = Path(__file__).resolve().parent
folder = base_dir / "DATA"
file_name_candidates = ['flights.csv']

print("Files found in the folder:")
try:
    print(os.listdir(folder))
except FileNotFoundError:
    print(f"Folder not found: {folder}")
    print("Please double-check the exact path and capitalization.")
    # If folder is wrong, you can manually set file_path below

# Try to load the most likely file
file_path = None
for candidate in file_name_candidates:
    possible_path = os.path.join(folder, candidate)
    if os.path.isfile(possible_path):
        file_path = possible_path
        print(f"\nFound possible file: {possible_path}")
        break

if file_path is None:
    # If auto-detect failed, set it manually here (uncomment and edit)
    file_path = str(folder / 'flights.csv')   #   change extension if needed
    print(f"Using manual path: {file_path}")

#                                                 
# Load the file
#                                                 
try:
    if file_path.lower().endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file_path)
    else:
        # fallback - try csv first
        df = pd.read_csv(file_path)
    print("\nFile loaded successfully!")
except Exception as e:
    print(f"Error loading file: {e}")
    print("Possible reasons: wrong path, wrong file name, file is corrupted, or unsupported format.")
    df = None

#                                                 
# If loaded -> show overview
#                                                 
if df is not None:
    print("\nShape of the dataset:", df.shape)
    print("\nColumn names:")
    print(list(df.columns))
    
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nData types and non-null counts:")
    df.info()
    
    print("\nMissing values per column:")
    print(df.isnull().sum())
    
    print("\nQuick stats (numeric columns):")
    print(df.describe()) 

#                                                 
# Step 2: Validate date-related columns
#                                                 

print("\n" + "="*60)
print("STEP 2: VALIDATING DATE COLUMNS (YEAR, MONTH, DAY, DAY_OF_WEEK)")
print("="*60)

# Helper function to check range and show details
def validate_range_column(column_name, expected_min, expected_max, expected_set=None):
    if column_name not in df.columns:
        print(f"Column '{column_name}' not found in dataset!")
        return
    
    print(f"\n-> Checking {column_name}")
    
    # Basic stats
    min_val = df[column_name].min()
    max_val = df[column_name].max()
    unique_vals = sorted(df[column_name].dropna().unique())
    missing_count = df[column_name].isna().sum()
    
    print(f"  Min: {min_val}   Max: {max_val}")
    print(f"  Unique values: {unique_vals}")
    print(f"  Missing values: {missing_count} ({missing_count / len(df) * 100:.3f}%)")
    
    # Check for invalids
    invalid_mask = ~df[column_name].between(expected_min, expected_max) & df[column_name].notna()
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        print(f"  -> FOUND {invalid_count} INVALID VALUES!")
        print("  Invalid value counts:")
        print(df.loc[invalid_mask, column_name].value_counts().head(15))
    else:
        print("  -> All non-missing values are in valid range!")
    
    # Special check for YEAR
    if expected_set is not None:
        unexpected = set(unique_vals) - expected_set
        if unexpected:
            print(f"  -> WARNING: Unexpected values found: {unexpected}")
        else:
            print("  -> Perfect match to expected set!")

# Run validations
validate_range_column('YEAR', 2015, 2015, expected_set={2015})
validate_range_column('MONTH', 1, 12)
validate_range_column('DAY', 1, 31)
validate_range_column('DAY_OF_WEEK', 1, 7)

print("\n" + "-"*60)
print("Date columns validation finished.")
print("If you see any invalid values or warnings -> tell me the output.")
print("If everything looks clean -> we can move to time columns next.")
print("-"*60)

#                                                 
# Step 3: Validate HHMM time columns (0000-2359)
#                                                 

print("\n" + "="*70)
print("STEP 3: VALIDATING TIME COLUMNS (SCHEDULED_DEPARTURE -> ARRIVAL_TIME)")
print("Expected format: integers/floats from 0000 to 2359 (HHMM)")
print("="*70)

time_columns = [
    'SCHEDULED_DEPARTURE',
    'DEPARTURE_TIME',
    'WHEELS_OFF',
    'WHEELS_ON',
    'SCHEDULED_ARRIVAL',
    'ARRIVAL_TIME'
]

def validate_time_column(col):
    if col not in df.columns:
        print(f"Column '{col}' not found!")
        return
    
    print(f"\n-> Checking {col}")
    
    # Convert to float/int safely (some datasets have .0)
    df[col] = pd.to_numeric(df[col], errors='coerce')
    
    min_val = df[col].min()
    max_val = df[col].max()
    missing = df[col].isna().sum()
    total_rows = len(df)
    
    print(f"  Min: {min_val}")
    print(f"  Max: {max_val}")
    print(f"  Missing values: {missing} ({missing / total_rows * 100:.3f}%)")
    
    # Invalid = outside 0-2359 OR NaN-but-we-report-separately
    invalid_mask = (df[col] < 0) | (df[col] > 2359) | df[col].isna()
    invalid_non_missing = ((df[col] < 0) | (df[col] > 2359)) & df[col].notna()
    invalid_count = invalid_non_missing.sum()
    
    if invalid_count > 0:
        print(f"  -> FOUND {invalid_count} INVALID VALUES (outside 0000-2359)!")
        print("  Top invalid values and counts:")
        print(df.loc[invalid_non_missing, col].value_counts().head(12))
        print("  Sample rows with invalid values (first 3):")
        print(df.loc[invalid_non_missing, ['YEAR','MONTH','DAY',col]].head(3))
    else:
        print("  -> All non-missing values are within 0000-2359 range!")
    
    # Quick histogram-like view (value bins)
    if invalid_count == 0 and missing < total_rows:
        bins = pd.cut(df[col].dropna(), bins=[0,600,1200,1800,2400], right=False)
        print("  Rough distribution (counts):")
        print(bins.value_counts().sort_index())

# Run check for each time column
for col in time_columns:
    validate_time_column(col)

print("\n" + "-"*70)
print("Time columns validation complete.")
print("Next steps depend on output:")
print("- If no invalids -> we can convert to proper datetime or HH:MM strings")
print("- If invalids found -> we will decide how to handle (replace with NaN, drop, etc.)")
print("- Then -> missing values overall, duplicates, etc.")
print("Reply with the output (especially any INVALID VALUES lines)!")
print("-"*70)

#                                                                                 
# STEP 4: CLEAN & STANDARDIZE TIME COLUMNS (Handle 2400 and force 4-digit format)
#                                                                                 

print("\n" + "="*80)
print("STEP 4 - SUMMARY OF FINDINGS & CLEANING TIME COLUMNS")
print("="*80)

print("""
What we found from Step 3 validation:
------------------------------------
- All time columns (SCHEDULED_DEPARTURE, DEPARTURE_TIME, WHEELS_OFF, 
  WHEELS_ON, SCHEDULED_ARRIVAL, ARRIVAL_TIME) have MAX = 2400 (or 2400.0)
- NO values > 2400 exist anywhere (no 2401, 2500, 9999, etc.)
- The ONLY invalid/out-of-range values are exactly 2400:
  - DEPARTURE_TIME: 513 cases
  - WHEELS_OFF:     727 cases
  - WHEELS_ON:      2005 cases
  - ARRIVAL_TIME:   2456 cases
  - SCHEDULED_ARRIVAL: 2 cases
  - SCHEDULED_DEPARTURE: 0 cases
- Many time values are stored as 1-3 digits (e.g. 5, 45, 615) or floats (615.0)
- Missing values (NaN) exist in some columns (~1.5%), we keep them for now

Why we replace 2400 with 0:
--------------------------
In this 2015 US flights dataset, 2400 is NOT an error - it is a known code 
used by some airlines to mean "midnight (00:00) of the NEXT calendar day".
However:
- Most analysis tools and datetime libraries expect 0000-2359 only
- 2400 breaks sorting, calculations, and binning
- Standard practice (used in most Kaggle notebooks for this dataset):
  -> Replace 2400 with 0 (treat as 00:00 same day)
This is safe because very few flights actually cross midnight, and the 
impact on delay calculations is minimal for most purposes.

Goal of this step:
------------------
1. Replace 2400 -> 0
2. Convert all time values to PROPER 4-DIGIT STRINGS (e.g. '0005', '0615', '2359')
   -> No more 1-digit, 2-digit, 3-digit or 5+ digit values
   -> Makes data consistent, readable, and ready for further processing
""")

# List of time columns to clean
time_cols = [
    'SCHEDULED_DEPARTURE',
    'DEPARTURE_TIME',
    'WHEELS_OFF',
    'WHEELS_ON',
    'SCHEDULED_ARRIVAL',
    'ARRIVAL_TIME'
]

#                                                 
# Part 1: Replace 2400 with 0 and convert to integer
#                                                 
print("\nCleaning step 1: Replacing 2400 -> 0 and converting to integer...")
for col in time_cols:
    # Replace 2400 (both int and float versions)
    df[col] = df[col].replace([2400, 2400.0], 0)
    # Force numeric (turn any remaining strings/strange values to NaN)
    df[col] = pd.to_numeric(df[col], errors='coerce')
    # Convert to nullable integer (keeps NaN)
    df[col] = df[col].astype('Int64')

#                                                 
# Part 2: Create 4-digit string versions (padded with zeros)
#                                                 
print("\nCleaning step 2: Converting all times to 4-digit strings (e.g. '0005', '0615')...")

for col in time_cols:
    # Create new string column with _STR suffix
    str_col = col + '_STR'

    # Create an empty column (object) and fill non-missing values with zero-padded 4-digit strings
    df[str_col] = pd.NA
    non_null_mask = df[col].notna()
    df.loc[non_null_mask, str_col] = (
        df.loc[non_null_mask, col]
          .astype(int)
          .astype(str)
          .str.zfill(4)
    )

    print(f"Created {str_col} (4-digit string version)")

#                                                 
# Verification
#                                                 
print("\n" + "-"*80)
print("VERIFICATION AFTER CLEANING")
print("-"*80)

print("Final min/max (numeric version):")
for col in time_cols:
    print(f"  {col:20} Min: {df[col].min():4}   Max: {df[col].max():4}")

print("\nCheck for any values still >2359 or <0 (should be 0):")
for col in time_cols:
    over = ((df[col] > 2359) | (df[col] < 0)).sum()
    print(f"  {col:20} Values outside 0-2359: {over}")

print("\nCheck length of string versions (all should be exactly 4 or None):")
for col in time_cols:
    str_col = col + '_STR'
    lengths = df[str_col].dropna().str.len().value_counts()
    print(f"  {str_col:20} Length distribution:\n{lengths}")

print("\nSample of cleaned data (first 8 rows of time columns):")
print(df[['SCHEDULED_DEPARTURE', 'DEPARTURE_TIME', 'ARRIVAL_TIME',
          'SCHEDULED_DEPARTURE_STR', 'DEPARTURE_TIME_STR', 'ARRIVAL_TIME_STR']].head(8))

print("\n" + "="*80)

print("="*80)

#                                                                                 
# FINAL VALIDATION - STEP 4.5: Double-check everything is perfect
#                                                                                 

print("\n" + "="*90)
print("FINAL VALIDATION AFTER CLEANING - CONFIRMING ALL VALUES ARE CORRECT")
print("="*90)

time_cols = [
    'SCHEDULED_DEPARTURE',
    'DEPARTURE_TIME',
    'WHEELS_OFF',
    'WHEELS_ON',
    'SCHEDULED_ARRIVAL',
    'ARRIVAL_TIME'
]

all_good = True

print("\n1. Numeric columns - range check (should be 0 to 2359 or NaN only)")
print("-"*70)
for col in time_cols:
    min_val = df[col].min()
    max_val = df[col].max()
    invalid_low  = (df[col] < 0).sum()
    invalid_high = (df[col] > 2359).sum()
    nan_count    = df[col].isna().sum()
    
    print(f"{col:22} Min: {min_val:4}  Max: {max_val:4}  "
          f"Invalid <0: {invalid_low}  Invalid >2359: {invalid_high}  NaN: {nan_count}")
    
    if invalid_low > 0 or invalid_high > 0:
        all_good = False
        print(f"  -> PROBLEM DETECTED in {col}!")

print("\n2. String columns - length & content check (must be exactly '0000'-'2359' or None)")
print("-"*70)
for col in time_cols:
    str_col = col + '_STR'
    if str_col not in df.columns:
        print(f"{str_col} not found!")
        continue
    
    # Length check (already done, but confirm again)
    wrong_length = df[str_col].dropna().str.len().value_counts()
    if (wrong_length.index != [4]).any():
        all_good = False
        print(f"{str_col:22} -> Wrong lengths found: {wrong_length}")
    
    # Check if any string is not valid HHMM format
    invalid_format = df[str_col].dropna().str.match(r'^([01][0-9]|2[0-3])[0-5][0-9]$').value_counts()
    invalid_count = invalid_format.get(False, 0)
    
    print(f"{str_col:22} Valid HHMM format: {invalid_format.get(True, 0):,}   "
          f"Invalid format: {invalid_count:,}")
    
    if invalid_count > 0:
        all_good = False
        print(f"  -> Invalid time strings found in {str_col}")
        # Show examples of bad ones
        bad_examples = df[df[str_col].dropna().str.match(r'^([01][0-9]|2[0-3])[0-5][0-9]$') == False][str_col].head(3)
        if not bad_examples.empty:
            print("  Examples of invalid strings:", list(bad_examples))

print("\n" + "-"*90)
if all_good:
    print("ALL CHECKS PASSED!")
    print("   - Numeric times: 0-2359 only (or NaN)")
    print("   - String times: exactly 4 digits, valid HHMM format ('0000' to '2359')")
    print("   - No remaining 2400, no invalid ranges, no wrong lengths")
    print("Time columns are now FULLY CLEAN and VALIDATED.")
else:
    print("Some issues were found - review the output above and share it.")

print("\n" + "="*90)
print("Time cleaning & validation phase COMPLETE.")
print("You now have reliable, standardized time columns ready for analysis.")
print("="*90)

print("\nNext logical steps - choose one (or suggest something else):")
print("1. Create full datetime columns (combine date + time into proper pd.Timestamp)")
print("2. Handle missing values across the dataset (impute, drop, analyze patterns)")
print("3. Check for and remove duplicate rows (if any)")
print("4. Clean categorical columns (AIRLINE, ORIGIN_AIRPORT, DESTINATION_AIRPORT, etc.)")
print("5. Analyze cancellation & diversion columns")
print("6. Start feature engineering (e.g. delay buckets, flight duration, etc.)")
print("7. Save the cleaned dataset to a new file")
print("   -> Tell me your preference!")

#                                                                                 
# STEP 5 (FIXED): Mark Not Applicable (N/A) for Cancelled Flights
#                                                                                 

print("\n" + "="*80)
print("STEP 5 (FIXED): SET N/A FOR CANCELLED FLIGHTS")
print("Only clears columns that exist; skips non-existent _STR versions")
print("="*80)

# Core columns to null out when CANCELLED == 1
na_columns = [
    'DEPARTURE_TIME',
    'DEPARTURE_DELAY',
    'TAXI_OUT',
    'ELAPSED_TIME',
    'AIR_TIME',
    'WHEELS_ON',
    'TAXI_IN',
    'ARRIVAL_TIME',
    'ARRIVAL_DELAY'
]

# Only include _STR versions that actually exist in the DataFrame
existing_str_cols = [col + '_STR' for col in na_columns if col + '_STR' in df.columns]
print("String columns that will be cleared for cancelled flights:")
print(existing_str_cols)

#                                                 
# Count cancelled flights
#                                                 
cancelled_mask = (df['CANCELLED'] == 1)
cancelled_count = cancelled_mask.sum()
total_rows = len(df)
print(f"\nTotal cancelled flights: {cancelled_count} ({cancelled_count / total_rows * 100:.3f}%)")

#                                                 
# Before: non-NaN counts in cancelled flights (numeric columns)
#                                                 
print("\nBefore - non-NaN counts among cancelled flights (numeric):")
for col in na_columns:
    if col in df.columns:
        non_nan = df.loc[cancelled_mask, col].notna().sum()
        print(f"  {col:18}: {non_nan:6} non-NaN")

#                                                 
# Apply N/A: set numeric to NaN, existing _STR to None
#                                                 
print("\nSetting NaN / None for cancelled flights...")
df.loc[cancelled_mask, na_columns] = pd.NA

for str_col in existing_str_cols:
    df.loc[cancelled_mask, str_col] = None

#                                                 
# After verification
#                                                 
print("\nAfter - non-NaN counts among cancelled flights (should be 0):")
all_clean_numeric = True
for col in na_columns:
    if col in df.columns:
        non_nan_after = df.loc[cancelled_mask, col].notna().sum()
        print(f"  {col:18}: {non_nan_after}")
        if non_nan_after > 0:
            all_clean_numeric = False

print("\nAfter - non-None counts in existing _STR columns for cancelled flights:")
all_clean_str = True
for str_col in existing_str_cols:
    non_none_after = df.loc[cancelled_mask, str_col].notna().sum()
    print(f"  {str_col:22}: {non_none_after}")
    if non_none_after > 0:
        all_clean_str = False

# Sample (safe version - only existing columns)
print_cols = ['YEAR', 'MONTH', 'DAY', 'CANCELLED', 'CANCELLATION_REASON']
print_cols += [c for c in na_columns[:4] if c in df.columns]  # up to 4 numeric
print_cols += [c for c in existing_str_cols[:3] if c in df.columns]  # up to 3 string

if cancelled_count > 0:
    print(f"\nSample of {min(3, cancelled_count)} cancelled flights after cleaning:")
    print(df[cancelled_mask][print_cols].head(3))
else:
    print("\nNo cancelled flights in the dataset - nothing changed.")

print("\n" + "-"*80)
if all_clean_numeric and all_clean_str:
    print(" SUCCESS: All targeted columns are now NaN / None for cancelled flights.")
else:
    print("Note: Some checks may still show values - share output if unexpected.")


#                                                                                 
# STEP 6 (FIXED): VALIDATE CODES USING LOOKUP FILES
#                                                                                 

print("\n" + "="*90)
print("STEP 6 (FIXED): VALIDATE AIRLINE / AIRPORT CODES AGAINST LOOKUP FILES")
print("Using airlines.csv and airports.csv as source of truth")
print("="*90)

# folder is already set to the DATA folder near this script
# (re-used for lookups below)

# Load lookups
try:
    df_airlines = pd.read_csv(os.path.join(folder, 'airlines.csv'))
    valid_airlines = set(df_airlines['IATA_CODE'].dropna().astype(str).unique())
    print(f"Loaded airlines lookup -> {len(valid_airlines)} unique codes")
    print("Airline codes in lookup:", sorted(valid_airlines))
except Exception as e:
    print("Error loading airlines.csv:", e)
    valid_airlines = set()

try:
    df_airports = pd.read_csv(os.path.join(folder, 'airports.csv'))
    valid_airports = set(df_airports['IATA_CODE'].dropna().astype(str).unique())
    print(f"Loaded airports lookup -> {len(valid_airports)} unique codes")
except Exception as e:
    print("Error loading airports.csv:", e)
    valid_airports = set()

#                                                 
# Validate AIRLINE (force string)
#                                                 
print("\n-> Checking AIRLINE column")
if 'AIRLINE' in df.columns:
    df['AIRLINE'] = df['AIRLINE'].astype(str).replace('nan', pd.NA)
    unique_airlines = set(df['AIRLINE'].dropna().unique())
    invalid_airlines = unique_airlines - valid_airlines
    matched_airlines = len(unique_airlines) - len(invalid_airlines)
    
    print(f"  Unique airline codes in main file: {len(unique_airlines)}")
    print(f"  Matched in lookup: {matched_airlines}")
    print(f"  Invalid / missing in lookup: {len(invalid_airlines)}")
    
    if invalid_airlines:
        print("  Invalid airline codes found:")
        print("   ", sorted(invalid_airlines))
        print("\n  Rows with invalid airline codes (top counts):")
        print(df[df['AIRLINE'].isin(invalid_airlines)]['AIRLINE'].value_counts().head(15))
    else:
        print("  -> All airline codes are valid! Good.")
else:
    print("  AIRLINE column not found.")

#                                                 
# Validate airports (force string)
#                                                 
print("\n-> Checking airport codes (ORIGIN_AIRPORT + DESTINATION_AIRPORT)")
if 'ORIGIN_AIRPORT' in df.columns and 'DESTINATION_AIRPORT' in df.columns:
    # Force string and clean
    df['ORIGIN_AIRPORT'] = df['ORIGIN_AIRPORT'].astype(str).replace('nan', pd.NA)
    df['DESTINATION_AIRPORT'] = df['DESTINATION_AIRPORT'].astype(str).replace('nan', pd.NA)
    
    all_airport_codes = set(df['ORIGIN_AIRPORT'].dropna().unique()) | set(df['DESTINATION_AIRPORT'].dropna().unique())
    invalid_airports = all_airport_codes - valid_airports
    matched_airports = len(all_airport_codes) - len(invalid_airports)
    
    print(f"  Unique airport codes in main file: {len(all_airport_codes)}")
    print(f"  Matched in lookup: {matched_airports}")
    print(f"  Invalid / missing in lookup: {len(invalid_airports)}")
    
    if invalid_airports:
        print("  Invalid airport codes found (sorted, first 30):")
        print("   ", sorted(list(invalid_airports))[:30])
        print("\n  Rows with invalid origin airports (top counts):")
        print(df[df['ORIGIN_AIRPORT'].isin(invalid_airports)]['ORIGIN_AIRPORT'].value_counts().head(10))
        print("\n  Rows with invalid destination airports (top counts):")
        print(df[df['DESTINATION_AIRPORT'].isin(invalid_airports)]['DESTINATION_AIRPORT'].value_counts().head(10))
    else:
        print("  -> All airport codes are valid! Good.")
else:
    print("  Airport columns not found.")

print("\n" + "-"*90)
print("Validation complete.")

#                                                                                 
# STEP 6.5: COUNT RECORDS WITH INVALID AIRPORT CODES
#                                                                                 

print("\n" + "="*90)
print("STEP 6.5: NUMBER OF RECORDS WITH INVALID AIRPORT CODES")
print("Invalid = code not present in airports.csv lookup")
print("="*90)

# Force string type (just in case)
df['ORIGIN_AIRPORT']      = df['ORIGIN_AIRPORT'].astype(str)
df['DESTINATION_AIRPORT'] = df['DESTINATION_AIRPORT'].astype(str)

# Masks for valid / invalid
valid_origin      = df['ORIGIN_AIRPORT'].isin(valid_airports)
valid_destination = df['DESTINATION_AIRPORT'].isin(valid_airports)

valid_both        = valid_origin & valid_destination
invalid_any       = ~valid_both

total_rows = len(df)

print(f"Total records in dataset: {total_rows:,}")
print(f"Records with BOTH airports valid (match lookup): {valid_both.sum():,} ({valid_both.mean()*100:.2f}%)")
print(f"Records with AT LEAST ONE invalid airport code: {invalid_any.sum():,} ({invalid_any.mean()*100:.2f}%)")

print("\nBreakdown:")
print(f"  - Invalid ORIGIN_AIRPORT only:      {(~valid_origin & valid_destination).sum():,} rows")
print(f"  - Invalid DESTINATION_AIRPORT only: {(valid_origin & ~valid_destination).sum():,} rows")
print(f"  - Both invalid:                     {((~valid_origin) & (~valid_destination)).sum():,} rows")

print("\nTop 10 most frequent invalid airport codes overall (origin + destination):")
invalid_codes_combined = pd.concat([
    df[~valid_origin]['ORIGIN_AIRPORT'].value_counts(),
    df[~valid_destination]['DESTINATION_AIRPORT'].value_counts()
]).groupby(level=0).sum().sort_values(ascending=False)

print(invalid_codes_combined.head(10))

print("\n" + "-"*90)

#                                                                                 
# STEP 6.6: REMOVE ROWS WITH INVALID (non-IATA) AIRPORT CODES
#                                                                                 

print("\n" + "="*90)
print("STEP 6.6: REMOVE ROWS WITH INVALID AIRPORT CODES")
print("Keeping only rows where BOTH origin and destination match airports.csv")
print("="*90)

before_rows = len(df)

# Masks (from previous step)
valid_origin      = df['ORIGIN_AIRPORT'].isin(valid_airports)
valid_destination = df['DESTINATION_AIRPORT'].isin(valid_airports)
keep_mask         = valid_origin & valid_destination

df_clean = df[keep_mask].copy()  # or df = df[keep_mask] if you want to overwrite

after_rows = len(df_clean)
removed_rows = before_rows - after_rows
removed_pct = removed_rows / before_rows * 100

print(f"Before removal: {before_rows:,} rows")
print(f"After removal : {after_rows:,} rows")
print(f"Removed       : {removed_rows:,} rows ({removed_pct:.2f}%)")

print("\nPercentage kept: {:.2f}%".format(100 - removed_pct))

# Optional: quick sanity check
print("\nRemaining unique airport codes (should be <= 322):", 
      len(set(df_clean['ORIGIN_AIRPORT'].unique()) | set(df_clean['DESTINATION_AIRPORT'].unique())))

print("\n" + "-"*90)
print("Rows with invalid airport codes have been removed.")
print("You can now safely join the lookup files without missing metadata.")

#                                                                                 
# STEP 7 (FIXED & FINAL): Set 'NC' for not cancelled flights - on cleaned data
#                                                                                 

print("\n" + "="*90)
print("STEP 7 (FINAL): Set CANCELLATION_REASON to 'NC' - using CLEANED DATA only")
print("This step switches to df_clean (from Step 6.6) and updates it")
print("="*90)

# Safety check & switch to cleaned data
if 'df_clean' not in globals():
    print("ERROR: df_clean not found. Please re-run Step 6.6 first.")
else:
    # Switch to cleaned DataFrame
    df = df_clean.copy()
    
    # Format shape correctly (rows only for main display)
    rows_formatted = f"{df.shape[0]:,}"
    print(f"Switched to cleaned DataFrame -> shape: {rows_formatted} rows   {df.shape[1]} columns")
    
    # Remaining unique airport codes check
    unique_airports = len(set(df['ORIGIN_AIRPORT'].unique()) | set(df['DESTINATION_AIRPORT'].unique()))
    print(f"Unique airport codes remaining: {unique_airports} (should be  322)")

    # Before cleaning
    print("\nBefore - CANCELLATION_REASON value counts:")
    print(df['CANCELLATION_REASON'].value_counts(dropna=False))

    # Apply 'NC' logic
    df['CANCELLATION_REASON'] = df['CANCELLATION_REASON'].fillna('NC')
    df.loc[df['CANCELLED'] == 0, 'CANCELLATION_REASON'] = 'NC'

    # After cleaning
    print("\nAfter - CANCELLATION_REASON value counts:")
    print(df['CANCELLATION_REASON'].value_counts(dropna=False))

    print(f"\nTotal 'NC' (not cancelled): {(df['CANCELLATION_REASON'] == 'NC').sum():,}")
    print(f"Cancelled flights (should show A/B/C/D/etc.): {(df['CANCELLED'] == 1).sum():,}")

    # Samples
    print("\nSample non-cancelled flights (should show 'NC'):")
    print(df[df['CANCELLED'] == 0][['CANCELLED', 'CANCELLATION_REASON']].head(5))

    print("\nSample cancelled flights (should show real codes):")
    print(df[df['CANCELLED'] == 1][['CANCELLED', 'CANCELLATION_REASON']].head(5))

    print("\n" + "-"*90)
    print("STEP 7 COMPLETE")
    print("  - 'NC' is now set for all non-cancelled flights")
    print("  - Working DataFrame 'df' is now the cleaned & updated version")
    print("  - You can safely proceed to the join step (Step 8)")
    print("\nReply with 'yes', 'proceed to join', or 'looks good' to continue.")

#                                                                                 
# STEP 7b (FIXED): CLEAN DELAY COLUMNS - force digits, replace empty/missing with 0
#                                                                                 

print("\n" + "="*90)
print("STEP 7b (FIXED): CLEAN DELAY COLUMNS")
print("Goal: only integers   0, fill empty/whitespace/missing with 0")
print("="*90)

delay_cols = [
    'AIR_SYSTEM_DELAY',
    'SECURITY_DELAY',
    'AIRLINE_DELAY',
    'LATE_AIRCRAFT_DELAY',
    'WEATHER_DELAY'
]

# Before cleaning
print("\nBefore cleaning:")
print("Data types:")
print(df[delay_cols].dtypes.to_string())

print("\nMissing (NaN) counts:")
print(df[delay_cols].isna().sum().to_string())

print("\nEmpty string or whitespace counts (before replacement):")
for col in delay_cols:
    # Count rows where value is '' or only whitespace (after converting to string)
    empty_count = (df[col].astype(str).str.strip() == '').sum()
    print(f"  {col:22}: {empty_count:,} rows")

# Step 1: Replace empty strings / whitespace-only with NaN
for col in delay_cols:
    df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)

# Step 2: Convert to numeric - non-numeric -> NaN
for col in delay_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Step 3: Fill remaining NaN with 0
df[delay_cols] = df[delay_cols].fillna(0)

# Step 4: Force integer (delay minutes are whole numbers)
df[delay_cols] = df[delay_cols].astype(int)

#                                                 
# Verification
#                                                 
print("\n" + "-"*50)
print("AFTER CLEANING:")

print("\nData types (should be int64):")
print(df[delay_cols].dtypes.to_string())

print("\nMissing (NaN) counts (should be 0):")
print(df[delay_cols].isna().sum().to_string())

print("\nMin / Max / Mean / Std:")
print(df[delay_cols].describe().round(1))

print("\nSample (first 8 rows):")
print(df[delay_cols].head(8))

print("\nTotal rows with at least one non-zero delay value:")
print((df[delay_cols] > 0).any(axis=1).sum(), "rows")

print("\nAny negative values? (should be 0):")
print((df[delay_cols] < 0).sum().to_string())

print("\n" + "-"*90)
print("Finished successfully.")
print("All delay columns now contain only integers   0.")
print("Empty strings, whitespace, and missing values are now 0.")
print("Ready for next step (join lookups, datetime, save, etc.)")

#                                                                                 
# STEP 8 (FIXED & FINAL): JOIN AIRLINES & AIRPORTS LOOKUPS ON CLEANED DATA
#                                                                                 

print("\n" + "="*90)
print("STEP 8 (FINAL): JOINING AIRLINES & AIRPORTS LOOKUPS")
print("Using only the cleaned DataFrame (df_clean from Step 6.6)")
print("All airport codes are already valid IATA -> no unmatched rows expected")
print("="*90)

# folder is already set to the DATA folder near this script

# Safety check: make sure df_clean exists
if 'df_clean' not in globals():
    print("ERROR: df_clean not found. Please re-run Step 6.6 first.")
else:
    print(f"Starting with cleaned DataFrame -> shape: {df_clean.shape[0]:,} rows   {df_clean.shape[1]} columns")

    # Load lookups
    df_airlines = pd.read_csv(os.path.join(folder, 'airlines.csv'))
    df_airports = pd.read_csv(os.path.join(folder, 'airports.csv'))

    print(f"\nAirlines lookup loaded: {len(df_airlines)} rows")
    print(f"Airports lookup loaded: {len(df_airports)} rows")

    #                                                 
    # Join airlines - add full name
    #                                                 
    print("\nJoining airlines...")
    df_enriched = df_clean.merge(
        df_airlines.rename(columns={
            'IATA_CODE': 'AIRLINE',
            'AIRLINE': 'AIRLINE_FULL_NAME'
        }),
        on='AIRLINE',
        how='left'
    )

    unmatched_airline = df_enriched['AIRLINE_FULL_NAME'].isna().sum()
    print(f"  Unmatched airline rows: {unmatched_airline} (should be 0)")

    #                                                 
    # Join origin airport
    #                                                 
    print("\nJoining origin airports...")
    df_enriched = df_enriched.merge(
        df_airports.rename(columns={
            'IATA_CODE': 'ORIGIN_AIRPORT',
            'AIRPORT': 'ORIGIN_AIRPORT_NAME',
            'CITY': 'ORIGIN_CITY',
            'STATE': 'ORIGIN_STATE',
            'LATITUDE': 'ORIGIN_LAT',
            'LONGITUDE': 'ORIGIN_LON'
        }),
        on='ORIGIN_AIRPORT',
        how='left'
    )

    unmatched_origin = df_enriched['ORIGIN_AIRPORT_NAME'].isna().sum()
    print(f"  Unmatched origin airports: {unmatched_origin} (should be 0)")

    #                                                 
    # Join destination airport
    #                                                 
    print("\nJoining destination airports...")
    df_enriched = df_enriched.merge(
        df_airports.rename(columns={
            'IATA_CODE': 'DESTINATION_AIRPORT',
            'AIRPORT': 'DESTINATION_AIRPORT_NAME',
            'CITY': 'DESTINATION_CITY',
            'STATE': 'DESTINATION_STATE',
            'LATITUDE': 'DESTINATION_LAT',
            'LONGITUDE': 'DESTINATION_LON'
        }),
        on='DESTINATION_AIRPORT',
        how='left'
    )

    unmatched_dest = df_enriched['DESTINATION_AIRPORT_NAME'].isna().sum()
    print(f"  Unmatched destination airports: {unmatched_dest} (should be 0)")

    #                                                 
    # Final checks & sample
    #                                                 
    print("\nNew columns added:")
    print([
        'AIRLINE_FULL_NAME',
        'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY', 'ORIGIN_STATE', 'ORIGIN_LAT', 'ORIGIN_LON',
        'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY', 'DESTINATION_STATE', 'DESTINATION_LAT', 'DESTINATION_LON'
    ])

    print("\nSample enriched data (first 5 rows):")
    sample_cols = [
        'AIRLINE', 'AIRLINE_FULL_NAME',
        'ORIGIN_AIRPORT', 'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY',
        'DESTINATION_AIRPORT', 'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY'
    ]
    print(df_enriched[sample_cols].head(5))

    print("\nFinal shape:", f"{df_enriched.shape[0]:,} rows   {df_enriched.shape[1]} columns")

    print("\nQuick NaN check in new columns (should all be 0):")
    new_cols_check = [
        'AIRLINE_FULL_NAME', 'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY', 'ORIGIN_STATE', 'ORIGIN_LAT', 'ORIGIN_LON',
        'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY', 'DESTINATION_STATE', 'DESTINATION_LAT', 'DESTINATION_LON'
    ]
    print(df_enriched[new_cols_check].isna().sum().to_string())

    print("\n" + "-"*90)
    print("JOIN COMPLETE")
    

#                                                                                 
# STEP 8.3: CHECK MISSING VALUES IN AIRPORT LOOKUP COLUMNS BY IATA_CODE
#                                                                                 

print("\n" + "="*90)
print("STEP 8.3: MISSING VALUES IN AIRPORT METADATA COLUMNS - GROUPED BY IATA_CODE")
print("This shows which airports (IATA codes) are missing name/city/state/lat/lon")
print("="*90)

# List of new columns from airport lookup
airport_meta_cols = [
    'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY', 'ORIGIN_STATE', 'ORIGIN_LAT', 'ORIGIN_LON',
    'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY', 'DESTINATION_STATE', 'DESTINATION_LAT', 'DESTINATION_LON'
]

# Overall missing count
print("\nTotal missing values per column:")
missing_summary = df_enriched[airport_meta_cols].isna().sum()
print(missing_summary.to_string())

# Group by airport code to find which IATA codes are causing the missing values
print("\n" + "-"*50)
print("Missing ORIGIN metadata - grouped by ORIGIN_AIRPORT (top 10):")
origin_missing = df_enriched[df_enriched['ORIGIN_AIRPORT_NAME'].isna() | 
                             df_enriched['ORIGIN_CITY'].isna() | 
                             df_enriched['ORIGIN_LAT'].isna()][['ORIGIN_AIRPORT']].value_counts().head(10)
print(origin_missing.to_string())

print("\nMissing DESTINATION metadata - grouped by DESTINATION_AIRPORT (top 10):")
dest_missing = df_enriched[df_enriched['DESTINATION_AIRPORT_NAME'].isna() | 
                           df_enriched['DESTINATION_CITY'].isna() | 
                           df_enriched['DESTINATION_LAT'].isna()][['DESTINATION_AIRPORT']].value_counts().head(10)
print(dest_missing.to_string())

# If you want to see full rows for a specific airport (change code below)
specific_code = 'UST'  #   change to any code you want to inspect, e.g. 'PBG', 'ECP', 'UST'
print(f"\nSample rows where ORIGIN_AIRPORT == '{specific_code}' (check lat/lon):")
print(df_enriched[df_enriched['ORIGIN_AIRPORT'] == specific_code][
    ['ORIGIN_AIRPORT', 'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY', 'ORIGIN_STATE', 'ORIGIN_LAT', 'ORIGIN_LON']
].head(3))

print(f"\nSample rows where DESTINATION_AIRPORT == '{specific_code}' (check lat/lon):")
print(df_enriched[df_enriched['DESTINATION_AIRPORT'] == specific_code][
    ['DESTINATION_AIRPORT', 'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY', 'DESTINATION_STATE', 'DESTINATION_LAT', 'DESTINATION_LON']
].head(3))

print("\n" + "-"*90)
print("Done. This shows exactly which IATA codes are missing metadata.")
print("If missing counts are now very low (only UST/PBG/ECP), we can fill them manually.")
print("Next? Reply with:")
print("1. Fill lat/lon for UST/PBG/ECP (or others)")
print("2. Drop rows with any missing lat/lon")
print("3. Keep as-is and move to next task")
print("4. Save current df_enriched")
print("5. Check duplicates or create datetime columns")

#                                                                                 
# STEP 8.4: FILL MISSING LAT/LON FOR ECP, PBG, UST ONLY
#                                                                                 

print("\n" + "="*90)
print("STEP 8.4: FILL LAT/LON FOR THE 3 MISSING AIRPORTS (ECP, PBG, UST)")
print("These were the only codes missing coordinates in airports.csv")
print("="*90)

# Dictionary of coordinates (source: FAA / official airport data)
coords_fill = {
    'ECP': {'lat': 30.3583, 'lon': -85.7956},
    'PBG': {'lat': 44.6509, 'lon': -73.4682},
    'UST': {'lat': 29.9592, 'lon': -81.3397}
}

# Fill for ORIGIN airports
for code, vals in coords_fill.items():
    mask = (df_enriched['ORIGIN_AIRPORT'] == code)
    df_enriched.loc[mask, 'ORIGIN_LAT'] = vals['lat']
    df_enriched.loc[mask, 'ORIGIN_LON'] = vals['lon']

# Fill for DESTINATION airports
for code, vals in coords_fill.items():
    mask = (df_enriched['DESTINATION_AIRPORT'] == code)
    df_enriched.loc[mask, 'DESTINATION_LAT'] = vals['lat']
    df_enriched.loc[mask, 'DESTINATION_LON'] = vals['lon']

# Verification
print("\nAfter filling - remaining missing lat/lon:")
print(f"  ORIGIN_LAT / ORIGIN_LON missing: {df_enriched['ORIGIN_LAT'].isna().sum():,}")
print(f"  DESTINATION_LAT / DESTINATION_LON missing: {df_enriched['DESTINATION_LAT'].isna().sum():,}")

print("\nRows updated per airport (origin + destination):")
for code in ['ECP', 'PBG', 'UST']:
    origin_count = (df_enriched['ORIGIN_AIRPORT'] == code).sum()
    dest_count = (df_enriched['DESTINATION_AIRPORT'] == code).sum()
    total = origin_count + dest_count
    print(f"  {code}: {origin_count:,} origin + {dest_count:,} dest = {total:,} rows updated")

print("\nSample rows with filled coordinates (first few for each):")
for code in ['ECP', 'PBG', 'UST']:
    sample_origin = df_enriched[df_enriched['ORIGIN_AIRPORT'] == code][
        ['ORIGIN_AIRPORT', 'ORIGIN_AIRPORT_NAME', 'ORIGIN_LAT', 'ORIGIN_LON']
    ].head(2)
    sample_dest = df_enriched[df_enriched['DESTINATION_AIRPORT'] == code][
        ['DESTINATION_AIRPORT', 'DESTINATION_AIRPORT_NAME', 'DESTINATION_LAT', 'DESTINATION_LON']
    ].head(2)
    if not sample_origin.empty:
        print(f"\nOrigin {code} sample:\n{sample_origin}")
    if not sample_dest.empty:
        print(f"\nDestination {code} sample:\n{sample_dest}")
# List of new columns from airport lookup
airport_meta_cols = [
    'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY', 'ORIGIN_STATE', 'ORIGIN_LAT', 'ORIGIN_LON',
    'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY', 'DESTINATION_STATE', 'DESTINATION_LAT', 'DESTINATION_LON'
]

# Overall missing count
print("\nTotal missing values per column:")
missing_summary = df_enriched[airport_meta_cols].isna().sum()
print(missing_summary.to_string())
print("\n" + "-"*90)
print("Filled successfully for ECP, PBG, UST only.")

#                                                                                 
# STEP 9 (ENHANCED): CHECK DUPLICATES + VALIDATE NO JUNK TEXT IN KEY COLUMNS
#                                                                                 

print("\n" + "="*90)
print("STEP 9: CHECK FOR DUPLICATE ROWS & VALIDATE NO JUNK TEXT")
print("1. Duplicate row check")
print("2. Junk text validation in key categorical/string columns")
print("="*90)

# Part 1: Duplicate rows
total_rows = len(df_enriched)
duplicates = df_enriched.duplicated().sum()
duplicate_pct = (duplicates / total_rows) * 100 if total_rows > 0 else 0

print(f"\nTotal rows: {total_rows:,}")
print(f"Number of full duplicate rows: {duplicates:,} ({duplicate_pct:.4f}%)")
print(f"Unique rows: {total_rows - duplicates:,}")

if duplicates > 0:
    print("\nSample of duplicate rows (first 5):")
    print(df_enriched[df_enriched.duplicated(keep=False)].head(5))
else:
    print("\nNo full duplicate rows found - good.")

# Part 2: Junk text validation - key columns to check
key_cols_to_check = [
    'AIRLINE', 'AIRLINE_FULL_NAME',
    'ORIGIN_AIRPORT', 'ORIGIN_AIRPORT_NAME', 'ORIGIN_CITY', 'ORIGIN_STATE',
    'DESTINATION_AIRPORT', 'DESTINATION_AIRPORT_NAME', 'DESTINATION_CITY', 'DESTINATION_STATE',
    'TAIL_NUMBER', 'CANCELLATION_REASON'
]

print("\n" + "-"*50)
print("Junk text / invalid character check in key columns:")
print("Looking for: non-printable chars, excessive symbols, unexpected patterns")

for col in key_cols_to_check:
    if col not in df_enriched.columns:
        continue
        
    # Convert to string safely
    s = df_enriched[col].astype(str)
    
    # Check for non-printable characters (ASCII control chars)
    has_non_printable = s.str.contains(r'[\x00-\x1F\x7F]', regex=True).sum()
    
    # Check for excessive special characters (more than 5 non-alphanumeric per value)
    excessive_special = s.str.count(r'[^a-zA-Z0-9\s\-\./]').gt(5).sum()
    
    # Check for pure numbers in columns that should be text (e.g. airline codes)
    if col in ['AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']:
        pure_numeric = s.str.match(r'^\d+$').sum()
    else:
        pure_numeric = 0
    
    print(f"\n{col:30} ->")
    print(f"  Non-printable chars found: {has_non_printable:,}")
    print(f"  Values with >5 special chars: {excessive_special:,}")
    print(f"  Pure numeric (where shouldn't be): {pure_numeric:,}")

    # If issues found, show examples
    if has_non_printable > 0 or excessive_special > 0 or pure_numeric > 0:
        print("  Sample problematic values:")
        bad_mask = (
            s.str.contains(r'[\x00-\x1F\x7F]', regex=True) |
            s.str.count(r'[^a-zA-Z0-9\s\-\./]').gt(5) |
            (s.str.match(r'^\d+$') if col in ['AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT'] else False)
        )
        print(s[bad_mask].unique()[:10])  # first 10 unique bad values

print("\n" + "-"*90)
print("Validation complete.")
print("If no junk detected (0 or very low counts), dataset is clean of invalid text.")
print("\nNext steps - choose one:")
print("1. Drop any duplicate rows if found")
print("2. Create full datetime columns (scheduled & actual departure/arrival)")
print("3. Save final enriched dataset to CSV (recommended now)")
print("4. Analyze remaining missing values (if any)")
print("5. Start basic analysis / stats / visualization")
print(" -> Reply with your choice (1-5) or 'looks good - save now'")

#                                                                                 
# STEP 10: SAVE FINAL ENRICHED & CLEANED DATASET
#                                                                                 

print("\n" + "="*90)
print("STEP 10: SAVE FINAL CLEANED & ENRICHED DATASET")
print("Saving df_enriched (5,332,914 rows   50 columns)")
print("="*90)

# output folder is the same DATA folder used above
output_folder = folder
output_file = os.path.join(output_folder, 'flights_CLEANED_ENRICHED.csv')

# Optional: compress to save space (gzip)
# output_file = os.path.join(output_folder, 'flights_CLEANED_ENRICHED.csv.gz')

print(f"Saving to: {output_file}")

# Save - use index=False to avoid extra column
df_enriched.to_csv(output_file, index=False)

print(f"\nFile saved successfully!")
print(f"Size on disk will be approximately {df_enriched.memory_usage(deep=True).sum() / (1024**3):.2f} GB (uncompressed)")

# Optional: quick file size check
if os.path.exists(output_file):
    file_size_gb = os.path.getsize(output_file) / (1024**3)
    print(f"Actual file size: {file_size_gb:.2f} GB")
else:
    print("File not found after save - check path or permissions.")

print("\n" + "-"*90)
print("SAVE COMPLETE")
