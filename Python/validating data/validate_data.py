import pytest
import pandas as pd

# ── Read from local CSVs saved by the notebook ───────────────────────────────
# These are written by: weather_df.to_csv('sampled_weather_df.csv', index=False)
#                        field_df.to_csv('sampled_field_df.csv', index=False)
weather_df = pd.read_csv('sampled_weather_df.csv')
field_df   = pd.read_csv('sampled_field_df.csv')

# ── Expected schema ───────────────────────────────────────────────────────────
EXPECTED_WEATHER_COLUMNS = ['Weather_station_ID', 'Message', 'Measurement', 'Value']

EXPECTED_FIELD_COLUMNS = [
    'Field_ID', 'Elevation', 'Latitude', 'Longitude', 'Location',
    'Slope', 'Annual_rainfall', 'Min_temperature_C', 'Max_temperature_C',
    'Ave_temps', 'Soil_fertility', 'Soil_type', 'pH', 'Pollution_level',
    'Plot_size', 'Annual_yield', 'Crop_type', 'Standard_yield'
]

VALID_CROP_TYPES = ['cassava', 'wheat', 'tea', 'maize', 'rice', 'coffee', 'banana']

# ── Tests ─────────────────────────────────────────────────────────────────────
def test_read_weather_DataFrame_shape():
    assert weather_df.shape[1] == len(EXPECTED_WEATHER_COLUMNS), \
        f"Expected {len(EXPECTED_WEATHER_COLUMNS)} columns, got {weather_df.shape[1]}"

def test_read_field_DataFrame_shape():
    assert field_df.shape[1] == len(EXPECTED_FIELD_COLUMNS), \
        f"Expected {len(EXPECTED_FIELD_COLUMNS)} columns, got {field_df.shape[1]}"

def test_weather_DataFrame_columns():
    assert list(weather_df.columns) == EXPECTED_WEATHER_COLUMNS, \
        f"Weather columns mismatch:\n{list(weather_df.columns)}"

def test_field_DataFrame_columns():
    assert list(field_df.columns) == EXPECTED_FIELD_COLUMNS, \
        f"Field columns mismatch:\n{list(field_df.columns)}"

def test_field_DataFrame_non_negative_elevation():
    assert (field_df['Elevation'] >= 0).all(), \
        "Some Elevation values are negative."

def test_crop_types_are_valid():
    invalid = set(field_df['Crop_type'].dropna().unique()) - set(VALID_CROP_TYPES)
    assert not invalid, f"Invalid crop types found: {invalid}"

def test_positive_rainfall_values():
    assert (field_df['Annual_rainfall'] > 0).all(), \
        "Some Annual_rainfall values are zero or negative."