import csv       # Writing processed data to CSV files
import requests  # Making API requests to the CMS data source
import pandas as pd  # Handling tabular data efficiently
import os        # File system operations (creating directories, file paths)
import datetime  # Tracking and comparing modification times of datasets
import json      # Parsing JSON responses from the API
import re        # Text processing, including renaming columns to snake_case

# Define API endpoint for retrieving CMS provider datasets
url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"

# Fetch the dataset metadata from the API
response = requests.get(url)
if response.status_code != 200:
    print(f"Error fetching data: {response.status_code}")
    exit()

data = response.json()

# Extract unique themes to explore available dataset categories
themes = set()
for dataset in data:
    if "theme" in dataset and isinstance(dataset["theme"], list):
        themes.update(dataset["theme"])  

# Print available themes
print("Unique Themes in the CMS Data:")
for theme in sorted(themes):
    print(theme)

# Extract media types for datasets with theme "Hospitals"
media_types = set()
for dataset in data:
    if "theme" in dataset and "Hospitals" in dataset["theme"]:
        if "distribution" in dataset:
            for dist in dataset["distribution"]:
                media_types.add(dist.get("mediaType", "Unknown"))

# Print media types to verify available file formats
print("Unique media types for theme='Hospitals':")
for media_type in sorted(media_types):
    print(media_type)

# Extract relevant metadata for hospital-related datasets
records = []
for dataset in data:
    if "theme" in dataset and "Hospitals" in dataset["theme"]:
        identifier = dataset.get("identifier", "Unknown")
        issued = dataset.get("issued", "Unknown")
        modified = dataset.get("modified", "Unknown")
        released = dataset.get("released", "Unknown")

        # Extract first available download URL
        download_url = "Unknown"
        if "distribution" in dataset and isinstance(dataset["distribution"], list):
            for dist in dataset["distribution"]:
                if "downloadURL" in dist:
                    download_url = dist["downloadURL"]
                    break  

        # Append dataset details
        records.append([identifier, download_url, issued, modified, released])

# Convert to DataFrame for structured processing
df_hospitals = pd.DataFrame(records, columns=["identifier", "downloadURL", "issued", "modified", "released"])
print(df_hospitals)

# Define tracking file for change detection
tracking_file = "tracking_file.csv"

# Ensure tracking file exists; if not, initialize an empty DataFrame
if os.path.exists(tracking_file):
    df_tracking = pd.read_csv(tracking_file, dtype=str)
else:
    df_tracking = pd.DataFrame(columns=["identifier", "downloadURL", "modified"])

# Merge API results with tracking records
df_merged = df_hospitals.merge(df_tracking, on="identifier", how="left", suffixes=("", "_old"))

# Ensure 'modified' columns are datetime formatted for accurate comparisons
df_merged["modified"] = pd.to_datetime(df_merged["modified"], errors="coerce")
df_merged["modified_old"] = pd.to_datetime(df_merged["modified_old"], errors="coerce")

# Identify new or updated datasets
df_updates = df_merged[
    df_merged["modified_old"].isna() |  # New dataset
    (df_merged["modified"] > df_merged["modified_old"])  # Modified dataset
]

# Download directory setup
download_dir = os.path.join(os.getcwd(), "downloaded_csvs")
os.makedirs(download_dir, exist_ok=True)

# Download new or updated datasets
for _, row in df_updates.iterrows():
    file_name = f"{row['identifier']}_{row['modified'].strftime('%Y%m%d')}.csv"
    file_path = os.path.join(download_dir, file_name)

    if row["downloadURL"]:
        print(f"Downloading: {file_name}")
        csv_response = requests.get(row["downloadURL"])

        # Validate successful download
        if csv_response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(csv_response.content)
        else:
            print(f"⚠️ Failed to download {file_name}: HTTP {csv_response.status_code}")
    else:
        print(f"Skipping {row['identifier']}: No valid download URL.")

# Update tracking file
df_tracking_updated = df_hospitals.copy()
df_tracking_updated.to_csv(tracking_file, index=False)

print("Process Complete: Tracking file updated, new datasets downloaded.")

# Define input and output directories for cleaned CSVs
input_dir = download_dir
output_dir = os.path.join(os.getcwd(), "cleaned_csvs")
os.makedirs(output_dir, exist_ok=True)

def to_snake_case(column_name):
    """Convert column names to snake_case."""
    column_name = column_name.lower().strip()  
    column_name = re.sub(r'[^a-z0-9\s_]', '', column_name)  # Remove special characters except underscores
    column_name = re.sub(r'\s+', '_', column_name)  # Replace spaces with underscores
    return column_name

# Process and clean CSV files
for file in os.listdir(input_dir):
    if file.endswith(".csv"):  
        file_path = os.path.join(input_dir, file)

        try:
            df = pd.read_csv(file_path, low_memory=False)  
            
            # Convert column names to snake_case
            df.columns = [to_snake_case(col) for col in df.columns]

            # Save cleaned file
            cleaned_file_path = os.path.join(output_dir, file)
            df.to_csv(cleaned_file_path, index=False)
            print(f"Processed: {file}")

        except Exception as e:
            print(f"Error processing {file}: {e}")

print(f"\nAll CSVs processed. Cleaned files saved in: {output_dir}")
