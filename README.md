# CMS Assessment

This repository contains a script to fetch, process, and clean hospital-related datasets from the **CMS Provider Data API**.

## Files Included:
- **`CMS_Assessment_JosephManning.py`** → Python script that:
  - Fetches hospital datasets from CMS API.
  - Checks for updated datasets.
  - Downloads new datasets.
  - Converts column names to `snake_case`.
  - Saves cleaned datasets.

- **`requirements.txt`** → Dependencies needed to run the script.

## How to Run:
1. **Clone the repository** (or download the `.py` file manually):
   ```sh
   git clone https://github.com/jmanning1991/CMS-Assessment.git
   cd CMS-Assessment
   ```

2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

3. **Run the script:**
   ```sh
   python CMS_Assessment_JosephManning.py
   ```

## Notes:
- The script downloads hospital-related datasets and processes them.
- It ensures datasets are only downloaded when updated, avoiding redundant downloads.
- The processed files are saved in the `cleaned_csvs/` directory.
