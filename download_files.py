import os
from pathlib import Path
import requests
import zipfile

BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"


# On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2010_4.csv
# On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2014_4.zip
LOCAL_SAVE_FOLDER  = "./downloaded_files"

Path(LOCAL_SAVE_FOLDER).mkdir(parents=True, exist_ok=True)
print(f"Save folder ready: {LOCAL_SAVE_FOLDER}")

YEARS_TO_DOWNLOAD  = [2021, 2022]


def download_one_month(year, month, save_folder):

    """
    Downloads flight data for a single month from the BTS website.
    Returns the path to the saved CSV file, or None if something went wrong.

    Parameters:
        year        : int  — e.g. 2023
        month       : int  — e.g. 6  (June)
        save_folder : str  — where to save the file locally
    """

    # Build the filename exactly as BTS names it
    zip_filename = (
        f"On_Time_Reporting_Carrier_On_Time_Performance_"
        f"1987_present_{year}_{month}.zip"
    )
    download_url = BTS_WEBSITE + zip_filename    #BTS_Website + s

    zip_save_path = os.path.join(save_folder, zip_filename)
    csv_save_path = zip_save_path.replace(".zip", ".csv")

    # If we already downloaded this file before, don't download it again.

    if os.path.exists(csv_save_path):
        print(f" Already downloaded: {year}-{month:02d}  (skipping)")
        return csv_save_path

    print(f"  Downloading {year}-{month} \n  from  {download_url}")

  
    try:
        # stream=True means we download the file in small chunks rather than
        # loading the whole thing into RAM first. This is essential for large files.
        response = requests.get(download_url, stream=True, timeout=120)
        response.raise_for_status()   # this crashes loudly if the URL returned an error

        # Write the ZIP file to disk, chunk by chunk (each chunk = 1 MB)
        with open(zip_save_path, "wb") as zip_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                zip_file.write(chunk)

        # Open the ZIP and pull out the CSV that's inside it
        with zipfile.ZipFile(zip_save_path, "r") as zip_ref:
            # Find the CSV file inside (there's always exactly one)
            csv_inside = [name for name in zip_ref.namelist() if name.endswith(".csv")][0]
            zip_ref.extract(csv_inside, save_folder)

            # Rename it to something clean and consistent
            extracted_path = os.path.join(save_folder, csv_inside)
            if os.path.exists(csv_save_path):
                os.remove(csv_save_path)   # avoid rename error
            os.rename(extracted_path, csv_save_path)

        # Remove the ZIP now that we have the CSV — free up disk space
        if os.path.exists(zip_save_path):
            os.remove(zip_save_path)

        # Print how big the file is so you can sanity check it
        file_size_mb = os.path.getsize(csv_save_path) / (1024 ** 2)
        print(f"  Done: {csv_save_path}  ({file_size_mb:.1f} MB)")

        # QUICK DATA QUALITY CHECK:
        # Count how many lines are in the file (approximate row count).
        # A healthy file should have 400,000+ rows.
        with open(csv_save_path, "r", encoding="utf-8", errors="replace") as f:
            row_count = sum(1 for _ in f) - 1   # subtract 1 for the header row
        print(f"  Row count: {row_count:,}  {'✅ looks good' if row_count > 400000 else '⚠️  seems low — double check!'}")

        return csv_save_path

    except Exception as error:
        print(f"  ERROR on {year}-{month:02d}: {error}")
        print(f"  Tip: Check your internet connection or try again in a few minutes.")

        # Make sure ZIP is deleted even if error happens
        if os.path.exists(zip_save_path):
            os.remove(zip_save_path)

        return None


for year in YEARS_TO_DOWNLOAD:
    print(f"\n  Year {year} ")

    for month in range(1, 4):   # months 1 through 12

        csv_file = download_one_month(year, month, LOCAL_SAVE_FOLDER)