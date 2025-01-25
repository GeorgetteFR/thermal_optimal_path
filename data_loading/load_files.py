import os
import requests
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def generate_url(pair, date):
    base_url = f"https://data.binance.vision/data/spot/daily/klines/{pair}/1s/"
    file_name = f"{pair}-1s-{date}.zip"
    return f"{base_url}{file_name}"

def download_parquet(pair, date, output_dir):
    """
    Downloads and saves the daily data for the given trading pair and date as a Parquet file.
    The Parquet files are saved in the specified output directory.

    Parameters:
        pair (str): The trading pair (e.g., ETHUSDT).
        date (str): The date in YYYY-MM-DD format.
        output_dir (str): The directory where the Parquet files will be saved.
    """
    # Generate the Binance data URL
    url = generate_url(pair, date)
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        print(f"Downloading {pair} data for {date}...")
        with ZipFile(BytesIO(response.content)) as zf:
            # Extract and read the CSV file
            csv_file = zf.namelist()[0]
            with zf.open(csv_file) as f:
                df = pd.read_csv(
                    f,
                    header=None,
                    usecols=[0, 1],  # Only load 'Open time' and 'Open' columns
                    names=["Open time", "Open"]  # Assign column names
                )
                
                # Create the pair-specific output directory if it doesn't exist
                pair_dir = os.path.join(output_dir, pair)
                os.makedirs(pair_dir, exist_ok=True)
                
                # Save the data as a Parquet file
                parquet_path = os.path.join(pair_dir, f"{pair}-1s-{date}.parquet")
                df.to_parquet(parquet_path, index=False)
                print(f"Saved {pair} data for {date} to {parquet_path}")
    else:
        print(f"Failed to download data for {pair} on {date}. Status code: {response.status_code}")

def process_date_range(pair, start_date, end_date, output_dir):
    """
    Downloads data for a given trading pair and date range, saving the files as Parquet files.

    Parameters:
        pair (str): The trading pair (e.g., ETHUSDT).
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
        output_dir (str): The directory where the Parquet files will be saved.
    """
    # Convert dates to datetime objects
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Iterate over the date range
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        download_parquet(pair, date_str, output_dir)
        current_date += timedelta(days=1)

def parallel_download(pairs, start_date, end_date, output_dir, max_workers=4):
    """
    Downloads data for multiple trading pairs in parallel.

    Parameters:
        pairs (list): List of trading pairs (e.g., ["ETHUSDT", "BTCUSDT"]).
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
        output_dir (str): The directory where the Parquet files will be saved.
        max_workers (int): The maximum number of threads to use for parallel downloads.
    """
    def process_pair(pair):
        process_date_range(pair, start_date, end_date, output_dir)

    # Use ThreadPoolExecutor for parallelism
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_pair, pairs)
