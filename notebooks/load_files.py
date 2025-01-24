def generate_url(pair, date):
    base_url = f"https://data.binance.vision/data/spot/daily/klines/{pair}/1s/"
    file_name = f"{pair}-1s-{date}.zip"
    return f"{base_url}{file_name}"

def download_parquet(pair, date):
    url = generate_url(pair, date)
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        print(f"Downloading {date} data...")
        with ZipFile(BytesIO(response.content)) as zf:
            csv_file = zf.namelist()[0]
            with zf.open(csv_file) as f:
                df = pd.read_csv(f, header=None)

                year, month = date.split("-")[:2]
                parquet_dir = os.path.join(OUTPUT_DIR, year, month)
                os.makedirs(parquet_dir, exist_ok=True)
                parquet_path = os.path.join(parquet_dir, f"BTCUSDT-1s-{date}.parquet")
                df.to_parquet(parquet_path, index=False)

                print(f"Saved {date} data to {parquet_path}")
    else:
        print(f"Failed to download data for {date}. Status code: {response.status_code}")

def process_date_range(pair, start_date, end_date):
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        download_parquet(pair, date_str)
        current_date += timedelta(days=1)