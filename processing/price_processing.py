import pandas as pd
import os

def process_crypto_data(data_path, output_path, cryptos, rolling_window=3600):
    """
    Process daily crypto data by adding various return metrics:
    - Percentage return
    - Rolling mean and std of returns over a 1-hour window
    - Rolling average of returns over the last 5 seconds (excluding the current second)
    - Rolling mean and std of the 5-second rolling average over a 1-hour window

    Parameters:
    ----------
    data_path : str
        Path to the folder containing the crypto data folders.
    output_path : str
        Path to the folder where processed files should be stored.
    cryptos : list of str
        List of cryptocurrencies to process (e.g., ['BTC', 'ETH', 'SOL']).
    rolling_window : int, optional
        Rolling window size in seconds for calculating mean and std (default=3600 seconds, 1 hour).

    Returns:
    -------
    None
    """
    for crypto in cryptos:
        input_folder = os.path.join(data_path, f"{crypto}USDT")
        output_folder = os.path.join(output_path, f"{crypto}USDT")
        
        # Ensure output directory exists
        os.makedirs(output_folder, exist_ok=True)
        
        # List all parquet files in the crypto's folder
        files = [f for f in os.listdir(input_folder) if f.endswith('.parquet')]
        
        for file in files:
            input_file_path = os.path.join(input_folder, file)
            output_file_path = os.path.join(output_folder, file)
            
            # Skip processing if the output file already exists
            if os.path.exists(output_file_path):
                print(f"Skipping {output_file_path}, already processed.")
                continue
            
            # Read the daily file
            df = pd.read_parquet(input_file_path)
            
            # Ensure the file contains the required columns
            if 'Open' not in df.columns:
                print(f"Skipping {input_file_path}, missing 'Open' column.")
                continue
            
            # Calculate returns (percentage change in price)
            df['return'] = df['Open'].pct_change()

            # Calculate rolling mean and std of returns over 1 hour
            df['return_mean'] = df['return'].rolling(window=rolling_window, min_periods=3599).mean()
            df['return_std'] = df['return'].rolling(window=rolling_window, min_periods=3599).std()

            # Calculate rolling average of returns over the last 5 seconds (excluding current second)
            df['rolling_return'] = df['return'].shift(1).rolling(window=5, min_periods=4).mean()

            # Calculate rolling mean and std of the 5-second rolling average over the last hour
            df['rolling_return_mean'] = df['rolling_return'].rolling(window=rolling_window, min_periods=3599).mean()
            df['rolling_return_std'] = df['rolling_return'].rolling(window=rolling_window, min_periods=3599).std()

            # Calculate rolling 5th, 25th, 75th, and 95th percentiles of returns over 1 hour
            df['rolling_return_5th'] = df['rolling_return'].rolling(window=rolling_window, min_periods=3599).quantile(0.05)
            df['rolling_return_25th'] = df['rolling_return'].rolling(window=rolling_window, min_periods=3599).quantile(0.25)
            df['rolling_return_75th'] = df['rolling_return'].rolling(window=rolling_window, min_periods=3599).quantile(0.75)
            df['rolling_return_95th'] = df['rolling_return'].rolling(window=rolling_window, min_periods=3599).quantile(0.95)
            
            # Save the processed file
            df.to_parquet(output_file_path, index=False)
            print(f"Processed and saved: {output_file_path}")

    print("Processing complete for all cryptos!")