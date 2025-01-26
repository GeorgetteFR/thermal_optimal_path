import numpy as np
import pandas as pd
import os
from helpers import *
from joblib import Parallel, delayed
from concurrent.futures import ThreadPoolExecutor, as_completed

def basic_strategy_new(crypto1_file, crypto2_file, metrics_file, minute=40):
    """
    Execute a trading strategy using data from parquet files containing detailed metrics.

    Parameters:
    ----------
    crypto1_file : str
        Path to the parquet file for the first cryptocurrency.
    crypto2_file : str
        Path to the parquet file for the second cryptocurrency.
    metrics_file : str
        Path to the parquet file containing the crypto couple metrics (e.g., df_avg).
    minute : int, optional
        Time threshold in minutes for managing trades (default=40).

    Returns:
    -------
    dict
        A dictionary containing daily returns, trade counts, and trade returns.
    """
    # Load data from parquet files
    crypto1_data = pd.read_parquet(crypto1_file)
    crypto2_data = pd.read_parquet(crypto2_file)
    metrics_data = pd.read_parquet(metrics_file)

    # Merge data on "Open time"
    merged_data = crypto1_data.merge(crypto2_data, on="Open time", suffixes=("_crypto1", "_crypto2"))
    merged_data = merged_data.merge(metrics_data, on="Open time")

    # Extract necessary columns
    total_money = 0
    threshold = 6

    crypto1_entry_price_long, crypto2_entry_price_long, crypto1_entry_price_short, crypto2_entry_price_short = 0, 0, 0, 0
    crypto1_long_timer, crypto2_long_timer, crypto1_short_timer, crypto2_short_timer = -1, -1, -1, -1

    daily_long_trades = 0
    daily_short_trades = 0
    daily_long_returns = 0
    daily_short_returns = 0

    total = len(merged_data)

    for i in range(total):
        row = merged_data.iloc[i]

        # Extract metrics for the current time step
        crypto1_price = row["Open_crypto1"]
        crypto2_price = row["Open_crypto2"]
        crypto1_return = row["rolling_return_crypto1"]
        crypto2_return = row["rolling_return_crypto2"]
        
        crypto1_5th = row["rolling_return_5th_crypto1"]
        crypto1_25th = row["rolling_return_25th_crypto1"]
        crypto1_75th = row["rolling_return_75th_crypto1"]
        crypto1_95th = row["rolling_return_95th_crypto1"]

        crypto2_5th = row["rolling_return_5th_crypto2"]
        crypto2_25th = row["rolling_return_25th_crypto2"]
        crypto2_75th = row["rolling_return_75th_crypto2"]
        crypto2_95th = row["rolling_return_95th_crypto2"]

        avg_metric = row["avg"]  # Replace "metric_column" with the column name for df_avg

        # Define conditions
        crypto2_flat, crypto1_flat, crypto2_grew, crypto1_grew, crypto2_fell, crypto1_fell = get_booleans(
            crypto2_return, crypto1_return, crypto2_5th, crypto2_95th, crypto2_25th, crypto2_75th,
            crypto1_5th, crypto1_95th, crypto1_25th, crypto1_75th
        )

        # Long management ETH against BTC
        total_money, crypto2_entry_price_long, took_trade = long_if_lag_and_grew(
            total_money, avg_metric, threshold, crypto1_grew, crypto2_flat, crypto2_entry_price_long, crypto2_price
        )
        if took_trade:
            daily_long_trades += 1
            daily_long_returns -= 1
            crypto2_long_timer = 0

        if crypto2_long_timer >= minute or (i == total - 1 and crypto2_entry_price_long > 0):
            ratio = crypto2_price / crypto2_entry_price_long
            total_money += ratio
            crypto2_entry_price_long = 0
            crypto2_long_timer = -1
        if crypto2_entry_price_long > 0:
            prev_total_money = total_money
            total_money, crypto2_entry_price_long = sell_if_price_changed(
                total_money, crypto2_entry_price_long, crypto2_price, crypto2_95th
            )
            if total_money != prev_total_money:
                daily_long_returns += total_money - prev_total_money
                crypto2_long_timer = -1
            else:
                crypto2_long_timer += 1

        # Long management BTC against ETH
        total_money, crypto1_entry_price_long, took_trade = long_if_lag_and_grew(
            total_money, -avg_metric, threshold, crypto2_grew, crypto1_flat, crypto1_entry_price_long, crypto1_price
        )
        if took_trade:
            daily_long_trades += 1
            daily_long_returns -= 1
            crypto1_long_timer = 0

        if crypto1_long_timer >= minute or (i == total - 1 and crypto1_entry_price_long > 0):
            ratio = crypto1_price / crypto1_entry_price_long
            total_money += ratio
            crypto1_entry_price_long = 0
            crypto1_long_timer = -1
        if crypto1_entry_price_long > 0:
            prev_total_money = total_money
            total_money, crypto1_entry_price_long = sell_if_price_changed(
                total_money, crypto1_entry_price_long, crypto1_price, crypto1_95th
            )
            if total_money != prev_total_money:
                daily_long_returns += total_money - prev_total_money
                crypto1_long_timer = -1
            else:
                crypto1_long_timer += 1

        # Short management ETH against BTC
        total_money, crypto2_entry_price_short, took_trade = short_if_lag_and_fell(
            total_money, avg_metric, threshold, crypto1_fell, crypto2_flat, crypto2_entry_price_short, crypto2_price
        )
        if took_trade:
            daily_short_trades += 1
            daily_short_returns += 1
            crypto2_short_timer = 0

        if crypto2_short_timer >= minute or (i == total - 1 and crypto2_entry_price_short > 0):
            ratio = crypto2_price / crypto2_entry_price_short
            total_money -= ratio
            crypto2_entry_price_short = 0
            crypto2_short_timer = -1
        if crypto2_entry_price_short > 0:
            prev_total_money = total_money
            total_money, crypto2_entry_price_short = buy_back_if_price_changed(
                total_money, crypto2_entry_price_short, crypto2_price, crypto2_95th
            )
            if total_money != prev_total_money:
                daily_short_returns += total_money - prev_total_money
                crypto2_short_timer = -1
            else:
                crypto2_short_timer += 1

        # Short management BTC against ETH
        total_money, crypto1_entry_price_short, took_trade = short_if_lag_and_fell(
            total_money, -avg_metric, threshold, crypto2_fell, crypto1_flat, crypto1_entry_price_short, crypto1_price
        )
        if took_trade:
            daily_short_trades += 1
            daily_short_returns += 1
            crypto1_short_timer = 0

        if crypto1_short_timer >= minute or (i == total - 1 and crypto1_entry_price_short > 0):
            ratio = crypto1_price / crypto1_entry_price_short
            total_money -= ratio
            crypto1_entry_price_short = 0
            crypto1_short_timer = -1
        if crypto1_entry_price_short > 0:
            prev_total_money = total_money
            total_money, crypto1_entry_price_short = buy_back_if_price_changed(
                total_money, crypto1_entry_price_short, crypto1_price, crypto1_95th
            )
            if total_money != prev_total_money:
                daily_short_returns += total_money - prev_total_money
                crypto1_short_timer = -1
            else:
                crypto1_short_timer += 1

    daily_data = {
        'Daily Returns': total_money,
        'Long Trades': daily_long_trades,
        'Short Trades': daily_short_trades,
    }


    return daily_data


def process_strategy_yearly(crypto1, crypto2, metrics_path, strategy_function, input_path, output_path):
    """
    Processes a full year of data for a given crypto pair and computes the results of the trading strategy.

    Parameters:
    ----------
    crypto1 : str
        Symbol of the first cryptocurrency (e.g., 'BTC').
    crypto2 : str
        Symbol of the second cryptocurrency (e.g., 'ETH').
    metrics_path : str
        Path to the folder containing the lag metrics files for the crypto pair.
    strategy_function : function
        The strategy function to apply (e.g., basic_strategy_new).
    input_path : str
        Path to the folder containing the price files for the cryptocurrencies.
    output_path : str
        Path to save the yearly results.

    Returns:
    -------
    pd.DataFrame
        A DataFrame containing the strategy's results for the year.
    """
    # Paths to input directories for the cryptos
    crypto1_path = os.path.join(input_path, f"{crypto1}USDT")
    crypto2_path = os.path.join(input_path, f"{crypto2}USDT")
    metric_path = os.path.join(metrics_path, f"{crypto1}-{crypto2}")

    # Define output file path
    output_file = os.path.join(output_path, f"{crypto1}-{crypto2}-yearly-results.parquet")

    # Skip processing if the output file already exists
    if os.path.exists(output_file):
        print(f"Skipping {crypto1}-{crypto2}, yearly results already exist at {output_file}.")
        return pd.read_parquet(output_file)
    
    # Generate 15 random dates from 2024
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    random_dates = random.sample(
        [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)], 
        15
    )

    print(f"Selected random dates: {random_dates}")

    yearly_results = []

    for date in random_dates:
        # Construct filenames based on the selected date
        crypto1_file = os.path.join(crypto1_path, f"{crypto1}USDT-1s-{date}.parquet")
        crypto2_file = os.path.join(crypto2_path, f"{crypto2}USDT-1s-{date}.parquet")
        metrics_file = os.path.join(metric_path, f"{crypto1}-{crypto2}-metrics-{date}.parquet")

        # Check if all files exist
        if not (os.path.exists(crypto1_file) and os.path.exists(crypto2_file) and os.path.exists(metrics_file)):
            print(f"Skipping {date}: Missing one or more files.")
            continue

        print(f"Processing: {date}")

        try:
            daily_result = strategy_function(crypto1_file, crypto2_file, metrics_file)

            # Ensure daily_result is a DataFrame
            if isinstance(daily_result, dict):
                daily_result = pd.DataFrame([daily_result])  # Convert dict to a single-row DataFrame
            elif not isinstance(daily_result, pd.DataFrame):
                raise TypeError("Strategy function must return a DataFrame or a dict.")

            # Add the date to the DataFrame
            daily_result['Date'] = date
            yearly_results.append(daily_result)

        except Exception as e:
            print(f"Error processing files for date {date}: {e}")
            continue

    # Convert the results to a DataFrame
    if not yearly_results:
        print("No results generated, check your strategy function or input files.")
        return None

    yearly_results_df = pd.concat(yearly_results, ignore_index=True)

    # Save the results to the output path
    os.makedirs(output_path, exist_ok=True)
    yearly_results_df.to_parquet(output_file, index=False)

    print(f"Results for random days saved to: {output_file}")

    return yearly_results_df



def process_all_crypto_pairs(crypto_pairs, metrics_path, strategy_function, input_path, output_path, max_workers=4):
    """
    Processes a list of crypto pairs in parallel for the yearly strategy.

    Parameters:
    ----------
    crypto_pairs : list of tuple
        List of crypto pairs as tuples, e.g., [('BTC', 'ETH'), ('BTC', 'XRP')].
    metrics_path : str
        Path to the folder containing the lag metrics files.
    strategy_function : function
        The strategy function to apply (e.g., basic_strategy_new).
    input_path : str
        Path to the folder containing the price files for the cryptocurrencies.
    output_path : str
        Path to save the yearly results.
    max_workers : int, optional
        Maximum number of parallel workers. Default is 4.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks to the executor
        futures = {
            executor.submit(
                process_strategy_yearly, crypto1, crypto2, metrics_path, strategy_function, input_path, output_path
            ): (crypto1, crypto2)
            for crypto1, crypto2 in crypto_pairs
        }
        
        # Collect and print results as they complete
        for future in as_completed(futures):
            crypto1, crypto2 = futures[future]
            try:
                result = future.result()  # Get the result of the function
                print(f"Completed processing for {crypto1}-{crypto2}.")
            except Exception as e:
                print(f"Error processing {crypto1}-{crypto2}: {e}")