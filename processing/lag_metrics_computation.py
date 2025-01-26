from thermal_optimal_path.lattice import *
from thermal_optimal_path.statistics import *
from notebooks.helpers import *
import sys
import os
import pandas as pd
import numpy as np
from joblib import Parallel, delayed


def process_day(btc_prices, eth_prices, temperature, window=40, rolling_window=3600):
    """
    Processes a single day of cryptocurrency price data to compute lag metrics.

    This function standardizes the input price data using a rolling window, precomputes lattices 
    for partition function and average path calculations, and iteratively computes weighted averages 
    and rolling volatilities for a specified window size. The computations leverage thermal optimal 
    path methods to analyze the relationship between two cryptocurrencies' price movements.

    Parameters:
    ----------
    btc_prices: pd.DataFrame
        DataFrame containing the price data for the first cryptocurrency (e.g., BTC), with an 'Open' column.
    eth_prices: pd.DataFrame
        DataFrame containing the price data for the second cryptocurrency (e.g., ETH), with an 'Open' column.
    temperature: float
        The temperature parameter for the partition function calculation.
    window: int, optional
        The size of the window for computing lag metrics (default=40).
    rolling_window: int, optional
        The size of the rolling window used for data standardization (default=3600 seconds).

    Returns:
    -------
    tuple of np.ndarray
        A tuple containing two arrays: weighted averages and rolling volatilities for the day's data.
    """
    
    # Apply rolling standardization
    btc_data = rolling_standardize_numba(btc_prices['Open'].to_numpy(), rolling_window)
    eth_data = rolling_standardize_numba(eth_prices['Open'].to_numpy(), rolling_window)

    # Precompute the lattices
    lattice_points_partition = precompute_lattice(window)
    lattice_points_avg_path = precompute_lattice(window, exclude_boundary=False)

    total = len(btc_prices)
    weights = np.exp(np.linspace(0, 2, window))
    weights /= weights.sum()

    # Initialize arrays for metrics with padding at the beginning
    weighted_averages = [np.nan] * window
    rolling_volatilities = [np.nan] * window

    # Compute metrics for the rest of the data
    for i in prange(total - window):
        btc = btc_data[i:i + window]
        eth = eth_data[i:i + window]

        # Use the precomputed lattice for partition function
        g = partition_function_precomp(btc, eth, temperature, error_func=error, lattice=lattice_points_partition)
        
        # Use the precomputed lattice for average path
        avg_path = average_path_with_precomputed_lattice(g, lattice=lattice_points_avg_path)[::2]
        # Compute metrics
        weighted_avg = weighted_average(avg_path, weights)
        vol = rolling_std(avg_path, window=5)

        weighted_averages.append(weighted_avg)
        rolling_volatilities.append(vol[-1])  # Last value of the rolling std

    # Convert lists to NumPy arrays
    return np.array(weighted_averages), np.array(rolling_volatilities)

def process_year(crypto1, crypto2, input_path, output_path, temperature, window=40, rolling_window=3600):
    """
    Processes a full year of data for a given crypto pair and stores daily lag metrics.

    Parameters:
    ----------
    crypto1: str
        Name of the first cryptocurrency (e.g., 'BTC').
    crypto2: str
        Name of the second cryptocurrency (e.g., 'ETH').
    input_path: str
        Path to the folder containing the price files for both cryptos.
    output_path: str
        Path to the folder where the lag metric files should be stored.
    temperature: float
        Temperature parameter for the partition function.
    window: int, optional
        Window size for metrics computation (default=40).
    rolling_window: int, optional
        Rolling window size for standardization (default=3600 seconds).

    Returns:
    -------
    None
    """
    # Define input directories for both cryptos
    crypto1_path = os.path.join(input_path, f"{crypto1}USDT")
    crypto2_path = os.path.join(input_path, f"{crypto2}USDT")
    
    # Ensure output directory exists
    output_dir = os.path.join(output_path, f"{crypto1}-{crypto2}")
    os.makedirs(output_dir, exist_ok=True)

    # Get sorted list of files for each crypto
    crypto1_files = sorted([os.path.join(crypto1_path, f) for f in os.listdir(crypto1_path) if f.endswith('.parquet')])
    crypto2_files = sorted([os.path.join(crypto2_path, f) for f in os.listdir(crypto2_path) if f.endswith('.parquet')])

    # Ensure the number of files match
    assert len(crypto1_files) == len(crypto2_files), "Mismatch in the number of files for the two cryptos."

    # Process each day's data
    for crypto1_file, crypto2_file in zip(crypto1_files, crypto2_files):
        # Extract the date from the input file name (e.g., 'ETHUSDT-1s-2024-01-01.parquet')
        date = "-".join(os.path.basename(crypto1_file).split('-')[2:5]).replace('.parquet', '')
        output_file = os.path.join(output_dir, f"{crypto1}-{crypto2}-metrics-{date}.parquet")
        
        # Skip calculation if the output file already exists
        if os.path.exists(output_file):
            print(f"Skipping {date} as {output_file} already exists.")
            continue

        print(f"Processing: {crypto1_file} and {crypto2_file}")
        
        # Load daily data
        btc_prices = pd.read_parquet(crypto1_file)
        eth_prices = pd.read_parquet(crypto2_file)

        # Check if the two files have the same size
        if len(btc_prices) != len(eth_prices):
            print(
                f"Skipping day due to size mismatch: "
                f"{crypto1_file} has {len(btc_prices)} rows, "
                f"{crypto2_file} has {len(eth_prices)} rows."
            )
            continue
        
        # Process the day's metrics
        weighted_avg, rolling_std = process_day(
            btc_prices, eth_prices, temperature, window=window, rolling_window=rolling_window
        )

        # Assert that the output metrics match the size of the input prices
        assert len(weighted_avg) == len(btc_prices), (
            f"Size mismatch: weighted_avg length ({len(weighted_avg)}) "
            f"does not match btc_prices length ({len(btc_prices)})."
        )
        assert len(rolling_std) == len(btc_prices), (
            f"Size mismatch: rolling_std length ({len(rolling_std)}) "
            f"does not match btc_prices length ({len(btc_prices)})."
        )

        # Prepare the output DataFrame
        result_df = pd.DataFrame({
            'Open time': btc_prices['Open time'],
            'avg': weighted_avg,
            'std': rolling_std,
        })

        # Write to Parquet file
        result_df.to_parquet(output_file, index=False)
        print(f"Saved: {output_file}")

    print("Year processing complete!")


def process_year_wrapper(crypto1, crypto2, input_path, output_path, temperature, window, rolling_window):
    """
    A wrapper function for process_year to pass multiple arguments.
    """
    process_year(crypto1, crypto2, input_path, output_path, temperature, window, rolling_window)

def process_multiple_years_joblib(crypto_pairs, input_path, output_path, temperature, window=40, rolling_window=5):
    """
    Processes multiple crypto pairs in parallel using joblib for Jupyter compatibility.

    Parameters:
    ----------
    crypto_pairs: list of tuples
        List of tuples, where each tuple contains two crypto names (e.g., [("BTC", "ETH"), ("BTC", "LTC")]).
    input_path: str
        Path to the folder containing the price files for all cryptos.
    output_path: str
        Path to the folder where the lag metric files should be stored.
    temperature: float
        Temperature parameter for the partition function.
    window: int, optional
        Window size for the rolling metrics (default=40).
    rolling_window: int, optional
        Window size for rolling standard deviation (default=5).

    Returns:
    -------
    None
    """
    # Use all available cores by default
    num_cores = os.cpu_count() - 2
    print(f"Running in parallel with {num_cores} workers...")

    # Use joblib's Parallel to run process_year for each pair
    Parallel(n_jobs=num_cores)(
        delayed(process_year_wrapper)(
            crypto1, crypto2, input_path, output_path, temperature, window, rolling_window
        )
        for crypto1, crypto2 in crypto_pairs
    )

    print("All crypto pairs processed successfully!")
