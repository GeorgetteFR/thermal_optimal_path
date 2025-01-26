import numpy as np
import pandas as pd  
import matplotlib.pyplot as plt
from numba import njit
import numpy as np

def sell_if_price_changed(total_money, entry_price, current_price, variation):
    ratio = current_price / entry_price
    if abs(ratio - 1) >= variation:
        total_money += ratio
        entry_price = 0 
    return total_money, entry_price

def buy_back_if_price_changed(total_money, entry_price, current_price, variation):
    ratio = current_price / entry_price
    if abs(ratio - 1) >= variation:
        total_money -= ratio
        entry_price = 0 
    return total_money, entry_price

def long_if_lag_and_grew(total_money, lag, threshold, cr_1_grew, cr_2_flat, entry_price, current_price):
    took_trade = False
    if entry_price == 0 and lag > threshold and cr_1_grew and cr_2_flat:
        entry_price = current_price
        total_money -= 1 
        took_trade = True

    return total_money, entry_price, took_trade

def short_if_lag_and_fell(total_money, lag, threshold, cr_1_fell, cr_2_flat, entry_price, current_price):
    took_trade = False
    if entry_price == 0 and lag > threshold and cr_1_fell and cr_2_flat:
        entry_price = current_price
        total_money += 1 
        took_trade = True


    return total_money, entry_price, took_trade

def get_booleans(crypto2_returns, crypto1_returns, per_5_crypto2, per_95_crypto2, per_25_crypto2, per_75_crypto2, per_5_crypto1, per_95_crypto1, per_25_crypto1, per_75_crypto1):
        crypto2_flat = crypto2_returns < per_75_crypto2 and crypto2_returns > per_25_crypto2
        crypto1_flat = crypto1_returns < per_75_crypto1 and crypto1_returns > per_25_crypto1

        crypto2_grew = crypto2_returns > per_95_crypto2
        crypto1_grew = crypto1_returns > per_95_crypto1

        crypto2_fell = crypto2_returns < per_5_crypto2
        crypto1_fell = crypto1_returns < per_5_crypto1

        return crypto2_flat, crypto1_flat, crypto2_grew, crypto1_grew, crypto2_fell, crypto1_fell

def get_percentiles(crypto1_returns, crypto2_returns, window):
    percentiles = [0.05, 0.25, 0.75, 0.95]  
    crypto1_rolling = rolling_percentiles_sorted(crypto1_returns, window, percentiles)
    crypto2_rolling = rolling_percentiles_sorted(crypto2_returns, window, percentiles)


    per_5_crypto1 = crypto1_rolling[0.05]
    per_25_crypto1 = crypto1_rolling[0.25]
    per_75_crypto1 = crypto1_rolling[0.75]
    per_95_crypto1 = crypto1_rolling[0.95]

    per_5_crypto2 = crypto2_rolling[0.05]
    per_25_crypto2 = crypto2_rolling[0.25]
    per_75_crypto2 = crypto2_rolling[0.75]
    per_95_crypto2 = crypto2_rolling[0.95]

    return per_5_crypto1, per_95_crypto1, per_25_crypto1, per_75_crypto1, per_5_crypto2, per_95_crypto2, per_25_crypto2, per_75_crypto2


from numba import njit
import numpy as np

@njit
def rolling_percentiles_sorted(column, window, percentiles):
    """
    Compute rolling percentiles using a dynamically sorted window.
    
    Parameters:
        column (1D array): Input data.
        window (int): Rolling window size.
        percentiles (list of floats): List of percentiles (e.g., [0.05, 0.25, 0.75, 0.95]).
    
    Returns:
        dict of 1D arrays: Percentile results for each given percentile.
    """
    n = len(column)
    sorted_window = np.empty(window)  # Array for the rolling window
    result = {p: np.full(n, np.nan) for p in percentiles}  # Initialize result with NaNs

    # Initialize the first window
    for i in range(window):
        sorted_window[i] = column[i]
    sorted_window.sort()  # Sort the initial window

    # Compute rolling percentiles
    for i in range(window - 1, n):
        # Compute percentiles for the current sorted window
        for p in percentiles:
            idx = int(p * window) if int(p * window) < window else window - 1
            result[p][i] = sorted_window[idx]

        # Update the sorted window
        if i + 1 < n:
            # Remove the oldest value
            old_value = column[i - window + 1]
            new_value = column[i + 1]
            sorted_window = remove_and_insert(sorted_window, old_value, new_value)

    return result


@njit
def remove_and_insert(sorted_window, old_value, new_value):
    """
    Remove the oldest value and insert the new value in the correct position to maintain a sorted window.
    """
    window = len(sorted_window)
    temp_window = np.empty(window - 1)  # Temporary array for shifting
    # Find and remove the old value
    index = -1
    for j in range(window):
        if sorted_window[j] == old_value:
            index = j
            break
    if index == -1:
        raise ValueError("Old value not found in the sorted window.")
    temp_window[:index] = sorted_window[:index]
    temp_window[index:] = sorted_window[index + 1:]

    # Insert the new value
    for j in range(window - 1):
        if temp_window[j] > new_value:
            sorted_window[:j] = temp_window[:j]
            sorted_window[j + 1:] = temp_window[j:]
            sorted_window[j] = new_value
            return sorted_window

    # If new_value is greater than all elements, insert at the end
    sorted_window[:-1] = temp_window
    sorted_window[-1] = new_value
    return sorted_window



def load_parquet(file_path):
    new_columns = [
        "Open time", 
        "Open", 
        "High", 
        "Low", 
        "Close", 
        "Volume", 
        "Close time", 
        "Quote asset volume", 
        "Number of trades", 
        "Taker buy base asset volume", 
        "Taker buy quote asset volume", 
        "Ignore"
    ]

    data = pd.read_parquet(file_path)

    data.columns = new_columns[:len(data.columns)]

    data["Open time"] = pd.to_datetime(data["Open time"], unit='ms')
    data["Close time"] = pd.to_datetime(data["Close time"], unit='ms')

    data = data[["Open time", "Open",]]

    return data

def standardize(column):
    column = np.array(column)
    return (column - np.mean(column)) / np.std(column)

@njit
def rolling_std(values, window=5):
    """
    Compute a rolling standard deviation for a 1D array.
    """
    result = np.empty(len(values))
    rolling_sum = 0.0
    rolling_sum_sq = 0.0

    for i in range(len(values)):
        if i < window:
            rolling_sum += values[i]
            rolling_sum_sq += values[i] ** 2
            result[i] = np.nan  # Not enough data points
        else:
            rolling_sum += values[i] - values[i - window]
            rolling_sum_sq += values[i] ** 2 - values[i - window] ** 2
            mean = rolling_sum / window
            std = np.sqrt((rolling_sum_sq / window) - mean ** 2)
            result[i] = std

    return result

@njit
def rolling_standardize_numba(column, window=3600):
    """
    Perform a rolling standardization on the given column using Numba.
    """
    result = np.empty(len(column))
    rolling_sum = 0.0
    rolling_sum_sq = 0.0

    for i in range(len(column)):
        if i < window:
            # Accumulate rolling sums for the initial window
            rolling_sum += column[i]
            rolling_sum_sq += column[i] ** 2
            result[i] = np.nan  # Not enough data points
        else:
            # Update rolling sums
            rolling_sum += column[i] - column[i - window]
            rolling_sum_sq += column[i] ** 2 - column[i - window] ** 2
            rolling_mean = rolling_sum / window
            rolling_std = np.sqrt((rolling_sum_sq / window) - rolling_mean ** 2)
            
            # Handle division by zero
            if rolling_std == 0:
                result[i] = 0
            else:
                result[i] = (column[i] - rolling_mean) / rolling_std

    return result


def rolling_standardize(column, window=40):
    """
    Perform a rolling standardization on the given column using Pandas.
    
    Parameters:
        column (array-like): The input column to standardize (1D array or list).
        window (int): The rolling window size in seconds.
        
    Returns:
        np.ndarray: A NumPy array of rolling standardized values.
    """
    column = pd.Series(column)  # Convert to a Pandas Series
    rolling_mean = column.rolling(window=window, min_periods=window).mean()
    rolling_std = column.rolling(window=window, min_periods=window).std()
    result = (column - rolling_mean) / rolling_std
    return result.to_numpy()  # Convert the result back to a NumPy array

@njit
def weighted_average(values, weights):
    return np.dot(values, weights)


def plot_lead_lag(df_avg):
    plt.figure(figsize=(10, 6))
    plt.plot(df_avg) 
    plt.title("Average Path Between Open Prices and Volume")
    plt.xlabel("Index")
    plt.ylabel("Average Path Value")
    plt.show()