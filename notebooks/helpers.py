import numpy as np
import pandas as pd  
import matplotlib.pyplot as plt
from numba import njit


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