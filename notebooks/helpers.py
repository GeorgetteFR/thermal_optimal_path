import numpy as np
import pandas as pd  
import matplotlib.pyplot as plt

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


def rolling_standardize(column, window=60):
    """
    Perform a rolling standardization on the given column using Pandas.
    
    Parameters:
        column (array-like): The input column to standardize (1D array or list).
        window (int): The rolling window size in seconds.
        
    Returns:
        pd.Series: A Pandas Series of rolling standardized values.
    """
    column = pd.Series(column)
    rolling_mean = column.rolling(window=window, min_periods=window).mean()
    rolling_std = column.rolling(window=window, min_periods=window).std()
    return (column - rolling_mean) / rolling_std


def plot_lead_lag(df_avg):
    plt.figure(figsize=(10, 6))
    plt.plot(df_avg) 
    plt.title("Average Path Between Open Prices and Volume")
    plt.xlabel("Index")
    plt.ylabel("Average Path Value")
    plt.show()