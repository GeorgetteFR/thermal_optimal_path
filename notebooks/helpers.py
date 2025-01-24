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

    return data

def standardize(column):
    column = np.array(column)
    return (column - np.mean(column)) / np.std(column)

def plot_lead_lag(df_avg):
    plt.figure(figsize=(10, 6))
    plt.plot(df_avg[::2]) 
    plt.title("Average Path Between Open Prices and Volume")
    plt.xlabel("Index")
    plt.ylabel("Average Path Value")
    plt.show()