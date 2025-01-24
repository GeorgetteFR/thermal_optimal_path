import polars as pl
import matplotlib.pyplot as plt
import polars as pl

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

    data = pl.scan_parquet(file_path)
    data = data.rename({str(i): name for i, name in enumerate(new_columns)})

    data = data.with_columns([
        (pl.col("Open time")).cast(pl.Datetime('ms')),  
        (pl.col("Close time")).cast(pl.Datetime('ms'))  
    ])

    return data

def standardize(column):
    return (column - column.mean()) / column.std()

def plot_lead_lag(df_avg):
    avg_values = df_avg["avg"].to_numpy()

    plt.figure(figsize=(10, 6))
    plt.plot(avg_values[::2])  # Plot every second point
    plt.title("Average Path Between Open Prices and Volume")
    plt.xlabel("Index")
    plt.ylabel("Average Path Value")
    plt.show()