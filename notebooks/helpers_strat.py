import numpy as np

def sell_if_price_changed(total_money, entry_price, current_price, variation):
    ratio = current_price / entry_price
    if abs(ratio - 1) >= variation:
        total_money += ratio
        entry_price = 0 
        print(f"Sold at {current_price}")
    return total_money, entry_price

def buy_back_if_price_changed(total_money, entry_price, current_price, variation):
    ratio = current_price / entry_price
    if abs(ratio - 1) >= variation:
        total_money -= ratio
        entry_price = 0 
        print(f"Bought back at {current_price}")
    return total_money, entry_price

def long_if_lag_and_grew(total_money, lag, threshold, cr_1_grew, cr_2_flat, entry_price, current_price):
    took_trade = False
    if entry_price == 0 and lag > threshold and cr_1_grew and cr_2_flat:
        entry_price = current_price
        total_money -= 1 
        took_trade = True
        print(f"Long at {current_price}")

    return total_money, entry_price, took_trade

def short_if_lag_and_fell(total_money, lag, threshold, cr_1_fell, cr_2_flat, entry_price, current_price):
    took_trade = False
    if entry_price == 0 and lag > threshold and cr_1_fell and cr_2_flat:
        entry_price = current_price
        total_money += 1 
        took_trade = True
        print(f"Short at {current_price}")

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

