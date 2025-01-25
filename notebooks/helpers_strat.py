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

def get_percentiles(crypto1_returns, crypto2_returns):
    per_5_crypto1 = np.percentile(crypto1_returns, 5)
    per_95_crypto1 = np.percentile(crypto1_returns, 95)
    per_25_crypto1 = np.percentile(crypto1_returns, 25)
    per_75_crypto1 = np.percentile(crypto1_returns, 75)

    per_5_crypto2 = np.percentile(crypto2_returns, 5)
    per_95_crypto2 = np.percentile(crypto2_returns, 95)
    per_25_crypto2 = np.percentile(crypto2_returns, 25)
    per_75_crypto2 = np.percentile(crypto2_returns, 75)

    return per_5_crypto1, per_95_crypto1, per_25_crypto1, per_75_crypto1, per_5_crypto2, per_95_crypto2, per_25_crypto2, per_75_crypto2
