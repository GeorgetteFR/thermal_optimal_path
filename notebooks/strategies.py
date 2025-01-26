import numpy as np
import pandas as pd
from helpers_strat import *

def basic_strategy(crypto1_prices, crypto1_returns, crypto2_prices, crypto2_returns, percentiles, df_avg, minute=60):
    total_money = 0
    threshold = 6.67

    crypto1_entry_price_long, crypto2_entry_price_long, crypto1_entry_price_short, crypto2_entry_price_short = 0, 0, 0, 0
    crypto1_long_timer, crypto2_long_timer, crypto1_short_timer, crypto2_short_timer = -1, -1, -1, -1

    daily_long_trades = 0
    daily_short_trades = 0
    daily_long_returns = 0
    daily_short_returns = 0

    (per_5_crypto1_df, per_95_crypto1_df, per_25_crypto1_df, per_75_crypto1_df,
    per_5_crypto2_df, per_95_crypto2_df, per_25_crypto2_df, per_75_crypto2_df) = percentiles

    for i in range(len(crypto1_prices)):
        per_5_crypto1 = per_5_crypto1_df[i]
        per_95_crypto1 = per_95_crypto1_df[i]
        per_25_crypto1 = per_25_crypto1_df[i]
        per_75_crypto1 = per_75_crypto1_df[i]

        per_5_crypto2 = per_5_crypto2_df[i]
        per_95_crypto2 = per_95_crypto2_df[i]
        per_25_crypto2 = per_25_crypto2_df[i]
        per_75_crypto2 = per_75_crypto2_df[i]

        crypto2_price = crypto2_prices.iloc[i]
        crypto1_price = crypto1_prices.iloc[i]

        crypto2_flat, crypto1_flat, crypto2_grew, crypto1_grew, crypto2_fell, crypto1_fell = get_booleans(crypto2_returns.iloc[i], crypto1_returns.iloc[i], per_5_crypto2, per_95_crypto2, per_25_crypto2, per_75_crypto2, per_5_crypto1, per_95_crypto1, per_25_crypto1, per_75_crypto1)
        

        # Long management ETH against BTC
        total_money, crypto2_entry_price_long, took_trade = long_if_lag_and_grew(total_money, df_avg.iloc[i], threshold, crypto1_grew, crypto2_flat, crypto2_entry_price_long, crypto2_price)
        if took_trade:
            daily_long_trades += 1
            daily_long_returns -= 1
            crypto2_long_timer = 0


        if crypto2_long_timer >= minute:
            ratio = crypto2_price / crypto2_entry_price_long
            total_money += ratio
            crypto2_entry_price_long = 0 
            crypto2_long_timer = -1
            print("BLABLABA")
        if crypto2_entry_price_long > 0:
            prev_total_money = total_money
            total_money, crypto2_entry_price_long = sell_if_price_changed(total_money, crypto2_entry_price_long, crypto2_price, per_95_crypto2)
            if total_money != prev_total_money:
                daily_long_returns += total_money - prev_total_money
                crypto2_long_timer = -1
            else:
                crypto2_long_timer += 1

        #Long management BTC against ETH
        total_money, crypto1_entry_price_long, took_trade = long_if_lag_and_grew(total_money, -df_avg.iloc[i], threshold, crypto2_grew, crypto1_flat, crypto1_entry_price_long, crypto1_price)
        if took_trade:
            daily_long_trades += 1
            daily_long_returns -= 1
            crypto1_long_timer = 0

        if crypto1_long_timer >= minute:
            ratio = crypto1_price / crypto1_entry_price_long
            total_money += ratio
            crypto1_entry_price_long = 0 
            crypto1_long_timer = -1
            print("BLABLABA")
        if crypto1_entry_price_long > 0:
            prev_total_money = total_money
            total_money, crypto1_entry_price_long = sell_if_price_changed(total_money, crypto1_entry_price_long, crypto1_price, per_95_crypto1)
            if total_money != prev_total_money:
                daily_long_returns += total_money - prev_total_money
                crypto1_long_timer = -1
            else:
                crypto1_long_timer += 1
        
        # Short management ETH against BTC
        total_money, crypto2_entry_price_short, took_trade = short_if_lag_and_fell(total_money, df_avg.iloc[i], threshold, crypto1_fell, crypto2_flat, crypto2_entry_price_short, crypto2_price)
        if took_trade:
            daily_short_trades += 1
            daily_short_returns += 1
            crypto2_short_timer = 0
            print(f"i: {i}, crypto2_entry_price_short: {crypto2_entry_price_short}, crypto2_price: {crypto2_price}")

        if crypto2_short_timer >= minute:
            ratio = crypto2_price / crypto2_entry_price_short
            total_money -= ratio
            crypto2_entry_price_short = 0 
            crypto2_short_timer = -1
            print("BLABLABA")
        if crypto2_entry_price_short > 0:
            prev_total_money = total_money
            total_money, crypto2_entry_price_short = buy_back_if_price_changed(total_money, crypto2_entry_price_short, crypto2_price, per_95_crypto2)
            if total_money != prev_total_money:
                daily_short_returns += total_money - prev_total_money
                crypto2_short_timer = -1
            else:
                crypto2_short_timer += 1

        # Short management BTC against ETH
        total_money, crypto1_entry_price_short, took_trade = short_if_lag_and_fell(total_money, -df_avg.iloc[i], threshold, crypto2_fell, crypto1_flat, crypto1_entry_price_short, crypto1_price)
        if took_trade:
            daily_short_trades += 1
            daily_short_returns += 1
            crypto1_short_timer = 0

        if crypto1_short_timer >= minute:
            ratio = crypto1_price / crypto1_entry_price_short
            total_money -= ratio
            crypto1_entry_price_short = 0 
            crypto1_short_timer = -1
            print("BLABLABA")
        if crypto1_entry_price_short > 0:
            prev_total_money = total_money
            total_money, crypto1_entry_price_short = buy_back_if_price_changed(total_money, crypto1_entry_price_short, crypto1_price, per_95_crypto1)
            if total_money != prev_total_money:
                daily_short_returns += total_money - prev_total_money
                crypto1_short_timer = -1
            else:
                crypto1_short_timer += 1

    daily_data = {
        'Daily Returns': total_money,
        'Long Trades': daily_long_trades,
        'Short Trades': daily_short_trades,
        'Long Returns': daily_long_returns,
        'Short Returns': daily_short_returns
    }
    df = pd.DataFrame([daily_data])
    df.to_csv('daily_returns.csv', mode='a', header=False, index=False)
    
    print(f"Total money after strategy: Base amount + {total_money}, Long trades: {daily_long_trades}, Short trades: {daily_short_trades}, Long returns: {daily_long_returns}, Short returns: {daily_short_returns}")









def basic_strategy_old(crypto1_prices, crypto1_returns, crypto2_prices, crypto2_returns, df_avg, minute=60):
    total_money = 0
    threshold = 6.67

    crypto1_entry_price_long, crypto2_entry_price_long, crypto1_entry_price_short, crypto2_entry_price_short = 0, 0, 0, 0
    crypto1_long_timer, crypto2_long_timer, crypto1_short_timer, crypto2_short_timer = -1, -1, -1, -1

    daily_long_trades = 0
    daily_short_trades = 0
    daily_long_returns = 0
    daily_short_returns = 0

    # Compute rolling percentiles
    percentiles = get_percentiles(crypto1_returns.values, crypto2_returns.values, window=3600)

    # Extract each percentile DataFrame
    (per_5_crypto1_df, per_95_crypto1_df, per_25_crypto1_df, per_75_crypto1_df,
    per_5_crypto2_df, per_95_crypto2_df, per_25_crypto2_df, per_75_crypto2_df) = percentiles

    # Truncate dataframes based on the first valid (non-NaN) index
    valid_start_idx = max(
        np.argmax(~np.isnan(per_5_crypto1_df)),
        np.argmax(~np.isnan(per_95_crypto1_df)),
        np.argmax(~np.isnan(per_25_crypto1_df)),
        np.argmax(~np.isnan(per_75_crypto1_df)),
        np.argmax(~np.isnan(per_5_crypto2_df)),
        np.argmax(~np.isnan(per_95_crypto2_df)),
        np.argmax(~np.isnan(per_25_crypto2_df)),
        np.argmax(~np.isnan(per_75_crypto2_df)),
    )

    # Drop NaNs and align all data
    crypto1_prices = crypto1_prices[valid_start_idx:]
    crypto2_prices = crypto2_prices[valid_start_idx:]
    crypto1_returns = crypto1_returns[valid_start_idx:]
    crypto2_returns = crypto2_returns[valid_start_idx:]
    df_avg = df_avg[valid_start_idx:]

    # Truncate percentiles
    per_5_crypto1_df = per_5_crypto1_df[valid_start_idx:]
    per_95_crypto1_df = per_95_crypto1_df[valid_start_idx:]
    per_25_crypto1_df = per_25_crypto1_df[valid_start_idx:]
    per_75_crypto1_df = per_75_crypto1_df[valid_start_idx:]
    per_5_crypto2_df = per_5_crypto2_df[valid_start_idx:]
    per_95_crypto2_df = per_95_crypto2_df[valid_start_idx:]
    per_25_crypto2_df = per_25_crypto2_df[valid_start_idx:]
    per_75_crypto2_df = per_75_crypto2_df[valid_start_idx:]

    total=len(crypto1_prices)
    print(f"Total: {total}, valid_start_idx: {valid_start_idx}")
    for i in range(total):
        per_5_crypto1 = per_5_crypto1_df[i]
        per_95_crypto1 = per_95_crypto1_df[i]
        per_25_crypto1 = per_25_crypto1_df[i]
        per_75_crypto1 = per_75_crypto1_df[i]

        per_5_crypto2 = per_5_crypto2_df[i]
        per_95_crypto2 = per_95_crypto2_df[i]
        per_25_crypto2 = per_25_crypto2_df[i]
        per_75_crypto2 = per_75_crypto2_df[i]

        crypto2_price = crypto2_prices.iloc[i]
        crypto1_price = crypto1_prices.iloc[i]

        crypto2_flat, crypto1_flat, crypto2_grew, crypto1_grew, crypto2_fell, crypto1_fell = get_booleans(crypto2_returns.iloc[i], crypto1_returns.iloc[i], per_5_crypto2, per_95_crypto2, per_25_crypto2, per_75_crypto2, per_5_crypto1, per_95_crypto1, per_25_crypto1, per_75_crypto1)
        

        # Long management ETH against BTC
        total_money, crypto2_entry_price_long, took_trade = long_if_lag_and_grew(total_money, df_avg.iloc[i], threshold, crypto1_grew, crypto2_flat, crypto2_entry_price_long, crypto2_price)
        if took_trade:
            daily_long_trades += 1
            daily_long_returns -= 1
            crypto2_long_timer = 0


        if crypto2_long_timer >= minute or (i == total - 1 and crypto2_entry_price_long > 0):
            ratio = crypto2_price / crypto2_entry_price_long
            total_money += ratio
            crypto2_entry_price_long = 0 
            crypto2_long_timer = -1
            print("BLABLABA")
        if crypto2_entry_price_long > 0:
            prev_total_money = total_money
            total_money, crypto2_entry_price_long = sell_if_price_changed(total_money, crypto2_entry_price_long, crypto2_price, per_95_crypto2)
            if total_money != prev_total_money:
                daily_long_returns += total_money - prev_total_money
                crypto2_long_timer = -1
            else:
                crypto2_long_timer += 1

        #Long management BTC against ETH
        total_money, crypto1_entry_price_long, took_trade = long_if_lag_and_grew(total_money, -df_avg.iloc[i], threshold, crypto2_grew, crypto1_flat, crypto1_entry_price_long, crypto1_price)
        if took_trade:
            daily_long_trades += 1
            daily_long_returns -= 1
            crypto1_long_timer = 0

        if crypto1_long_timer >= minute or (i == total - 1 and crypto1_entry_price_long > 0):
            ratio = crypto1_price / crypto1_entry_price_long
            total_money += ratio
            crypto1_entry_price_long = 0 
            crypto1_long_timer = -1
            print("BLABLABA")
        if crypto1_entry_price_long > 0:
            prev_total_money = total_money
            total_money, crypto1_entry_price_long = sell_if_price_changed(total_money, crypto1_entry_price_long, crypto1_price, per_95_crypto1)
            if total_money != prev_total_money:
                daily_long_returns += total_money - prev_total_money
                crypto1_long_timer = -1
            else:
                crypto1_long_timer += 1
        
        # Short management ETH against BTC
        total_money, crypto2_entry_price_short, took_trade = short_if_lag_and_fell(total_money, df_avg.iloc[i], threshold, crypto1_fell, crypto2_flat, crypto2_entry_price_short, crypto2_price)
        if took_trade:
            daily_short_trades += 1
            daily_short_returns += 1
            crypto2_short_timer = 0
            print(f"i: {i}, crypto2_entry_price_short: {crypto2_entry_price_short}, crypto2_price: {crypto2_price}")

        if crypto2_short_timer >= minute or (i == total - 1 and crypto2_entry_price_short > 0):
            ratio = crypto2_price / crypto2_entry_price_short
            total_money -= ratio
            crypto2_entry_price_short = 0 
            crypto2_short_timer = -1
            print("BLABLABA")
        if crypto2_entry_price_short > 0:
            prev_total_money = total_money
            total_money, crypto2_entry_price_short = buy_back_if_price_changed(total_money, crypto2_entry_price_short, crypto2_price, per_95_crypto2)
            if total_money != prev_total_money:
                daily_short_returns += total_money - prev_total_money
                crypto2_short_timer = -1
            else:
                crypto2_short_timer += 1

        # Short management BTC against ETH
        total_money, crypto1_entry_price_short, took_trade = short_if_lag_and_fell(total_money, -df_avg.iloc[i], threshold, crypto2_fell, crypto1_flat, crypto1_entry_price_short, crypto1_price)
        if took_trade:
            daily_short_trades += 1
            daily_short_returns += 1
            crypto1_short_timer = 0
            print(f"i: {i}, crypto1_entry_price_short: {crypto1_entry_price_short}, crypto1_price: {crypto1_price}")

        if crypto1_short_timer >= minute or (i == total - 1 and crypto1_entry_price_short > 0):
            ratio = crypto1_price / crypto1_entry_price_short
            total_money -= ratio
            crypto1_entry_price_short = 0 
            crypto1_short_timer = -1
            print("BLABLABA")
        if crypto1_entry_price_short > 0:
            prev_total_money = total_money
            total_money, crypto1_entry_price_short = buy_back_if_price_changed(total_money, crypto1_entry_price_short, crypto1_price, per_95_crypto1)
            if total_money != prev_total_money:
                daily_short_returns += total_money - prev_total_money
                crypto1_short_timer = -1
            else:
                crypto1_short_timer += 1

    daily_data = {
        'Daily Returns': total_money,
        'Long Trades': daily_long_trades,
        'Short Trades': daily_short_trades,
        'Long Returns': daily_long_returns,
        'Short Returns': daily_short_returns
    }
    df = pd.DataFrame([daily_data])
    df.to_csv('daily_returns.csv', mode='a', header=False, index=False)
    
    print(f"Total money after strategy: Base amount + {total_money}, Long trades: {daily_long_trades}, Short trades: {daily_short_trades}, Long returns: {daily_long_returns}, Short returns: {daily_short_returns}")