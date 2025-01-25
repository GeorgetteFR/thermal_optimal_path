import numpy as np

def sell_if_price_changed(total_money, entry_price, current_price, variation):
    ratio = current_price / entry_price
    if abs(ratio - 1) >= variation:
        total_money += ratio
        entry_price = 0 
    return total_money, entry_price


def basic_strategy(btc_prices, btc_returns, eth_prices, eth_returns, df_avg):
    threshold = 10 * 40/60
    per_5_btc = np.percentile(btc_returns, 5)
    per_95_btc = np.percentile(btc_returns, 95)
    per_25_btc = np.percentile(btc_returns, 25)
    per_75_btc = np.percentile(btc_returns, 75)

    per_5_eth = np.percentile(eth_returns, 5)
    per_95_eth = np.percentile(eth_returns, 95)
    per_25_eth = np.percentile(eth_returns, 25)
    per_75_eth = np.percentile(eth_returns, 75)

    total_money = 0

    eth_price_entry_long = 0
    btc_price_entry_long = 0
    eth_price_entry_short = 0
    btc_price_entry_short = 0
    for i in range(len(btc_prices)):
        eth_price = eth_prices.iloc[i]
        btc_price = btc_prices.iloc[i]

        eth_flat = eth_returns.iloc[i] < per_75_eth and eth_returns.iloc[i] > per_25_eth
        btc_flat = btc_returns.iloc[i] < per_75_btc and btc_returns.iloc[i] > per_25_btc

        eth_grew = eth_returns.iloc[i] > per_95_eth
        btc_grew = btc_returns.iloc[i] > per_95_btc

        eth_fell = eth_returns.iloc[i] < per_5_eth
        btc_fell = btc_returns.iloc[i] < per_5_btc

        # Long strategy
        if eth_price_entry_long > 0:
            total_money, eth_price_entry_long = sell_if_price_changed(total_money, eth_price_entry_long, eth_price, per_95_eth)

        if eth_price_entry_long == 0 and df_avg[i] > threshold and btc_grew and eth_flat:
            eth_price_entry_long = eth_price
            total_money -= 1 
            # print(f"Long ETH, price: {eth_price}, total money: {total_money}, i: {i}")

        if btc_price_entry_long > 0:
            ratio = btc_price / btc_price_entry_long
            if abs(ratio - 1) >= per_95_btc:
                total_money += ratio
                # print(f"Sold BTC at price: {btc_price}, total money: {total_money}, i: {i}")
                btc_price_entry_long = 0 

        if btc_price_entry_long == 0 and df_avg[i] < -threshold and eth_grew and btc_flat:
            btc_price_entry_long = btc_price
            total_money -= 1  
            # print(f"Long BTC, price: {btc_price}, total money: {total_money}, i: {i}")

        # Short strategy
        if eth_price_entry_short > 0:
            ratio = eth_price / eth_price_entry_short
            if abs(ratio - 1) >= per_95_eth:
                total_money -= ratio
                # print(f"Bought back ETH at price: {eth_price}, total money: {total_money}, i: {i}")
                eth_price_entry_short = 0  

        if eth_price_entry_short == 0 and df_avg[i] > threshold and btc_fell and eth_flat:
            eth_price_entry_short = eth_price
            total_money += 1  
            # print(f"Short ETH, price: {eth_price}, total money: {total_money}, i: {i}")

        if btc_price_entry_short > 0:
            ratio = btc_price / btc_price_entry_short
            if abs(ratio - 1) >= per_95_btc:
                total_money -= ratio
                # print(f"Bought back BTC at price: {btc_price}, total money: {total_money}, i: {i}")
                btc_price_entry_short = 0 

        if btc_price_entry_short == 0 and df_avg[i] < -threshold and eth_fell and btc_flat:
            btc_price_entry_short = btc_price
            total_money += 1  
            # print(f"Short BTC, price: {btc_price}, total money: {total_money}, i: {i}")

    print(f"Total money after strategy: Base amount + {total_money}")