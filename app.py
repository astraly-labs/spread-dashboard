import streamlit as st
import requests
import time
import math
import pandas as pd

TOKENS = [
    {'symbol': 'ETH', 'address': '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7', 'decimals': 18},
    {'symbol': 'USDT', 'address': '0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8', 'decimals': 6},
    {'symbol': 'DAI', 'address': '0x05574eb6b8789a91466f902c380d978e472db68170ff82a5b650b95a58ddf4ad', 'decimals': 18},
    {'symbol': 'WBTC', 'address': '0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac', 'decimals': 8},
    {'symbol': 'STRK', 'address': '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d', 'decimals': 18},
]

USD_TOKEN = {'symbol': 'USDC', 'address': '0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8', 'decimals': 6}

def fetch_quote(sell_address, buy_address, sell_amount):
    url = "https://starknet.api.avnu.fi/swap/v2/quotes"
    params = {
        'sellTokenAddress': sell_address,
        'buyTokenAddress': buy_address,
        'sellAmount': hex(sell_amount),
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None
        quote = data[0]
        return {
            'sell_amount': int(quote['sellAmount'], 0),
            'buy_amount': int(quote['buyAmount'], 0),
            'sell_token_price_in_usd': quote['sellTokenPriceInUsd'],
            'buy_token_price_in_usd': quote['buyTokenPriceInUsd'],
            'gas_fees_in_usd': quote['gasFeesInUsd'] + quote['avnuFeesInUsd'],
        }
    except Exception:
        return None

def compute_slippage(quote, sell_dec, buy_dec):
    sell_amount = quote['sell_amount'] / 10 ** sell_dec
    buy_amount = quote['buy_amount'] / 10 ** buy_dec
    sell_usd = sell_amount * quote['sell_token_price_in_usd']
    buy_usd = buy_amount * quote['buy_token_price_in_usd']
    print("sell_usd", sell_usd)
    print("buy_usd", buy_usd)
    if buy_usd == 0:
        return float('inf')
    return 1 - (sell_usd / buy_usd)

def find_depth_amount(sell_token, buy_token, is_sell_side):
    TARGET_SLIPPAGE = 0.02
    TOLERANCE_FROM_TARGET = 0.001
    MAX_ITERATIONS = 20
    MIN_AMOUNT_USD = 10000.0
    MAX_AMOUNT_USD = 500000000.0

    small_amount = max(1, 10 ** (sell_token['decimals'] - 6))
    small_quote = fetch_quote(sell_token['address'], buy_token['address'], small_amount)
    if not small_quote:
        return None
    sell_price = small_quote['sell_token_price_in_usd']
    if sell_price <= 0:
        return None

    min_amount = math.ceil(MIN_AMOUNT_USD / sell_price * 10 ** sell_token['decimals'])
    max_amount = math.ceil(MAX_AMOUNT_USD / sell_price * 10 ** sell_token['decimals'])

    for _ in range(MAX_ITERATIONS):
        amount = (min_amount + max_amount) // 2
        if amount == 0:
            return None
        quote = fetch_quote(sell_token['address'], buy_token['address'], amount)
        print("quote", quote)
        if not quote:
            return None
        slippage = compute_slippage(quote, sell_token['decimals'], buy_token['decimals'])
        print("slippage", slippage)
        diff = abs(slippage - TARGET_SLIPPAGE)
        if diff < TOLERANCE_FROM_TARGET:
            return quote['buy_amount'] if is_sell_side else amount
        if slippage < TARGET_SLIPPAGE:
            min_amount = amount
        else:
            max_amount = amount
        if max_amount - min_amount <= 10:
            return quote['buy_amount'] if is_sell_side else amount
        time.sleep(1)
    return None

st.title("Starknet ±2% Depth Dashboard via AVNU")

with st.spinner("Fetching market depth data..."):
    data = []
    for token in TOKENS:
        buy_depth_raw = find_depth_amount(USD_TOKEN, token, False)
        buy_depth = buy_depth_raw / 10 ** USD_TOKEN['decimals'] if buy_depth_raw else 0.0
        sell_depth_raw = find_depth_amount(token, USD_TOKEN, True)
        sell_depth = sell_depth_raw / 10 ** USD_TOKEN['decimals'] if sell_depth_raw else 0.0
        data.append({
            'Token': token['symbol'],
            'Buy Depth (USD)': f"{buy_depth:,.2f}",
            'Sell Depth (USD)': f"{sell_depth:,.2f}",
        })
    df = pd.DataFrame(data)

st.dataframe(df, use_container_width=True)

time.sleep(60)
st.rerun()