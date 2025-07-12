import json
import requests
import math
import psycopg2
from datetime import datetime, timedelta
import os

# Token configurations
TOKENS = [
    {'symbol': 'ETH', 'address': '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7', 'decimals': 18},
    {'symbol': 'USDT', 'address': '0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8', 'decimals': 6},
    {'symbol': 'tBTC', 'address': '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d', 'decimals': 18},
    {'symbol': 'EKUBO', 'address': '0x075afe6402ad5a5c20dd25e10ec3b3986acaa647b77e4ae24b0cbc9a54a27a87', 'decimals': 18},
    {'symbol': 'WBTC', 'address': '0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac', 'decimals': 8},
    {'symbol': 'STRK', 'address': '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d', 'decimals': 18},
]

USD_TOKEN = {'symbol': 'USDC', 'address': '0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8', 'decimals': 6}

def get_db_connection():
    """Get database connection using environment variable"""
    db_conn_string = os.environ['DB_CONN_STRING']
    return psycopg2.connect(db_conn_string)

def insert_depths(token_symbol, buy_depth, sell_depth):
    """Insert market depth data into database"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO depths (token, buy_depth, sell_depth) VALUES (%s, %s, %s)",
            (token_symbol, buy_depth, sell_depth)
        )
        conn.commit()
        print(f"Successfully inserted depths for {token_symbol}: buy={buy_depth}, sell={sell_depth}")
    except Exception as e:
        print(f"Error inserting depths for {token_symbol}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_latest_depths(token_symbol):
    """Get latest depth data for a token"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT buy_depth, sell_depth, timestamp FROM depths WHERE token = %s ORDER BY timestamp DESC LIMIT 1",
            (token_symbol,)
        )
        result = cur.fetchone()
        return result  # (buy, sell, timestamp) or None
    except Exception as e:
        print(f"Error fetching latest depths for {token_symbol}: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def fetch_quote(sell_address, buy_address, sell_amount):
    """Fetch quote from AVNU API"""
    url = "https://starknet.api.avnu.fi/swap/v2/quotes"
    params = {
        'sellTokenAddress': sell_address,
        'buyTokenAddress': buy_address,
        'sellAmount': hex(sell_amount),
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"API request failed with status {resp.status_code}")
            return None
        data = resp.json()
        if not data:
            print("Empty response from API")
            return None
        quote = data[0]
        return {
            'sell_amount': int(quote['sellAmount'], 0),
            'buy_amount': int(quote['buyAmount'], 0),
            'sell_token_price_in_usd': quote['sellTokenPriceInUsd'],
            'buy_token_price_in_usd': quote['buyTokenPriceInUsd'],
            'gas_fees_in_usd': quote['gasFeesInUsd'] + quote['avnuFeesInUsd'],
        }
    except Exception as e:
        print(f"Error fetching quote: {e}")
        return None

def compute_slippage(quote, sell_dec, buy_dec):
    """Calculate slippage from quote data"""
    sell_amount = quote['sell_amount'] / 10 ** sell_dec
    buy_amount = quote['buy_amount'] / 10 ** buy_dec
    sell_usd = sell_amount * quote['sell_token_price_in_usd']
    buy_usd = buy_amount * quote['buy_token_price_in_usd']
    if buy_usd == 0:
        return float('inf')
    return 1 - (sell_usd / buy_usd)

def find_depth_amount(sell_token, buy_token, is_sell_side, token_symbol):
    """Find the depth amount for 2% slippage using binary search"""
    TARGET_SLIPPAGE = 0.02
    TOLERANCE_FROM_TARGET = 0.001
    MAX_ITERATIONS = 20
    MIN_AMOUNT_USD = 10000.0
    MAX_AMOUNT_USD = 500000000.0
    RANGE_FACTOR_LOW = 0.5
    RANGE_FACTOR_HIGH = 2.0

    # Get small quote to determine price
    small_amount = max(1, 10 ** (sell_token['decimals']))
    small_quote = fetch_quote(sell_token['address'], buy_token['address'], small_amount)
    if not small_quote:
        print(f"Failed to get small quote for {token_symbol}")
        return None
    
    sell_price = small_quote['sell_token_price_in_usd']
    if sell_price <= 0:
        print(f"Invalid sell price for {token_symbol}: {sell_price}")
        return None

    # Default full range
    min_amount = math.ceil(MIN_AMOUNT_USD / sell_price * 10 ** sell_token['decimals'])
    max_amount = math.ceil(MAX_AMOUNT_USD / sell_price * 10 ** sell_token['decimals'])

    # Narrow range using last known depth if available
    last_data = get_latest_depths(token_symbol)
    if last_data:
        last_depth = last_data[1] if is_sell_side else last_data[0]  # sell or buy
        if last_depth and last_depth > 0 and sell_price > 0:
            last_depth_float = float(last_depth)
            # Convert USD depth to token amount
            last_amount = last_depth_float / sell_price * 10 ** sell_token['decimals']
            min_amount = max(min_amount, math.ceil(last_amount * RANGE_FACTOR_LOW))
            max_amount = min(max_amount, math.ceil(last_amount * RANGE_FACTOR_HIGH))

    print(f"Binary search for {token_symbol}, range: {min_amount} - {max_amount}")

    # Binary search for target slippage
    for iteration in range(MAX_ITERATIONS):
        amount = (min_amount + max_amount) // 2
        if amount == 0:
            print(f"Amount reached 0 for {token_symbol}")
            return None
            
        quote = fetch_quote(sell_token['address'], buy_token['address'], amount)
        if not quote:
            print(f"Failed to get quote at iteration {iteration} for {token_symbol}")
            return None
            
        slippage = compute_slippage(quote, sell_token['decimals'], buy_token['decimals'])
        diff = abs(slippage - TARGET_SLIPPAGE)
        
        print(f"Iteration {iteration}: amount={amount}, slippage={slippage:.4f}, diff={diff:.4f}")
        
        if diff < TOLERANCE_FROM_TARGET:
            result = quote['buy_amount'] if is_sell_side else amount
            print(f"Found target slippage for {token_symbol}: {result}")
            return result
            
        if slippage < TARGET_SLIPPAGE:
            min_amount = amount
        else:
            max_amount = amount
            
        if max_amount - min_amount <= 10:
            result = quote['buy_amount'] if is_sell_side else amount
            print(f"Converged for {token_symbol}: {result}")
            return result
            
    print(f"Max iterations reached for {token_symbol}")
    return None

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    print("Starting market depth data collection...")
    
    successful_updates = 0
    total_tokens = len(TOKENS)
    
    for token in TOKENS:
        try:
            print(f"\nProcessing token: {token['symbol']}")
            
            # Calculate buy depth (USD -> Token)
            buy_depth_raw = find_depth_amount(USD_TOKEN, token, False, token['symbol'])
            buy_depth = buy_depth_raw / 10 ** USD_TOKEN['decimals'] if buy_depth_raw else 0.0
            
            # Calculate sell depth (Token -> USD)
            sell_depth_raw = find_depth_amount(token, USD_TOKEN, True, token['symbol'])
            sell_depth = sell_depth_raw / 10 ** USD_TOKEN['decimals'] if sell_depth_raw else 0.0
            
            print(f"Token: {token['symbol']}, Buy depth: {buy_depth}, Sell depth: {sell_depth}")
            
            # Only insert if both depths are greater than 0
            if buy_depth > 0 and sell_depth > 0:
                insert_depths(token['symbol'], buy_depth, sell_depth)
                successful_updates += 1
                print(f"Successfully processed {token['symbol']}")
            else:
                print(f"Skipping {token['symbol']} - invalid depths")
                
        except Exception as e:
            print(f"Error processing {token['symbol']}: {e}")
            continue
    
    # Return response
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Market depth data collection completed',
            'successful_updates': successful_updates,
            'total_tokens': total_tokens,
            'timestamp': datetime.now().isoformat()
        })
    }

# For local testing
if __name__ == "__main__":
    # Set environment variable for local testing
    # os.environ['DB_CONN_STRING'] = 'your_connection_string_here'
    
    # Test the lambda handler
    result = lambda_handler({}, {})
    print("Lambda result:", result)