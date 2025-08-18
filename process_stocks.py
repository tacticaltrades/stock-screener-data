import requests
import json
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

API_KEY = os.environ.get('POLYGON_API_KEY')
BASE_URL = "https://api.polygon.io"

def get_all_tickers():
    """Get all active US stock tickers"""
    print("Fetching all active US stock tickers...")
    url = f"{BASE_URL}/v3/reference/tickers"
    params = {
        'market': 'stocks',
        'active': 'true',
        'limit': 1000,
        'apikey': API_KEY
    }
    
    all_tickers = []
    page_count = 0
    
    while True:
        try:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"API Error: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            page_count += 1
            print(f"Processing page {page_count}...")
            
            if 'results' in data:
                # Filter for US stocks only, exclude complex symbols
                us_stocks = []
                for ticker in data['results']:
                    symbol = ticker.get('ticker', '')
                    if (ticker.get('market') == 'stocks' and 
                        ticker.get('locale') == 'us' and
                        len(symbol) <= 5 and  # Avoid complex symbols
                        '.' not in symbol and  # Avoid preferred shares
                        symbol.isalpha()):     # Only letters
                        us_stocks.append(symbol)
                
                all_tickers.extend(us_stocks)
                print(f"Added {len(us_stocks)} tickers, total: {len(all_tickers)}")
            
            # Check for next page
            if 'next_url' not in data:
                break
            url = data['next_url'] + f"&apikey={API_KEY}"
            
            # Rate limiting - be conservative
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching tickers: {e}")
            break
    
    print(f"Found {len(all_tickers)} total US stocks")
    return all_tickers

def get_stock_data(ticker, start_date, end_date):
    """Get historical data for a single stock"""
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    params = {'apikey': API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'results' in data and len(data['results']) > 200:  # Need sufficient data
                return data['results']
        elif response.status_code == 429:  # Rate limited
            print(f"Rate limited for {ticker}, waiting...")
            time.sleep(2)
            return get_stock_data(ticker, start_date, end_date)  # Retry
        else:
            print(f"Error for {ticker}: {response.status_code}")
    except Exception as e:
        print(f"Exception for {ticker}: {e}")
    
    return None

def calculate_returns(prices):
    """Calculate returns for different periods"""
    if len(prices) < 252:  # Need at least 1 year
        return None
    
    # Sort by timestamp
    prices = sorted(prices, key=lambda x: x['t'])
    current_price = prices[-1]['c']  # Close price
    
    # Calculate returns (approximate trading days)
    returns = {}
    periods = {
        '3m': 63,   # ~3 months
        '6m': 126,  # ~6 months  
        '9m': 189,  # ~9 months
        '12m': 252  # ~12 months
    }
    
    for period, days in periods.items():
        if len(prices) > days:
            old_price = prices[-(days+1)]['c']
            if old_price > 0:
                returns[period] = (current_price - old_price) / old_price
            else:
                returns[period] = 0
        else:
            returns[period] = 0
    
    # Calculate average volume (last 20 days)
    recent_volumes = [p['v'] for p in prices[-20:]]
    avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
    
    return returns, avg_volume

def calculate_rs_score(returns):
    """Calculate IBD-style RS score"""
    if not returns:
        return 0
    
    # RS Score = (2 × 3-month) + (1 × 6-month) + (1 × 9-month) + (1 × 12-month)
    rs_score = (
        2 * returns.get('3m', 0) +
        1 * returns.get('6m', 0) +
        1 * returns.get('9m', 0) +
        1 * returns.get('12m', 0)
    )
    
    return rs_score

def format_volume(volume):
    """Format volume as XXXk or XXXm"""
    if volume >= 1000000:
        return f"{volume/1000000:.1f}M"
    elif volume >= 1000:
        return f"{volume/1000:.0f}k"
    else:
        return str(int(volume))

def main():
    print("=== Starting Full Stock Data Processing ===")
    
    if not API_KEY:
        print("ERROR: POLYGON_API_KEY not found!")
        return
    
    # Date range for historical data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=400)  # Extra buffer for weekends/holidays
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Fetching data from {start_date_str} to {end_date_str}")
    
    # Get all tickers
    tickers = get_all_tickers()
    if not tickers:
        print("Failed to get tickers!")
        return
    
    # Limit to reasonable number for processing time
    tickers = tickers[:3000]  # Process top 3000 stocks
    print(f"Processing {len(tickers)} stocks...")
    
    all_stock_data = []
    processed = 0
    
    for i, ticker in enumerate(tickers):
        try:
            # Progress indicator
            if i % 100 == 0:
                print(f"Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%)")
            
            # Get historical data
            prices = get_stock_data(ticker, start_date_str, end_date_str)
            
            if prices:
                result = calculate_returns(prices)
                if result:
                    returns, avg_volume = result
                    rs_score = calculate_rs_score(returns)
                    
                    all_stock_data.append({
                        'symbol': ticker,
                        'rs_score': rs_score,
                        'avg_volume': int(avg_volume),
                        'returns_3m': returns['3m'],
                        'returns_6m': returns['6m'], 
                        'returns_9m': returns['9m'],
                        'returns_12m': returns['12m']
                    })
                    processed += 1
            
            # Rate limiting - respect API limits
            time.sleep(0.07)  # ~14 requests per second, well under 1000/minute limit
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue
    
    print(f"Successfully processed {processed} stocks")
    
    # Calculate percentile rankings
    if all_stock_data:
        print("Calculating RS percentile rankings...")
        
        # Sort by RS score and assign rankings
        all_stock_data.sort(key=lambda x: x['rs_score'], reverse=True)
        
        # Assign percentile rankings (1-99)
        total_stocks = len(all_stock_data)
        for i, stock in enumerate(all_stock_data):
            percentile = int(((total_stocks - i) / total_stocks) * 99) + 1
            stock['rs_rank'] = min(percentile, 99)
        
        # Format for output
        output_data = []
        for stock in all_stock_data:
            output_data.append({
                'symbol': stock['symbol'],
                'rs_rank': stock['rs_rank'],
                'avg_volume': format_volume(stock['avg_volume']),
                'raw_volume': stock['avg_volume']
            })
        
        # Save to JSON file
        output = {
            'last_updated': datetime.now().isoformat(),
            'total_stocks': len(output_data),
            'data': output_data
        }
        
        with open('rankings.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"✅ Successfully saved {len(output_data)} stocks to rankings.json")
        print(f"Top 10 RS Rankings:")
        for i, stock in enumerate(output_data[:10]):
            print(f"{i+1}. {stock['symbol']} - RS Rank: {stock['rs_rank']}")
    
    else:
        print("❌ No stock data was successfully processed")

if __name__ == "__main__":
    main()
