import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

API_KEY = os.environ['POLYGON_API_KEY']
BASE_URL = "https://api.polygon.io"

def get_all_tickers():
    """Get all active stock tickers"""
    url = f"{BASE_URL}/v3/reference/tickers"
    params = {
        'market': 'stocks',
        'active': 'true',
        'limit': 1000,
        'apikey': API_KEY
    }
    
    all_tickers = []
    
    while True:
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'results' in data:
            # Filter for US stocks only
            us_stocks = [
                ticker['ticker'] for ticker in data['results'] 
                if ticker.get('market') == 'stocks' and 
                ticker.get('locale') == 'us' and
                len(ticker['ticker']) <= 5  # Avoid complex symbols
            ]
            all_tickers.extend(us_stocks)
        
        # Check for next page
        if 'next_url' not in data:
            break
        url = data['next_url'] + f"&apikey={API_KEY}"
    
    return all_tickers[:8000]  # Limit to manageable number

def get_bulk_daily_data(date):
    """Get all stocks data for a specific date using bulk endpoint"""
    url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}"
    params = {'apikey': API_KEY}
    
    response = requests.get(url, params=params)
    return response.json()

def calculate_returns(prices_df):
    """Calculate returns for different periods"""
    if len(prices_df) < 252:  # Need at least 1 year of data
        return None
    
    current_price = prices_df.iloc[-1]['close']
    
    # Calculate returns (approximate trading days)
    returns = {}
    periods = {
        '3m': 63,   # ~3 months
        '6m': 126,  # ~6 months  
        '9m': 189,  # ~9 months
        '12m': 252  # ~12 months
    }
    
    for period, days in periods.items():
        if len(prices_df) > days:
            old_price = prices_df.iloc[-(days+1)]['close']
            returns[period] = (current_price - old_price) / old_price
        else:
            returns[period] = 0
    
    return returns

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

def get_sp500_return():
    """Get S&P 500 returns for comparison"""
    # Use SPY as S&P 500 proxy
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    url = f"{BASE_URL}/v2/aggs/ticker/SPY/range/1/day/{start_date}/{end_date}"
    params = {'apikey': API_KEY}
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if 'results' in data and len(data['results']) > 0:
        prices = pd.DataFrame(data['results'])
        prices['date'] = pd.to_datetime(prices['t'], unit='ms')
        prices = prices.sort_values('date')
        return calculate_returns(prices[['close']])
    
    return None

def main():
    print("Starting stock data processing...")
    
    # Get S&P 500 benchmark returns
    print("Getting S&P 500 benchmark data...")
    sp500_returns = get_sp500_return()
    if not sp500_returns:
        print("Failed to get S&P 500 data")
        return
    
    # Get recent trading dates (last 252 days)
    end_date = datetime.now()
    dates = []
    current_date = end_date
    
    # Generate last 300 calendar days to ensure we get 252 trading days
    for i in range(300):
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date -= timedelta(days=1)
    
    # Get all tickers
    print("Getting all stock tickers...")
    tickers = get_all_tickers()
    print(f"Processing {len(tickers)} stocks...")
    
    all_stock_data = []
    
    # Process in batches to avoid overwhelming the API
    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1}")
        
        for ticker in batch:
            try:
                # Get historical data for this stock
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                end_date = datetime.now().strftime('%Y-%m-%d')
                
                url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
                params = {'apikey': API_KEY}
                
                response = requests.get(url, params=params)
                data = response.json()
                
                if 'results' in data and len(data['results']) > 200:  # Need sufficient data
                    prices_df = pd.DataFrame(data['results'])
                    prices_df['date'] = pd.to_datetime(prices_df['t'], unit='ms')
                    prices_df = prices_df.sort_values('date')
                    
                    # Calculate returns
                    returns = calculate_returns(prices_df[['close']])
                    if returns:
                        rs_score = calculate_rs_score(returns)
                        
                        # Get current volume (average of last 20 days)
                        avg_volume = prices_df.tail(20)['v'].mean()
                        
                        all_stock_data.append({
                            'symbol': ticker,
                            'rs_score': rs_score,
                            'avg_volume': int(avg_volume),
                            'returns_3m': returns['3m'],
                            'returns_6m': returns['6m'], 
                            'returns_9m': returns['9m'],
                            'returns_12m': returns['12m']
                        })
                
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                continue
    
    # Calculate percentile rankings
    if all_stock_data:
        df = pd.DataFrame(all_stock_data)
        df['rs_rank'] = pd.qcut(df['rs_score'].rank(method='first'), 99, labels=range(1, 100)).astype(int)
        
        # Format for output
        output_data = []
        for _, row in df.iterrows():
            # Format volume as "XXXk" or "XXXm"
            if row['avg_volume'] >= 1000000:
                volume_str = f"{row['avg_volume']/1000000:.1f}M"
            else:
                volume_str = f"{row['avg_volume']/1000:.0f}k"
            
            output_data.append({
                'symbol': row['symbol'],
                'rs_rank': int(row['rs_rank']),
                'avg_volume': volume_str,
                'raw_volume': int(row['avg_volume'])
            })
        
        # Sort by RS rank (highest first)
        output_data.sort(key=lambda x: x['rs_rank'], reverse=True)
        
        # Save to JSON file
        with open('rankings.json', 'w') as f:
            json.dump({
                'last_updated': datetime.now().isoformat(),
                'total_stocks': len(output_data),
                'data': output_data
            }, f, indent=2)
        
        print(f"Successfully processed {len(output_data)} stocks")
        print("Data saved to rankings.json")
    
    else:
        print("No stock data was successfully processed")

if __name__ == "__main__":
    main()
