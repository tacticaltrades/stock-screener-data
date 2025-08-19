import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd

API_KEY = os.environ.get('POLYGON_API_KEY')
BASE_URL = "https://api.polygon.io"

def load_existing_data():
    """Load existing historical data and rankings"""
    try:
        with open('historical_data.json', 'r') as f:
            historical = json.load(f)
        with open('rankings.json', 'r') as f:
            current_rankings = json.load(f)
        return historical, current_rankings
    except FileNotFoundError:
        print("No existing data found - run full rebuild first")
        return None, None

def get_daily_data(tickers, date):
    """Get yesterday's closing data for all tickers"""
    print(f"Fetching daily data for {date}...")
    
    # Use grouped daily bars endpoint for efficiency
    url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}"
    params = {'apikey': API_KEY}
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'results' in data:
            # Convert to dict for easy lookup
            daily_data = {result['T']: result for result in data['results']}
            return daily_data
    
    print(f"Failed to get daily data: {response.status_code}")
    return {}

def update_rs_calculations(historical_data, daily_data, sp500_daily):
    """Update RS calculations with new daily data"""
    updated_stocks = []
    
    for stock in historical_data['stocks']:
        symbol = stock['symbol']
        
        # Add new day's data if available
        if symbol in daily_data:
            new_bar = daily_data[symbol]
            stock['price_history'].append({
                't': new_bar['t'],
                'c': new_bar['c'],
                'v': new_bar['v']
            })
            
            # Keep only last 500 days to prevent file from growing too large
            if len(stock['price_history']) > 500:
                stock['price_history'] = stock['price_history'][-500:]
            
            # Recalculate RS score with updated data
            rs_data = calculate_rs_score(stock['price_history'], sp500_daily)
            if rs_data:
                updated_stocks.append({
                    'symbol': symbol,
                    'rs_score': rs_data['rs_score'],
                    'avg_volume': rs_data['avg_volume']
                })
    
    return updated_stocks

def main():
    print("=== Daily RS Update ===")
    
    # Load existing data
    historical_data, current_rankings = load_existing_data()
    if not historical_data:
        print("Run full rebuild first!")
        return
    
    # Get yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Get daily data
    daily_data = get_daily_data([], yesterday)
    if not daily_data:
        print("No daily data available")
        return
    
    # Get SPY data for benchmark
    spy_data = daily_data.get('SPY')
    if not spy_data:
        print("No SPY benchmark data")
        return
    
    # Update calculations
    updated_stocks = update_rs_calculations(historical_data, daily_data, spy_data)
    
    # Recalculate rankings
    updated_stocks.sort(key=lambda x: x['rs_score'], reverse=True)
    
    # Assign new rankings
    for i, stock in enumerate(updated_stocks):
        percentile = int(((len(updated_stocks) - i) / len(updated_stocks)) * 99) + 1
        stock['rs_rank'] = min(percentile, 99)
    
    # Save updated rankings
    output = {
        'last_updated': datetime.now().isoformat(),
        'total_stocks': len(updated_stocks),
        'update_type': 'daily_incremental',
        'data': updated_stocks
    }
    
    with open('rankings.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    # Update historical data file
    with open('historical_data.json', 'w') as f:
        json.dump(historical_data, f, indent=2)
    
    print(f"✅ Daily update complete - {len(updated_stocks)} stocks updated")

if __name__ == "__main__":
    main()
