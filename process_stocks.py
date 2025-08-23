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
                        len(symbol) <= 6 and  # Allow 6-character symbols
                        not symbol.endswith('.') and  # Avoid most preferreds
                        symbol.replace('.', '').isalnum()):  # Allow numbers in symbols
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
            if response.status_code != 404:  # Don't log 404s (delisted stocks)
                print(f"Error for {ticker}: {response.status_code}")
    except Exception as e:
        print(f"Exception for {ticker}: {e}")
    
    return None

def get_sp500_benchmark(start_date, end_date):
    """Get S&P 500 benchmark data using SPY ETF"""
    print("Fetching S&P 500 benchmark data...")
    return get_stock_data('SPY', start_date, end_date)

def calculate_aligned_returns(stock_prices, sp500_prices):
    """Calculate stock returns relative to S&P 500 benchmark"""
    if not stock_prices or not sp500_prices:
        return None, None, None
    
    if len(stock_prices) < 252 or len(sp500_prices) < 252:  # Need at least 1 year
        return None, None, None
    
    # Sort by timestamp
    stock_prices = sorted(stock_prices, key=lambda x: x['t'])
    sp500_prices = sorted(sp500_prices, key=lambda x: x['t'])
    
    # Create dataframes for alignment
    stock_df = pd.DataFrame(stock_prices)
    stock_df['date'] = pd.to_datetime(stock_df['t'], unit='ms')
    stock_df = stock_df.set_index('date')
    
    spy_df = pd.DataFrame(sp500_prices)
    spy_df['date'] = pd.to_datetime(spy_df['t'], unit='ms')
    spy_df = spy_df.set_index('date')
    
    # Align dates (only trading days where both have data)
    aligned = stock_df.join(spy_df, rsuffix='_spy', how='inner')
    
    if len(aligned) < 252:  # Need sufficient aligned data
        return None, None, None
    
    # Get current prices
    current_stock = aligned['c'].iloc[-1]
    current_spy = aligned['c_spy'].iloc[-1]
    
    # Calculate returns for IBD periods
    periods = {
        '3m': 63,   # ~3 months
        '6m': 126,  # ~6 months  
        '9m': 189,  # ~9 months
        '12m': 252  # ~12 months
    }
    
    stock_returns = {}
    relative_returns = {}
    
    for period, days in periods.items():
        if len(aligned) > days:
            # Stock return
            old_stock = aligned['c'].iloc[-(days+1)]
            if old_stock > 0:
                stock_return = (current_stock - old_stock) / old_stock
            else:
                stock_return = 0
            
            # S&P 500 return
            old_spy = aligned['c_spy'].iloc[-(days+1)]
            if old_spy > 0:
                spy_return = (current_spy - old_spy) / old_spy
            else:
                spy_return = 0
            
            # Relative performance (stock return - benchmark return)
            relative_return = stock_return - spy_return
            
            stock_returns[period] = stock_return
            relative_returns[period] = relative_return
        else:
            stock_returns[period] = 0
            relative_returns[period] = 0
    
    # Calculate average volume (last 20 days)
    recent_volumes = [float(p['v']) for p in stock_prices[-20:] if p['v'] > 0]
    avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
    
    return relative_returns, stock_returns, avg_volume

def calculate_ibd_rs_score(relative_returns):
    """Calculate IBD-style RS score using the discovered formula
    
    Formula: RS = 2×(3-month relative) + (6-month relative) + (9-month relative) + (12-month relative)
    Where relative = (stock return - S&P 500 return)
    """
    if not relative_returns:
        return 0
    
    # IBD RS Score with 3-month period weighted 2x
    rs_score = (
        2 * relative_returns.get('3m', 0) +
        1 * relative_returns.get('6m', 0) +
        1 * relative_returns.get('9m', 0) +
        1 * relative_returns.get('12m', 0)
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

def format_return(return_val):
    """Format return as percentage"""
    return f"{return_val*100:.1f}%"

def main():
    print("=== IBD-Style Relative Strength Stock Processor (FULL REBUILD) ===")
    print("Using discovered formula: RS = 2×(3m relative) + 6m + 9m + 12m relative performance vs S&P 500")
    
    if not API_KEY:
        print("ERROR: POLYGON_API_KEY not found!")
        print("Set it with: export POLYGON_API_KEY='your_key_here'")
        return
    
    # Date range for historical data (need extra buffer for alignment)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=450)  # Extra buffer for weekends/holidays
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Fetching data from {start_date_str} to {end_date_str}")
    
    # Get S&P 500 benchmark first
    sp500_data = get_sp500_benchmark(start_date_str, end_date_str)
    if not sp500_data:
        print("ERROR: Failed to get S&P 500 benchmark data!")
        return
    
    print(f"Got {len(sp500_data)} days of S&P 500 benchmark data")
    
    # Get all tickers
    tickers = get_all_tickers()
    if not tickers:
        print("Failed to get tickers!")
        return
    
    # Limit to reasonable number for processing time
    tickers = tickers[:5000]  # Process top 5000 stocks
    print(f"Processing {len(tickers)} stocks...")
    
    all_stock_data = []
    historical_stocks = []  # Store historical data for daily updates
    processed = 0
    failed = 0
    
    for i, ticker in enumerate(tickers):
        try:
            # Progress indicator
            if i % 100 == 0:
                print(f"Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%) - Processed: {processed}, Failed: {failed}")
            
            # Get historical data
            stock_prices = get_stock_data(ticker, start_date_str, end_date_str)
            
            if stock_prices:
                result = calculate_aligned_returns(stock_prices, sp500_data)
                if result[0] is not None:  # Check if we got valid data
                    relative_returns, stock_returns, avg_volume = result
                    rs_score = calculate_ibd_rs_score(relative_returns)
                    
                    all_stock_data.append({
                        'symbol': ticker,
                        'rs_score': rs_score,
                        'avg_volume': int(avg_volume),
                        'relative_3m': relative_returns['3m'],
                        'relative_6m': relative_returns['6m'], 
                        'relative_9m': relative_returns['9m'],
                        'relative_12m': relative_returns['12m'],
                        'stock_return_3m': stock_returns['3m'],
                        'stock_return_12m': stock_returns['12m']
                    })
                    
                    # Store ultra-minimal historical data (only what we need for RS calculations)
                    # Only store every 5th day to reduce size, plus recent 30 days for volume calc
                    minimal_history = []
                    
                    # Get every 5th day for older data (for RS calculation periods)
                    older_data = stock_prices[:-30:5]  # Every 5th day, excluding recent 30
                    for price in older_data:
                        minimal_history.append({
                            't': price['t'],
                            'c': price['c']  # Only closing price, no volume for old data
                        })
                    
                    # Get all recent 30 days (for volume calculation)
                    recent_data = stock_prices[-30:]
                    for price in recent_data:
                        minimal_history.append({
                            't': price['t'],
                            'c': price['c'],
                            'v': price['v']  # Include volume for recent data
                        })
                    
                    historical_stocks.append({
                        's': ticker,  # Shorter field name
                        'h': minimal_history,  # Shorter field name
                        'u': datetime.now().isoformat()  # Shorter field name
                    })
                    
                    processed += 1
                else:
                    failed += 1
            else:
                failed += 1
            
            # Rate limiting - respect API limits (5 calls per second = 0.2 seconds between calls)
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            failed += 1
            continue
    
    print(f"\nProcessing complete!")
    print(f"Successfully processed: {processed} stocks")
    print(f"Failed: {failed} stocks")
    
    # Calculate percentile rankings
    if all_stock_data:
        print("\nCalculating IBD-style RS percentile rankings...")
        
        # Sort by RS score and assign rankings
        all_stock_data.sort(key=lambda x: x['rs_score'], reverse=True)
        
        # Assign percentile rankings (1-99)
        total_stocks = len(all_stock_data)
        for i, stock in enumerate(all_stock_data):
            # Percentile: what percentage of stocks this stock beats
            percentile = int(((total_stocks - i) / total_stocks) * 99) + 1
            stock['rs_rank'] = min(percentile, 99)
        
        # Format for output
        output_data = []
        for stock in all_stock_data:
            output_data.append({
                'symbol': stock['symbol'],
                'rs_rank': stock['rs_rank'],
                'rs_score': round(stock['rs_score'], 4),
                'avg_volume': format_volume(stock['avg_volume']),
                'raw_volume': stock['avg_volume'],
                'relative_3m': format_return(stock['relative_3m']),
                'relative_12m': format_return(stock['relative_12m']),
                'stock_return_3m': format_return(stock['stock_return_3m']),
                'stock_return_12m': format_return(stock['stock_return_12m'])
            })
        
        # Save main rankings JSON file
        output = {
            'last_updated': datetime.now().isoformat(),
            'formula_used': 'RS = 2×(3m relative vs S&P500) + 6m + 9m + 12m relative performance',
            'total_stocks': len(output_data),
            'benchmark': 'S&P 500 (SPY)',
            'update_type': 'full_rebuild',
            'data': output_data
        }
        
        with open('rankings.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"✅ Successfully saved {len(output_data)} stocks to 'rankings.json'")
        
        # Save ultra-minimal historical data 
        minimal_spy_data = []
        # SPY data: every 5th day for older data, all recent 30 days
        older_spy = sp500_data[:-30:5]
        recent_spy = sp500_data[-30:]
        
        for bar in older_spy:
            minimal_spy_data.append({'t': bar['t'], 'c': bar['c']})
        for bar in recent_spy:
            minimal_spy_data.append({'t': bar['t'], 'c': bar['c'], 'v': bar['v']})
        
        historical_output = {
            'u': datetime.now().isoformat(),  # Shorter field names
            's': minimal_spy_data,  # SPY data
            'n': len(historical_stocks),  # Total stocks
            'd': historical_stocks  # Stock data
        }
        
        with open('historical_data.json', 'w') as f:
            json.dump(historical_output, f, indent=2)
        
        print(f"✅ Historical data saved for daily updates ({len(historical_stocks)} stocks)")
        
        # Show top performers
        print(f"\n🏆 Top 20 IBD-Style RS Rankings:")
        print("Rank | Symbol | RS | 3M Rel | 12M Rel | Volume")
        print("-" * 55)
        for i, stock in enumerate(output_data[:20]):
            print(f"{i+1:2d}   | {stock['symbol']:6s} | {stock['rs_rank']:2d} | {stock['relative_3m']:7s} | {stock['relative_12m']:8s} | {stock['avg_volume']:>8s}")
        
        # Show some statistics
        rs_scores = [s['rs_score'] for s in all_stock_data]
        print(f"\n📊 RS Score Statistics:")
        print(f"   Highest RS Score: {max(rs_scores):.3f}")
        print(f"   Lowest RS Score: {min(rs_scores):.3f}")
        print(f"   Average RS Score: {np.mean(rs_scores):.3f}")
        print(f"   Median RS Score: {np.median(rs_scores):.3f}")
        
        # Count high RS stocks
        high_rs_count = len([s for s in output_data if s['rs_rank'] >= 90])
        print(f"   Stocks with RS ≥ 90: {high_rs_count}")
        
    else:
        print("❌ No stock data was successfully processed")
        print("Check your API key and internet connection.")

if __name__ == "__main__":
    main()
