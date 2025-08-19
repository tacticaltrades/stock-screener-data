import requests
import json
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

API_KEY = os.environ.get('POLYGON_API_KEY')
BASE_URL = "https://api.polygon.io"

def load_existing_data():
    """Load existing historical data and rankings"""
    try:
        with open('historical_data.json', 'r') as f:
            historical = json.load(f)
        print(f"✅ Loaded historical data for {len(historical.get('d', []))} stocks")
        return historical
    except FileNotFoundError:
        print("❌ No existing historical data found - run full rebuild first (process_stocks.py)")
        return None
    except Exception as e:
        print(f"❌ Error loading historical data: {e}")
        return None

def get_daily_data(date):
    """Get yesterday's closing data for all tickers using grouped daily bars"""
    print(f"Fetching daily market data for {date}...")
    
    # Use grouped daily bars endpoint for efficiency - gets all stocks at once
    url = f"{BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}"
    params = {'apikey': API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if 'results' in data and len(data['results']) > 0:
                # Convert to dict for easy lookup by symbol
                daily_data = {result['T']: result for result in data['results']}
                print(f"✅ Got daily data for {len(daily_data)} stocks")
                return daily_data
            else:
                print("⚠️  No results in daily data response")
                return {}
        elif response.status_code == 429:
            print("⚠️  Rate limited, waiting 30 seconds...")
            time.sleep(30)
            return get_daily_data(date)
        else:
            print(f"❌ API Error getting daily data: {response.status_code} - {response.text}")
            return {}
    except Exception as e:
        print(f"❌ Exception getting daily data: {e}")
        return {}

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
    """Calculate IBD-style RS score using the discovered formula"""
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

def update_rs_calculations(historical_data, daily_data):
    """Update RS calculations with new daily data"""
    print("📊 Updating RS calculations...")
    
    updated_stocks = []
    sp500_data = historical_data.get('s', [])  # Fixed: using 's' instead of 'sp500_data'
    
    # Add new SPY data if available
    if 'SPY' in daily_data:
        spy_bar = daily_data['SPY']
        new_spy_data = {
            't': spy_bar['t'],
            'c': spy_bar['c'],
            'v': spy_bar['v']  # Only keeping essential fields like the rebuild script
        }
        sp500_data.append(new_spy_data)
        # Keep only last 300 days
        sp500_data = sp500_data[-300:]
        historical_data['s'] = sp500_data  # Fixed: using 's' instead of 'sp500_data'
        print(f"✅ Updated SPY benchmark data")
    else:
        print("⚠️  No SPY data available for today")
        return []
    
    processed = 0
    failed = 0
    
    for i, stock in enumerate(historical_data.get('d', [])):  # Fixed: using 'd' instead of 'stocks'
        symbol = stock['s']  # Fixed: using 's' instead of 'symbol'
        
        try:
            # Add new day's data if available
            if symbol in daily_data:
                daily_bar = daily_data[symbol]
                new_price_data = {
                    't': daily_bar['t'],
                    'c': daily_bar['c'],
                    'v': daily_bar['v']  # Only keeping essential fields like the rebuild script
                }
                
                # Add to historical data
                stock['h'].append(new_price_data)  # Fixed: using 'h' instead of 'price_history'
                
                # Keep only last 300 days to prevent file from growing too large
                stock['h'] = stock['h'][-300:]  # Fixed: using 'h' instead of 'price_history'
                stock['u'] = datetime.now().isoformat()  # Fixed: using 'u' instead of 'last_updated'
                
                # Recalculate RS score with updated data
                result = calculate_aligned_returns(stock['h'], sp500_data)  # Fixed: using 'h' instead of 'price_history'
                if result[0] is not None:
                    relative_returns, stock_returns, avg_volume = result
                    rs_score = calculate_ibd_rs_score(relative_returns)
                    
                    updated_stocks.append({
                        'symbol': symbol,
                        'rs_score': rs_score,
                        'avg_volume': int(avg_volume),
                        'relative_3m': relative_returns['3m'],
                        'relative_6m': relative_returns['6m'], 
                        'relative_9m': relative_returns['9m'],
                        'relative_12m': relative_returns['12m'],
                        'stock_return_3m': stock_returns['3m'],
                        'stock_return_12m': stock_returns['12m']
                    })
                    processed += 1
                else:
                    failed += 1
            else:
                # No new data for this stock - skip or use old calculation
                failed += 1
                
        except Exception as e:
            print(f"❌ Error updating {symbol}: {e}")
            failed += 1
            continue
    
    print(f"📊 Daily update complete: {processed} updated, {failed} failed")
    return updated_stocks

def main():
    print("=== IBD-Style RS Daily Update ===")
    print("Performing incremental update with yesterday's data...")
    
    if not API_KEY:
        print("❌ ERROR: POLYGON_API_KEY not found!")
        return
    
    # Load existing historical data
    historical_data = load_existing_data()
    if not historical_data:
        print("❌ Cannot proceed without historical data. Run process_stocks.py first.")
        return
    
    # Get yesterday's date (markets are usually 1 day behind)
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Skip weekends - if yesterday was Saturday or Sunday, get Friday's data
    yesterday_dt = datetime.now() - timedelta(days=1)
    if yesterday_dt.weekday() == 5:  # Saturday
        yesterday_dt = yesterday_dt - timedelta(days=1)  # Get Friday
    elif yesterday_dt.weekday() == 6:  # Sunday
        yesterday_dt = yesterday_dt - timedelta(days=2)  # Get Friday
    
    yesterday = yesterday_dt.strftime('%Y-%m-%d')
    print(f"📅 Getting data for: {yesterday}")
    
    # Get daily data for all stocks
    daily_data = get_daily_data(yesterday)
    if not daily_data:
        print("❌ No daily data available - market might be closed or API issue")
        return
    
    # Update calculations
    updated_stocks = update_rs_calculations(historical_data, daily_data)
    if not updated_stocks:
        print("❌ No stocks were updated")
        return
    
    # Sort and assign new rankings
    print("🏆 Calculating new RS rankings...")
    updated_stocks.sort(key=lambda x: x['rs_score'], reverse=True)
    
    # Assign percentile rankings (1-99)
    total_stocks = len(updated_stocks)
    for i, stock in enumerate(updated_stocks):
        percentile = int(((total_stocks - i) / total_stocks) * 99) + 1
        stock['rs_rank'] = min(percentile, 99)
    
    # Format for output
    output_data = []
    for stock in updated_stocks:
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
    
    # Save updated rankings
    output = {
        'last_updated': datetime.now().isoformat(),
        'formula_used': 'RS = 2×(3m relative vs S&P500) + 6m + 9m + 12m relative performance',
        'total_stocks': len(output_data),
        'benchmark': 'S&P 500 (SPY)',
        'update_type': 'daily_incremental',
        'data_date': yesterday,
        'data': output_data
    }
    
    with open('rankings.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"✅ Updated rankings saved - {len(output_data)} stocks")
    
    # Update historical data file with shorter field names structure
    historical_data['u'] = datetime.now().isoformat()  # Fixed: using 'u' instead of 'last_updated'
    with open('historical_data.json', 'w') as f:
        json.dump(historical_data, f, indent=2)
    
    print(f"✅ Historical data updated")
    
    # Show top performers
    print(f"\n🏆 Top 20 RS Rankings (Updated {yesterday}):")
    print("Rank | Symbol | RS | 3M Rel | 12M Rel | Volume")
    print("-" * 55)
    for i, stock in enumerate(output_data[:20]):
        print(f"{i+1:2d}   | {stock['symbol']:6s} | {stock['rs_rank']:2d} | {stock['relative_3m']:7s} | {stock['relative_12m']:8s} | {stock['avg_volume']:>8s}")
    
    # Show update statistics
    rs_scores = [s['rs_score'] for s in updated_stocks]
    high_rs_count = len([s for s in output_data if s['rs_rank'] >= 90])
    
    print(f"\n📊 Daily Update Statistics:")
    print(f"   Stocks Updated: {len(updated_stocks)}")
    print(f"   Highest RS Score: {max(rs_scores):.3f}")
    print(f"   Lowest RS Score: {min(rs_scores):.3f}")
    print(f"   Average RS Score: {np.mean(rs_scores):.3f}")
    print(f"   Stocks with RS ≥ 90: {high_rs_count}")
    print(f"   Data Date: {yesterday}")
    
    print(f"\n✅ Daily update completed successfully!")

if __name__ == "__main__":
    main()
