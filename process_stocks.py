import requests
import json
import os
from datetime import datetime, timedelta

API_KEY = os.environ.get('POLYGON_API_KEY')
BASE_URL = "https://api.polygon.io"

def test_api_connection():
    """Test if API key works"""
    print(f"Testing API key: {API_KEY[:5]}..." if API_KEY else "No API key found!")
    
    if not API_KEY:
        print("ERROR: POLYGON_API_KEY environment variable not set")
        return False
    
    # Simple test call
    url = f"{BASE_URL}/v3/reference/tickers"
    params = {
        'market': 'stocks',
        'active': 'true',
        'limit': 10,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        print(f"API Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"API working! Got {len(data.get('results', []))} tickers")
            return True
        else:
            print(f"API Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"Connection error: {e}")
        return False

def create_sample_data():
    """Create sample JSON with just a few stocks"""
    sample_stocks = [
        {'symbol': 'AAPL', 'rs_rank': 95, 'avg_volume': '89.2M', 'raw_volume': 89200000},
        {'symbol': 'MSFT', 'rs_rank': 88, 'avg_volume': '42.1M', 'raw_volume': 42100000},
        {'symbol': 'GOOGL', 'rs_rank': 76, 'avg_volume': '28.5M', 'raw_volume': 28500000},
        {'symbol': 'AMZN', 'rs_rank': 82, 'avg_volume': '35.7M', 'raw_volume': 35700000},
        {'symbol': 'TSLA', 'rs_rank': 91, 'avg_volume': '91.3M', 'raw_volume': 91300000}
    ]
    
    output = {
        'last_updated': datetime.now().isoformat(),
        'total_stocks': len(sample_stocks),
        'data': sample_stocks
    }
    
    with open('rankings.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print("Created sample rankings.json file")

def main():
    print("=== Debugging Stock Data Processor ===")
    
    # Test 1: Check API connection
    print("\n1. Testing API connection...")
    if test_api_connection():
        print("✅ API connection successful")
    else:
        print("❌ API connection failed")
        print("Creating sample data instead...")
        create_sample_data()
        return
    
    # Test 2: Try to get a single stock's data
    print("\n2. Testing single stock data fetch...")
    try:
        ticker = "AAPL"
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
        params = {'apikey': API_KEY}
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Got {len(data.get('results', []))} days of data for {ticker}")
        else:
            print(f"❌ Failed to get {ticker} data: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error fetching single stock: {e}")
    
    # For now, create sample data
    print("\n3. Creating sample data file...")
    create_sample_data()
    print("✅ Debug complete - check rankings.json file")

if __name__ == "__main__":
    main()
