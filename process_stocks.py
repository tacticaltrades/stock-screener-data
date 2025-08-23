import json
import random
from datetime import datetime

def generate_sample_stock_data():
    """Generate sample stock data that matches the real structure but is much smaller"""
    
    # Sample stock symbols (mix of real and fictional)
    sample_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'AMD', 'CRM',
        'ADBE', 'PYPL', 'INTC', 'CSCO', 'ORCL', 'IBM', 'QCOM', 'TXN', 'AVGO', 'MU',
        'AMAT', 'ADI', 'MRVL', 'LRCX', 'KLAC', 'SNPS', 'CDNS', 'FTNT', 'PANW', 'CRWD',
        'ZS', 'OKTA', 'DDOG', 'SNOW', 'PLTR', 'RBLX', 'U', 'TWLO', 'ZM', 'DOCU',
        'SQ', 'SHOP', 'ROKU', 'PINS', 'SNAP', 'UBER', 'LYFT', 'ABNB', 'COIN', 'HOOD'
    ]
    
    stock_data = []
    
    # Generate data for each symbol
    for i, symbol in enumerate(sample_symbols):
        # Create some variation in performance
        # Top performers get better scores
        base_score = 70 - (i * 1.4)  # Decreasing scores
        base_score += random.uniform(-10, 10)  # Add some randomness
        
        # Generate relative performance (some positive, some negative)
        rel_3m = random.uniform(-0.5, 1.5)  # -50% to +150%
        rel_12m = random.uniform(-0.3, 2.0)  # -30% to +200%
        
        # Stock returns (usually higher than relative performance)
        stock_3m = rel_3m + random.uniform(-0.1, 0.3)
        stock_12m = rel_12m + random.uniform(-0.1, 0.4)
        
        # Volume varies widely
        raw_volume = random.randint(50000, 50000000)
        
        # Format volume
        if raw_volume >= 1000000:
            avg_volume = f"{raw_volume/1000000:.1f}M"
        elif raw_volume >= 1000:
            avg_volume = f"{raw_volume/1000:.0f}k"
        else:
            avg_volume = str(raw_volume)
        
        # Format returns as percentages
        def format_return(val):
            return f"{val*100:.1f}%"
        
        stock_data.append({
            "symbol": symbol,
            "rs_rank": max(1, min(99, int(base_score))),  # Keep between 1-99
            "rs_score": round(base_score, 4),
            "avg_volume": avg_volume,
            "raw_volume": raw_volume,
            "relative_3m": format_return(rel_3m),
            "relative_12m": format_return(rel_12m),
            "stock_return_3m": format_return(stock_3m),
            "stock_return_12m": format_return(stock_12m)
        })
    
    # Sort by RS rank (highest first)
    stock_data.sort(key=lambda x: x['rs_rank'], reverse=True)
    
    # Reassign ranks to be sequential
    for i, stock in enumerate(stock_data):
        # Create realistic distribution of ranks
        percentile = int(((len(stock_data) - i) / len(stock_data)) * 99) + 1
        stock['rs_rank'] = min(percentile, 99)
    
    return stock_data

def create_sample_json():
    """Create the complete sample JSON structure"""
    
    stock_data = generate_sample_stock_data()
    
    output = {
        "last_updated": datetime.now().isoformat(),
        "formula_used": "RS = 2×(3m relative vs S&P500) + 6m + 9m + 12m relative performance",
        "total_stocks": len(stock_data),
        "benchmark": "S&P 500 (SPY)",
        "update_type": "sample_data",
        "data": stock_data
    }
    
    return output

def main():
    """Generate and save sample data"""
    print("Generating sample stock screening data...")
    
    sample_data = create_sample_json()
    
    # Save to file
    with open('sample_rankings.json', 'w') as f:
        json.dump(sample_data, f, indent=2)
    
    print(f"✅ Generated sample data with {len(sample_data['data'])} stocks")
    print("📄 Saved as 'sample_rankings.json'")
    
    # Show first few entries
    print(f"\n🏆 Top 10 Sample Stocks:")
    print("Rank | Symbol | RS | 3M Rel | 12M Rel | Volume")
    print("-" * 55)
    
    for i, stock in enumerate(sample_data['data'][:10]):
        print(f"{stock['rs_rank']:2d}   | {stock['symbol']:6s} | {stock['rs_rank']:2d} | {stock['relative_3m']:7s} | {stock['relative_12m']:8s} | {stock['avg_volume']:>8s}")
    
    # File size check
    import os
    file_size = os.path.getsize('sample_rankings.json')
    print(f"\n📊 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    print("This should be small enough to avoid Git LFS!")
    
    print(f"\n🔧 To use this data:")
    print("1. Upload 'sample_rankings.json' to your GitHub repo")
    print("2. Update your app to fetch from the new file")
    print("3. Or embed this data directly in your React component")

if __name__ == "__main__":
    main()
