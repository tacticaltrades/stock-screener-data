#!/usr/bin/env python3
"""
Full OHLC Data Rebuild - Runs Fridays at 4:30pm EST
Fetches complete historical data for all stocks and creates base files
"""

import json
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OHLCFullProcessor:
    def __init__(self):
        self.api_key = os.environ.get('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable is required")
        
        self.base_url = "https://api.polygon.io/v2"
        self.symbols = self.load_symbols()
        self.rate_limit_delay = 0.1  # 10 requests per second for paid plans
        
    def load_symbols(self) -> List[str]:
        """Load symbols from rankings.json"""
        try:
            with open('rankings.json', 'r') as f:
                rankings_data = json.load(f)
            symbols = [item['symbol'] for item in rankings_data['data']]
            logger.info(f"Loaded {len(symbols)} symbols from rankings.json")
            return ["AAPL"]
        except Exception as e:
            logger.error(f"Failed to load symbols: {e}")
            return []
    
    def get_date_ranges(self) -> Dict[str, Dict]:
        """Calculate date ranges for different timeframes"""
        today = datetime.now()
        
        return {
            "1D": {
                "from": (today - timedelta(days=400)).strftime("%Y-%m-%d"),  # ~1.5 years of daily data
                "to": today.strftime("%Y-%m-%d"),
                "multiplier": 1,
                "timespan": "day"
            },
            "1W": {
                "from": (today - timedelta(weeks=104)).strftime("%Y-%m-%d"),  # 2 years of weekly data
                "to": today.strftime("%Y-%m-%d"), 
                "multiplier": 1,
                "timespan": "week"
            },
        }
    
    def fetch_ohlc_data(self, symbol: str, timeframe: str, date_range: Dict) -> List[Dict]:
        """Fetch OHLC data for a symbol and timeframe"""
        url = f"{self.base_url}/aggs/ticker/{symbol}/range/{date_range['multiplier']}/{date_range['timespan']}/{date_range['from']}/{date_range['to']}"
        
        # Debug the first few requests
        if symbol in ['AAPL', 'MSFT', 'GOOGL']:  # Test with known good symbols
            logger.info(f"DEBUG: Fetching {symbol} {timeframe}")
            logger.info(f"DEBUG: URL = {url}")
            logger.info(f"DEBUG: Date range = {date_range}")
        
        params = {
            'apikey': self.api_key,
            'adjusted': 'true',
            'sort': 'asc'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Debug the response for test symbols
            if symbol in ['AAPL', 'MSFT', 'GOOGL']:
                logger.info(f"DEBUG: {symbol} response status = {data.get('status')}")
                logger.info(f"DEBUG: {symbol} has results = {'results' in data}")
                if 'results' in data:
                    logger.info(f"DEBUG: {symbol} results count = {len(data['results'])}")
            
            if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                # Transform to consistent format
                ohlc_data = []
                for bar in data['results']:
                    ohlc_data.append({
                        'timestamp': bar['t'],
                        'open': bar['o'],
                        'high': bar['h'],
                        'low': bar['l'],
                        'close': bar['c'],
                        'volume': bar['v'],
                        'date': datetime.fromtimestamp(bar['t'] / 1000).strftime('%Y-%m-%d')
                    })
                
                logger.info(f"Fetched {len(ohlc_data)} bars for {symbol} {timeframe}")
                return ohlc_data
            else:
                logger.warning(f"No data for {symbol} {timeframe}: {data.get('message', 'Unknown error')}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol} {timeframe}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error processing {symbol} {timeframe}: {e}")
            return []
    
    def process_all_symbols(self) -> Dict[str, Any]:
        """Process all symbols for all timeframes"""
        date_ranges = self.get_date_ranges()
        all_data = {}
        
        total_requests = len(self.symbols) * len(date_ranges)
        completed_requests = 0
        
        logger.info(f"Starting full rebuild for {len(self.symbols)} symbols across {len(date_ranges)} timeframes")
        logger.info(f"Total API requests needed: {total_requests}")
        
        for symbol in self.symbols:
            symbol_data = {}
            
            for timeframe, date_range in date_ranges.items():
                ohlc_data = self.fetch_ohlc_data(symbol, timeframe, date_range)
                symbol_data[timeframe] = ohlc_data
                
                completed_requests += 1
                if completed_requests % 100 == 0:
                    logger.info(f"Progress: {completed_requests}/{total_requests} requests completed ({completed_requests/total_requests*100:.1f}%)")
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
            
            all_data[symbol] = symbol_data
        
        logger.info(f"Completed full rebuild: {completed_requests} requests processed")
        return all_data
    
    def save_data(self, data: Dict[str, Any]):
        """Save data to single JSON file"""
        timestamp = datetime.now().isoformat()
        
        # Create single ohlc.json file
        ohlc_data = {
            "last_updated": timestamp,
            "update_type": "full_rebuild",
            "total_symbols": len(data),
            "timeframes": ["1D", "1W"],
            "data": data
        }
        
        # Save to single file
        with open('ohlc.json', 'w') as f:
            json.dump(ohlc_data, f, separators=(',', ':'))
        
        logger.info("Saved ohlc.json")
        
        # Log file size
        ohlc_size = os.path.getsize('ohlc.json') / (1024 * 1024)  # MB
        logger.info(f"File size: ohlc.json={ohlc_size:.1f}MB")

def main():
    """Main execution function"""
    logger.info("Starting OHLC full rebuild process")
    
    try:
        processor = OHLCFullProcessor()
        all_data = processor.process_all_symbols()
        processor.save_data(all_data)
        logger.info("Full OHLC rebuild completed successfully")
        
    except Exception as e:
        logger.error(f"Full rebuild failed: {e}")
        raise

if __name__ == "__main__":
    main()
