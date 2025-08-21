#!/usr/bin/env python3
"""
Daily OHLC Data Updates - Runs Monday-Thursday at 4:30pm EST
Updates existing historical data with latest market data
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

class OHLCDailyProcessor:
    def __init__(self):
        self.api_key = os.environ.get('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable is required")
        
        self.base_url = "https://api.polygon.io/v2"
        self.rate_limit_delay = 0.1  # 10 requests per second
        
    def load_existing_data(self) -> Dict[str, Any]:
        """Load existing OHLC data"""
        try:
            with open('ohlc.json', 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded existing data for {data.get('total_symbols', 0)} symbols")
            return data
        except FileNotFoundError:
            logger.error("ohlc.json not found. Run full rebuild first.")
            raise
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            raise
    
    def get_update_date(self) -> str:
        """Get the date for today's update"""
        # For after-hours updates, use today's date
        return datetime.now().strftime("%Y-%m-%d")
    
    def fetch_latest_daily_data(self, symbol: str, date: str) -> Dict:
        """Fetch latest daily bar for a symbol"""
        # Get data for the specific date
        url = f"{self.base_url}/aggs/ticker/{symbol}/range/1/day/{date}/{date}"
        
        params = {
            'apikey': self.api_key,
            'adjusted': 'true'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') == 'OK' and 'results' in data and len(data['results']) > 0:
                bar = data['results'][0]
                return {
                    'timestamp': bar['t'],
                    'open': bar['o'],
                    'high': bar['h'],
                    'low': bar['l'],
                    'close': bar['c'],
                    'volume': bar['v'],
                    'date': datetime.fromtimestamp(bar['t'] / 1000).strftime('%Y-%m-%d')
                }
            else:
                logger.debug(f"No new data for {symbol} on {date}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed for {symbol}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error processing {symbol}: {e}")
            return None
    
    def update_symbol_data(self, symbol: str, symbol_data: Dict, latest_bar: Dict) -> bool:
        """Update a symbol's data with the latest bar"""
        updated = False
        update_date = latest_bar['date']
        
        # Update daily data
        if '1D' in symbol_data:
            daily_data = symbol_data['1D']
            
            # Check if we already have this date
            existing_dates = {bar['date'] for bar in daily_data}
            
            if update_date not in existing_dates:
                daily_data.append(latest_bar)
                # Keep only last 400 days
                daily_data.sort(key=lambda x: x['timestamp'])
                if len(daily_data) > 400:
                    symbol_data['1D'] = daily_data[-400:]
                updated = True
                logger.debug(f"Added new daily bar for {symbol} on {update_date}")
            else:
                # Update existing bar if it exists (in case of corrections)
                for i, bar in enumerate(daily_data):
                    if bar['date'] == update_date:
                        daily_data[i] = latest_bar
                        updated = True
                        logger.debug(f"Updated existing daily bar for {symbol} on {update_date}")
                        break
        
        return updated
    
    def process_daily_updates(self, existing_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process daily updates for all symbols"""
        update_date = self.get_update_date()
        symbols = list(existing_data['data'].keys())
        
        logger.info(f"Starting daily update for {len(symbols)} symbols on {update_date}")
        
        updated_symbols = 0
        failed_symbols = 0
        
        for i, symbol in enumerate(symbols):
            # Fetch latest data
            latest_bar = self.fetch_latest_daily_data(symbol, update_date)
            
            if latest_bar:
                # Update the symbol's data
                if self.update_symbol_data(symbol, existing_data['data'][symbol], latest_bar):
                    updated_symbols += 1
            else:
                failed_symbols += 1
            
            # Progress logging
            if (i + 1) % 100 == 0:
                logger.info(f"Progress: {i + 1}/{len(symbols)} symbols processed ({(i + 1)/len(symbols)*100:.1f}%)")
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        logger.info(f"Daily update completed: {updated_symbols} updated, {failed_symbols} failed")
        
        # Update metadata
        existing_data['last_updated'] = datetime.now().isoformat()
        existing_data['update_type'] = 'daily_update'
        
        return existing_data
    
    def save_updated_data(self, data: Dict[str, Any]):
        """Save updated data to single JSON file"""
        # Save to single file
        with open('ohlc.json', 'w') as f:
            json.dump(data, f, separators=(',', ':'))
        
        logger.info("Saved updated ohlc.json")
        
        # Log file size
        ohlc_size = os.path.getsize('ohlc.json') / (1024 * 1024)  # MB
        logger.info(f"File size: ohlc.json={ohlc_size:.1f}MB")
        
        # Log file sizes
        ohlc_size = os.path.getsize('ohlc.json') / (1024 * 1024)  # MB
        hist_size = os.path.getsize('ohlc.json') / (1024 * 1024)  # MB
        logger.info(f"File sizes: ohlc.json={ohlc_size:.1f}MB, ohlc.json={hist_size:.1f}MB")

def main():
    """Main execution function"""
    logger.info("Starting OHLC daily update process")
    
    try:
        processor = OHLCDailyProcessor()
        existing_data = processor.load_existing_data()
        updated_data = processor.process_daily_updates(existing_data)
        processor.save_updated_data(updated_data)
        logger.info("Daily OHLC update completed successfully")
        
    except Exception as e:
        logger.error(f"Daily update failed: {e}")
        raise

if __name__ == "__main__":
    main()
