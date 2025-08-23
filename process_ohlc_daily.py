#!/usr/bin/env python3
"""
Daily OHLC Data Updates - Runs Monday-Thursday at 4:30pm EST
Updates existing historical data with latest market data across split files
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
        
    def load_index_file(self) -> Dict[str, Any]:
        """Load the index file that describes all split files"""
        try:
            with open('ohlc_index.json', 'r') as f:
                index_data = json.load(f)
            logger.info(f"Loaded index file with {index_data.get('total_files', 0)} split files, "
                       f"{index_data.get('total_symbols', 0)} total symbols")
            return index_data
        except FileNotFoundError:
            logger.error("ohlc_index.json not found. Run full rebuild first.")
            raise
        except Exception as e:
            logger.error(f"Failed to load index file: {e}")
            raise
    
    def load_split_file(self, filename: str) -> Dict[str, Any]:
        """Load a specific split file"""
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load split file {filename}: {e}")
            raise
    
    def save_split_file(self, filename: str, data: Dict[str, Any]):
        """Save a specific split file"""
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, separators=(',', ':'))
        except Exception as e:
            logger.error(f"Failed to save split file {filename}: {e}")
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
                    't': bar['t'],
                    'o': round(bar['o'], 2),
                    'h': round(bar['h'], 2),
                    'l': round(bar['l'], 2),
                    'c': round(bar['c'], 2),
                    'v': int(bar['v']),
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
    
    def update_symbol_data(self, symbol: str, symbol_data: Dict, latest_bar: Dict, update_timestamp: int) -> bool:
        """Update a symbol's data with the latest bar"""
        updated = False
        
        # Update daily data
        if '1D' in symbol_data:
            daily_data = symbol_data['1D']
            
            # Check if we already have this timestamp
            existing_timestamps = {bar['t'] for bar in daily_data}
            
            if update_timestamp not in existing_timestamps:
                daily_data.append(latest_bar)
                # Keep only last 400 days, sorted by timestamp
                daily_data.sort(key=lambda x: x['t'])
                if len(daily_data) > 400:
                    symbol_data['1D'] = daily_data[-400:]
                else:
                    symbol_data['1D'] = daily_data
                updated = True
                logger.debug(f"Added new daily bar for {symbol} at timestamp {update_timestamp}")
            else:
                # Update existing bar if it exists (in case of corrections)
                for i, bar in enumerate(daily_data):
                    if bar['t'] == update_timestamp:
                        daily_data[i] = latest_bar
                        updated = True
                        logger.debug(f"Updated existing daily bar for {symbol} at timestamp {update_timestamp}")
                        break
        
        return updated
    
    def process_split_file(self, file_info: Dict, update_date: str) -> Dict[str, int]:
        """Process daily updates for one split file"""
        filename = file_info['filename']
        logger.info(f"Processing {filename} ({file_info['range_start']}-{file_info['range_end']}, "
                   f"{file_info['symbol_count']} symbols)")
        
        # Load the split file
        file_data = self.load_split_file(filename)
        
        updated_symbols = 0
        failed_symbols = 0
        
        for symbol in file_info['symbols']:
            if symbol in file_data['data']:
                # Fetch latest data for this symbol
                latest_bar = self.fetch_latest_daily_data(symbol, update_date)
                
                if latest_bar:
                    # Update the symbol's data
                    if self.update_symbol_data(symbol, file_data['data'][symbol], 
                                             latest_bar, latest_bar['t']):
                        updated_symbols += 1
                else:
                    failed_symbols += 1
            else:
                logger.warning(f"Symbol {symbol} not found in {filename}")
                failed_symbols += 1
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        # Update file metadata
        file_data['last_updated'] = datetime.now().isoformat()
        file_data['update_type'] = 'daily_update'
        
        # Save the updated file
        self.save_split_file(filename, file_data)
        
        logger.info(f"Completed {filename}: {updated_symbols} updated, {failed_symbols} failed")
        return {'updated': updated_symbols, 'failed': failed_symbols}
    
    def process_daily_updates(self, index_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process daily updates for all split files"""
        update_date = self.get_update_date()
        logger.info(f"Starting daily update for {index_data['total_symbols']} symbols "
                   f"across {index_data['total_files']} files on {update_date}")
        
        total_updated = 0
        total_failed = 0
        
        for i, file_info in enumerate(index_data['files']):
            logger.info(f"Processing file {i + 1}/{index_data['total_files']}")
            
            results = self.process_split_file(file_info, update_date)
            total_updated += results['updated']
            total_failed += results['failed']
        
        logger.info(f"Daily update completed: {total_updated} updated, {total_failed} failed")
        
        # Update index metadata
        index_data['last_updated'] = datetime.now().isoformat()
        index_data['update_type'] = 'daily_update'
        
        return index_data
    
    def save_updated_index(self, index_data: Dict[str, Any]):
        """Save updated index file"""
        with open('ohlc_index.json', 'w') as f:
            json.dump(index_data, f, separators=(',', ':'))
        
        logger.info("Saved updated ohlc_index.json")
        
        # Calculate and log file sizes
        total_size = 0
        for file_info in index_data['files']:
            if os.path.exists(file_info['filename']):
                file_size = os.path.getsize(file_info['filename'])
                file_info['file_size'] = file_size  # Update size in index
                total_size += file_size
        
        index_size = os.path.getsize('ohlc_index.json')
        
        logger.info(f"File summary after update:")
        logger.info(f"- Split files: {index_data['total_files']} files, {total_size / (1024 * 1024):.1f}MB total")
        logger.info(f"- Index file: {index_size / 1024:.1f}KB")
        
        # Log individual file sizes
        for file_info in index_data['files']:
            if 'file_size' in file_info:
                size_mb = file_info['file_size'] / (1024 * 1024)
                logger.info(f"  {file_info['filename']}: {size_mb:.1f}MB")

def main():
    """Main execution function"""
    logger.info("Starting OHLC daily update process (split files)")
    
    try:
        processor = OHLCDailyProcessor()
        index_data = processor.load_index_file()
        updated_index = processor.process_daily_updates(index_data)
        processor.save_updated_index(updated_index)
        logger.info("Daily OHLC update completed successfully")
        
    except Exception as e:
        logger.error(f"Daily update failed: {e}")
        raise

if __name__ == "__main__":
    main()
