#!/usr/bin/env python3
"""
Full OHLC Data Rebuild - Runs Fridays at 4:30pm EST
Fetches complete historical data for all stocks and creates split files
"""

import json
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import math

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
        
        # Split configuration
        self.target_symbols_per_file = 50  # Adjust based on your data size needs
        
    def load_symbols(self) -> List[str]:
        """Load symbols from rankings.json"""
        try:
            with open('rankings.json', 'r') as f:
                rankings_data = json.load(f)
            symbols = [item['symbol'] for item in rankings_data['data']]
            logger.info(f"Loaded {len(symbols)} symbols from rankings.json")
            return symbols
        except Exception as e:
            logger.error(f"Failed to load symbols: {e}")
            return []
    
    def get_split_ranges(self) -> List[Dict]:
        """Calculate how to split symbols into files"""
        if not self.symbols:
            return []
        
        # Sort symbols alphabetically
        sorted_symbols = sorted(self.symbols)
        total_symbols = len(sorted_symbols)
        num_files = math.ceil(total_symbols / self.target_symbols_per_file)
        
        splits = []
        for i in range(num_files):
            start_idx = i * self.target_symbols_per_file
            end_idx = min(start_idx + self.target_symbols_per_file, total_symbols)
            
            symbols_chunk = sorted_symbols[start_idx:end_idx]
            splits.append({
                'file_index': i + 1,
                'filename': f'ohlc_{i + 1:03d}.json',
                'range_start': symbols_chunk[0],
                'range_end': symbols_chunk[-1],
                'symbols': symbols_chunk,
                'symbol_count': len(symbols_chunk)
            })
        
        logger.info(f"Will create {len(splits)} split files with ~{self.target_symbols_per_file} symbols each")
        return splits
    
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
                        't': bar['t'],
                        'o': round(bar['o'], 2),
                        'h': round(bar['h'], 2),
                        'l': round(bar['l'], 2),
                        'c': round(bar['c'], 2),
                        'v': int(bar['v']),
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
    
    def process_symbol_chunk(self, symbols_chunk: List[str]) -> Dict[str, Any]:
        """Process a chunk of symbols for all timeframes"""
        date_ranges = self.get_date_ranges()
        chunk_data = {}
        
        for symbol in symbols_chunk:
            symbol_data = {}
            
            for timeframe, date_range in date_ranges.items():
                ohlc_data = self.fetch_ohlc_data(symbol, timeframe, date_range)
                symbol_data[timeframe] = ohlc_data
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
            
            chunk_data[symbol] = symbol_data
        
        return chunk_data
    
    def process_all_symbols(self) -> Dict[str, Any]:
        """Process all symbols and create split files"""
        splits = self.get_split_ranges()
        timestamp = datetime.now().isoformat()
        
        total_requests = len(self.symbols) * len(self.get_date_ranges())
        completed_requests = 0
        
        logger.info(f"Starting full rebuild for {len(self.symbols)} symbols across {len(self.get_date_ranges())} timeframes")
        logger.info(f"Total API requests needed: {total_requests}")
        logger.info(f"Creating {len(splits)} split files")
        
        # Clean up old split files
        self.cleanup_old_files()
        
        file_info = []
        
        for split_info in splits:
            logger.info(f"Processing file {split_info['file_index']}/{len(splits)}: {split_info['filename']} "
                       f"({split_info['range_start']}-{split_info['range_end']}, {split_info['symbol_count']} symbols)")
            
            # Process this chunk of symbols
            chunk_data = self.process_symbol_chunk(split_info['symbols'])
            
            # Save this chunk to file
            file_data = {
                "last_updated": timestamp,
                "update_type": "full_rebuild",
                "file_index": split_info['file_index'],
                "range_start": split_info['range_start'],
                "range_end": split_info['range_end'],
                "symbol_count": split_info['symbol_count'],
                "timeframes": ["1D", "1W"],
                "data": chunk_data
            }
            
            with open(split_info['filename'], 'w') as f:
                json.dump(file_data, f, separators=(',', ':'))
            
            # Track file info for index
            file_size = os.path.getsize(split_info['filename'])
            file_info.append({
                "filename": split_info['filename'],
                "range_start": split_info['range_start'],
                "range_end": split_info['range_end'],
                "symbol_count": split_info['symbol_count'],
                "file_size": file_size,
                "symbols": split_info['symbols']
            })
            
            completed_requests += split_info['symbol_count'] * len(self.get_date_ranges())
            logger.info(f"Completed {split_info['filename']} - Progress: {completed_requests}/{total_requests} "
                       f"requests ({completed_requests/total_requests*100:.1f}%)")
        
        logger.info(f"Completed full rebuild: {completed_requests} requests processed")
        return {
            "timestamp": timestamp,
            "total_symbols": len(self.symbols),
            "files": file_info
        }
    
    def cleanup_old_files(self):
        """Remove old split files and single ohlc.json"""
        # Remove old split files
        for filename in os.listdir('.'):
            if filename.startswith('ohlc_') and filename.endswith('.json'):
                os.remove(filename)
                logger.info(f"Removed old file: {filename}")
        
        # Remove old single file
        if os.path.exists('ohlc.json'):
            os.remove('ohlc.json')
            logger.info("Removed old ohlc.json")
    
    def save_index(self, summary_data: Dict[str, Any]):
        """Save index file with metadata about all split files"""
        index_data = {
            "last_updated": summary_data["timestamp"],
            "update_type": "full_rebuild",
            "total_symbols": summary_data["total_symbols"],
            "total_files": len(summary_data["files"]),
            "timeframes": ["1D", "1W"],
            "files": summary_data["files"]
        }
        
        with open('ohlc_index.json', 'w') as f:
            json.dump(index_data, f, separators=(',', ':'))
        
        logger.info("Saved ohlc_index.json")
        
        # Log file sizes
        total_size = sum(file_info['file_size'] for file_info in summary_data["files"])
        index_size = os.path.getsize('ohlc_index.json')
        
        logger.info(f"File summary:")
        logger.info(f"- Split files: {len(summary_data['files'])} files, {total_size / (1024 * 1024):.1f}MB total")
        logger.info(f"- Index file: {index_size / 1024:.1f}KB")
        
        for file_info in summary_data["files"]:
            size_mb = file_info['file_size'] / (1024 * 1024)
            logger.info(f"  {file_info['filename']}: {size_mb:.1f}MB "
                       f"({file_info['range_start']}-{file_info['range_end']}, {file_info['symbol_count']} symbols)")

def main():
    """Main execution function"""
    logger.info("Starting OHLC full rebuild process (split files)")
    
    try:
        processor = OHLCFullProcessor()
        summary_data = processor.process_all_symbols()
        processor.save_index(summary_data)
        logger.info("Full OHLC rebuild completed successfully")
        
    except Exception as e:
        logger.error(f"Full rebuild failed: {e}")
        raise

if __name__ == "__main__":
    main()
