"""
Data Provider module for the backtester.

This module contains the DataProvider interface and implementations for different data sources.
"""

import os
import json
import hashlib
import pandas as pd
import pytz
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Union

from rich.console import Console
from fyers_utils import FyersBroker

# Define timezone
IST = pytz.timezone('Asia/Kolkata')
console = Console()

class DataProvider(ABC):
    """Abstract interface for data providers."""
    
    @abstractmethod
    def get_historical_data(self, ticker: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get historical data for a ticker between start and end dates.
        
        Args:
            ticker: Stock ticker symbol
            resolution: Time resolution (e.g., '1', '5', '15' for minutes)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Historical data as a DataFrame
        """
        pass
    
    @abstractmethod
    def structure_data(self, ticker: str, data: Any) -> pd.DataFrame:
        """
        Structure data into a standard format for backtesting.
        
        Args:
            ticker: Stock ticker symbol
            data: Raw data from the data source
            
        Returns:
            pd.DataFrame: Structured DataFrame with required fields
        """
        pass


class FyersDataProvider(DataProvider):
    """Data provider implementation for Fyers API."""
    
    def __init__(self, cache_dir: str = ".cache/data", use_cache: bool = True):
        """
        Initialize Fyers data provider.
        
        Args:
            cache_dir: Directory to store cached data
            use_cache: Whether to use cached data
        """
        self.fyers_broker = FyersBroker()
        self.cache_dir = Path(cache_dir)
        self.use_cache = use_cache
        
        # Create cache directory if it doesn't exist
        if self.use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_historical_data(self, ticker: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get historical data for a ticker between start and end dates.
        
        Args:
            ticker: Stock ticker symbol
            resolution: Time resolution (e.g., '1', '5', '15' for minutes)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Historical data as a DataFrame
        """
        # Check cache first if enabled
        if self.use_cache:
            cached_data = self._get_from_cache(ticker, resolution, start_date, end_date)
            if cached_data is not None:
                return cached_data
        
        # Define retry parameters
        max_attempts = 3
        attempts = 0
        
        # Fetch data with retries
        while attempts < max_attempts:
            try:
                # console.print(f"[cyan]Fetching data for {ticker} from {start_date} to {end_date}[/cyan]")
                data = self.fyers_broker.get_history(
                    ticker,
                    resolution,
                    start_date,
                    end_date
                )
                # Validate response
                if not isinstance(data, dict):
                    console.print(f"[yellow]Warning: Unexpected response type for {ticker}: {type(data)}[/yellow]")
                    attempts += 1
                    continue
                    
                if 'code' in data.keys():
                    console.print(f"[yellow]API Error: {data.get('message', 'Unknown error')}[/yellow]")
                    attempts += 1
                    continue
                    
                if not data.get('candles', []):
                    # console.print(f"[yellow]No candles returned for {ticker} from {start_date} to {end_date}[/yellow]")
                    return pd.DataFrame()  # Return empty DataFrame
                
                # Structure data
                df = self.structure_data(ticker, data)
                # Cache data if enabled
                if self.use_cache:
                    self._save_to_cache(ticker, resolution, start_date, end_date, df)
                
                return df
                
            except Exception as e:
                console.print(f"[red]Error fetching data for {ticker}: {str(e)}[/red]")
                attempts += 1
        
        # If all attempts fail, return empty DataFrame
        console.print(f"[red]Failed to fetch data for {ticker} after {max_attempts} attempts[/red]")
        return pd.DataFrame()
    
    def structure_data(self, ticker: str, data: Any) -> pd.DataFrame:
        """
        Structure data into a standard format for backtesting.
        
        Args:
            ticker: Stock ticker symbol
            data: Raw data from Fyers API
            
        Returns:
            pd.DataFrame: Structured DataFrame with required fields
        """
        # Check if data is already a DataFrame
        if isinstance(data, pd.DataFrame):
            return data
            
        # Handle empty or invalid responses
        if not isinstance(data, dict) or 'candles' not in data or not data['candles']:
            console.print(f"[yellow]Warning: Empty or invalid data structure for {ticker}[/yellow]")
            return pd.DataFrame()  # Return empty DataFrame
            
        # Create DataFrame from fetched data
        df = pd.DataFrame(data['candles'], columns=['t', 'o', 'h', 'l', 'c', 'v'])
        
        # Convert timestamp to datetime with proper timezone
        df['datetime'] = pd.to_datetime(df['t'], unit='s').dt.tz_localize(pytz.UTC).dt.tz_convert(IST)
        
        # Calculate cumulative volume
        df['cum_vol'] = df['v'].cumsum()
        # Calculate previous close price using shift
        df['prev_close'] = df['c'].shift(1).fillna(df['o'].iloc[0])

        # Calculate change and change percentage
        df['ch'] = df['c'] - df['prev_close']
        df['chp'] = (df['ch'] / df['prev_close'] * 100).fillna(0)

        # Calculate average trade price
        df['avg_trade_price'] = (df['o'] + df['h'] + df['l'] + df['c']) / 4

        # Prepare the final DataFrame with required metrics
        metrics_df = pd.DataFrame({
            'datetime': df['datetime'],  # Keep the datetime column as is
            'symbol': ticker,
            'ltp': df['c'].round(2),
            'vol_traded_today': df['cum_vol'].astype(int),
            'last_traded_time': df['t'].astype(int),
            'exch_feed_time': df['t'].astype(int),
            'bid_size': 0,
            'ask_size': 0,
            'bid_price': (df['c'] - 0.05).round(2),
            'ask_price': (df['c'] + 0.05).round(2),
            'last_traded_qty': df['v'].astype(int),
            'tot_buy_qty': (df['cum_vol'] // 2).astype(int),
            'tot_sell_qty': (df['cum_vol'] - (df['cum_vol'] // 2)).astype(int),
            'avg_trade_price': df['avg_trade_price'].round(2),
            'low_price': df['l'].round(2),
            'high_price': df['h'].round(2),
            'open_price': df['o'].round(2),
            'prev_close_price': df['prev_close'].round(2),
            'type': "historical",
            'ch': df['ch'].round(2),
            'chp': df['chp'].round(2)
        })
        
        return metrics_df
    
    def _get_cache_path(self, ticker: str, resolution: str, start_date: str, end_date: str) -> Path:
        """Generate a unique cache file path based on query parameters."""
        # Create a unique key based on parameters
        key = f"{ticker}_{resolution}_{start_date}_{end_date}"
        filename = hashlib.md5(key.encode()).hexdigest() + ".csv"
        return self.cache_dir / filename
    
    def _get_from_cache(self, ticker: str, resolution: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Try to get data from cache."""
        if not self.use_cache:
            return None
            
        cache_path = self._get_cache_path(ticker, resolution, start_date, end_date)
        
        if not cache_path.exists():
            return None
            
        try:
            # Read cache file
            # with open(cache_path, 'r') as f:
            #     cache_data = json.load(f)
                
            # # Check if cache is valid (has metadata and actual data)
            # if not isinstance(cache_data, dict) or 'metadata' not in cache_data or 'data' not in cache_data:
            #     console.print(f"[yellow]Invalid cache format for {ticker}[/yellow]")
            #     return None
                
            # # Check if cache is expired
            # cache_timestamp = cache_data['metadata'].get('timestamp', 0)
            # cache_age = datetime.now().timestamp() - cache_timestamp
            
            # # Cache expires after 7 days for historical data
            # if cache_age > 7 * 24 * 60 * 60:
            #     console.print(f"[yellow]Cache expired for {ticker}[/yellow]")
            #     return None
            df = pd.read_csv(cache_path)
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Return structured data
            # console.print(f"[green]Using cached data for {ticker} from {start_date} to {end_date}[/green]")
            return df
            
        except Exception as e:
            console.print(f"[yellow]Error reading cache for {ticker}: {str(e)}[/yellow]")
            return None
    
    def _save_to_cache(self, ticker: str, resolution: str, start_date: str, end_date: str, data: Dict[str, Any]) -> None:
        """Save data to cache."""
        if not self.use_cache:
            return
            
        cache_path = self._get_cache_path(ticker, resolution, start_date, end_date)
        
        try:
            # Prepare cache data with metadata
            # cache_data = {
            #     'metadata': {
            #         'ticker': ticker,
            #         'resolution': resolution,
            #         'start_date': start_date,
            #         'end_date': end_date,
            #         'timestamp': datetime.now().timestamp()
            #     },
            #     'data': data
            # }
            
            # # Write to cache file
            # with open(cache_path, 'w') as f:
            #     json.dump(cache_data, f)
            data.to_csv(cache_path, index=False)
            # console.print(f"[green]Cached data for {ticker} from {start_date} to {end_date}[/green]")
            
        except Exception as e:
            console.print(f"[yellow]Error caching data for {ticker}: {str(e)}[/yellow]")


class CsvDataProvider(DataProvider):
    """Data provider implementation for CSV files."""
    
    def __init__(self, data_dir: str):
        """
        Initialize CSV data provider.
        
        Args:
            data_dir: Directory containing CSV files
        """
        self.data_dir = Path(data_dir)
        
    def get_historical_data(self, ticker: str, resolution: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get historical data for a ticker between start and end dates.
        
        Args:
            ticker: Stock ticker symbol
            resolution: Time resolution (e.g., '1', '5', '15' for minutes)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: Historical data as a DataFrame
        """
        # Find the CSV file for the ticker
        csv_path = self.data_dir / f"{ticker}.csv"
        
        if not csv_path.exists():
            console.print(f"[yellow]CSV file not found for {ticker}[/yellow]")
            return pd.DataFrame()
            
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # Convert date column to datetime
            date_column = next((col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()), None)
            
            if date_column is None:
                console.print(f"[yellow]No date/time column found in CSV for {ticker}[/yellow]")
                return pd.DataFrame()
                
            df['datetime'] = pd.to_datetime(df[date_column])
            
            # Filter by date range
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df['datetime'] >= start_dt) & (df['datetime'] <= end_dt)]
            
            # Structure data
            return self.structure_data(ticker, df)
            
        except Exception as e:
            console.print(f"[red]Error reading CSV for {ticker}: {str(e)}[/red]")
            return pd.DataFrame()
    
    def structure_data(self, ticker: str, data: Union[pd.DataFrame, Any]) -> pd.DataFrame:
        """
        Structure data into a standard format for backtesting.
        
        Args:
            ticker: Stock ticker symbol
            data: CSV data as a DataFrame
            
        Returns:
            pd.DataFrame: Structured DataFrame with required fields
        """
        if not isinstance(data, pd.DataFrame) or data.empty:
            return pd.DataFrame()
            
        # Map CSV columns to required format
        # This is a simplified example - adjust to match your CSV format
        required_columns = {
            'datetime': 'datetime',
            'open': 'o',
            'high': 'h',
            'low': 'l',
            'close': 'c',
            'volume': 'v'
        }
        
        # Create a mapping between CSV columns and required columns
        column_mapping = {}
        for req_col, alias in required_columns.items():
            found = False
            for csv_col in data.columns:
                if alias == csv_col or req_col.lower() in csv_col.lower():
                    column_mapping[req_col] = csv_col
                    found = True
                    break
            
            if not found and req_col != 'datetime':  # datetime already handled
                console.print(f"[yellow]Column {req_col} not found in CSV for {ticker}[/yellow]")
                # Use sensible defaults
                if req_col in ['open', 'high', 'low', 'close'] and 'close' in column_mapping:
                    column_mapping[req_col] = column_mapping['close']
                elif req_col == 'volume':
                    # Set volume to 0 if not available
                    data['volume'] = 0
                    column_mapping[req_col] = 'volume'
        
        # Create a copy with only the required columns
        metrics_df = pd.DataFrame()
        metrics_df['datetime'] = data['datetime']
        metrics_df['symbol'] = ticker
        
        # Map the rest of the columns
        metrics_df['ltp'] = data[column_mapping.get('close')].round(2)
        metrics_df['open_price'] = data[column_mapping.get('open')].round(2)
        metrics_df['high_price'] = data[column_mapping.get('high')].round(2)
        metrics_df['low_price'] = data[column_mapping.get('low')].round(2)
        
        # Generate other required fields
        metrics_df['vol_traded_today'] = data[column_mapping.get('volume')].cumsum().astype(int)
        metrics_df['last_traded_qty'] = data[column_mapping.get('volume')].astype(int)
        metrics_df['tot_buy_qty'] = (metrics_df['vol_traded_today'] // 2).astype(int)
        metrics_df['tot_sell_qty'] = (metrics_df['vol_traded_today'] - metrics_df['tot_buy_qty']).astype(int)
        metrics_df['prev_close_price'] = metrics_df['ltp'].shift(1).fillna(metrics_df['open_price']).round(2)
        metrics_df['ch'] = (metrics_df['ltp'] - metrics_df['prev_close_price']).round(2)
        metrics_df['chp'] = (metrics_df['ch'] / metrics_df['prev_close_price'] * 100).fillna(0).round(2)
        metrics_df['avg_trade_price'] = ((metrics_df['open_price'] + metrics_df['high_price'] + 
                                       metrics_df['low_price'] + metrics_df['ltp']) / 4).round(2)
        
        # Add broker-specific fields
        metrics_df['bid_size'] = 0
        metrics_df['ask_size'] = 0
        metrics_df['bid_price'] = (metrics_df['ltp'] - 0.05).round(2)
        metrics_df['ask_price'] = (metrics_df['ltp'] + 0.05).round(2)
        metrics_df['type'] = "historical"
        
        # Convert datetime to timestamp for compatibility
        metrics_df['last_traded_time'] = metrics_df['datetime'].apply(lambda x: int(x.timestamp()))
        metrics_df['exch_feed_time'] = metrics_df['last_traded_time']
        
        return metrics_df


# Factory function to create data providers
def get_data_provider(provider_type: str, **kwargs) -> DataProvider:
    """
    Factory function to create data providers.
    
    Args:
        provider_type: Type of data provider ('fyers' or 'csv')
        **kwargs: Additional arguments for the data provider
        
    Returns:
        DataProvider: Instance of a data provider
    """
    if provider_type.lower() == 'fyers':
        return FyersDataProvider(**kwargs)
    elif provider_type.lower() == 'csv':
        return CsvDataProvider(**kwargs)
    else:
        raise ValueError(f"Unknown data provider type: {provider_type}") 