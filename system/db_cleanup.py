#!/usr/bin/env python3
"""
Database Cleanup Script - Delete trading data for specific day

Usage:
  python db_cleanup.py YYYY-MM-DD
"""

import os
from dotenv import load_dotenv

load_dotenv()
import pandas as pd
import sys
from datetime import datetime
from sqlalchemy import text
from db import setup

# Table configurations with their timestamp columns
TABLE_CONFIG = {
    "market_data": ["last_traded_time", "exch_feed_time"],
    "stock_signals": ["time"],
    "trade_log": ["entry_time", "exit_time"],
    "active_trades": ["entry_time"],
    "capital_log": ["log_date"],
}

engine, session = setup()


def delete_day_data(target_date):
    """Delete data for specific date across all tables using SQLAlchemy"""
    total_deleted = 0

    # Convert target_date to ISO format if it's a string
    if isinstance(target_date, str):
        try:
            # Try to parse as DD-MM-YYYY
            parsed_date = datetime.strptime(target_date, "%d-%m-%Y").date()
            target_date = parsed_date.isoformat()  # Convert to YYYY-MM-DD
        except ValueError:
            # If that fails, try to parse as YYYY-MM-DD
            try:
                datetime.strptime(target_date, "%Y-%m-%d")
                # Already in correct format, no need to change
            except ValueError:
                print(
                    f"Invalid date format: {target_date}. Use YYYY-MM-DD or DD-MM-YYYY"
                )
                return

    print(f"Deleting data for date: {target_date}")

    try:
        # Use the engine to create a connection
        with engine.connect() as conn:
            # Create a transaction
            with conn.begin():
                # Delete from market_data table
                query = text("""
                    DELETE FROM market_data
                    WHERE DATE(to_timestamp(last_traded_time)) = :target_date
                """)
                result = conn.execute(query, {"target_date": target_date})
                deleted = result.rowcount
                total_deleted += deleted
                print(f"Deleted {deleted} rows from market_data")

                # Delete from stock_signals table
                query = text("""
                    DELETE FROM stock_signals
                    WHERE SUBSTRING(time, 1, 10) = :target_date
                """)
                result = conn.execute(query, {"target_date": target_date})
                deleted = result.rowcount
                total_deleted += deleted
                print(f"Deleted {deleted} rows from stock_signals")

                # Delete from trade_log table
                query = text("""
                    DELETE FROM trade_log
                    WHERE trade_date = :target_date
                """)
                result = conn.execute(query, {"target_date": target_date})
                deleted = result.rowcount
                total_deleted += deleted
                print(f"Deleted {deleted} rows from trade_log")

                # Delete from active_trades table
                query = text("""
                    DELETE FROM active_trades
                    WHERE DATE(entry_time) = :target_date
                """)
                result = conn.execute(query, {"target_date": target_date})
                deleted = result.rowcount
                total_deleted += deleted
                print(f"Deleted {deleted} rows from active_trades")

                # Delete from capital_log table
                query = text("""
                    DELETE FROM capital_log
                    WHERE log_date = :target_date
                """)
                result = conn.execute(query, {"target_date": target_date})
                deleted = result.rowcount
                total_deleted += deleted
                print(f"Deleted {deleted} rows from capital_log")

                # Transaction will be automatically committed if no exceptions occur

        if total_deleted > 0:
            print(f"\nTotal rows deleted: {total_deleted}")
        else:
            print(f"No data found for date: {target_date}")

    except Exception as e:
        print(f"Error deleting data: {e}")
        raise e


def main():
    if len(sys.argv) != 2:
        print("Usage: python db_cleanup.py YYYY-MM-DD")
        return

    try:
        target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD")
        return

    print(
        f"\nWARNING: This will permanently delete all trading data for {target_date}!"
    )
    confirmation = input("Type 'DELETE' to confirm: ")

    if confirmation.strip().upper() != "DELETE":
        print("Operation cancelled")
        return

    try:
        delete_day_data(target_date)
    except Exception as e:
        print(f"Error: {e}")
