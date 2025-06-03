from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    BigInteger,
    Text,
    TIMESTAMP,
    Boolean,
    and_,
    func,
    text,
    Table,
    MetaData,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()


import logging
from logger import logger

logger.setLevel(logging.WARNING)

# Base class for all ORM models
Base = declarative_base()


# ------------------------------------------------------------------------------
# CapitalLog Table
# ------------------------------------------------------------------------------
class CapitalLog(Base):
    """
    Records daily capital details.

    Attributes:
        log_date (Date): The date of the log entry (primary key).
        starting_capital (Float): Capital at the beginning of the day.
        ending_capital (Float): Capital at the end of the day.
        absolute_gain (Float): Absolute gain or loss during the day.
        percent_gain (Float): Percentage gain or loss.
    """

    __tablename__ = "capital_log"

    log_date = Column(Date, primary_key=True, doc="The date of the log entry.")
    starting_capital = Column(Float, doc="Capital at the start of the day.")
    ending_capital = Column(Float, doc="Capital at the end of the day.")
    absolute_gain = Column(Float, doc="Absolute gain (or loss) for the day.")
    percent_gain = Column(Float, doc="Percentage gain (or loss) for the day.")
    instrument = Column(String, doc="Instrument that was traded")


# ------------------------------------------------------------------------------
# ActiveTrades Table
# ------------------------------------------------------------------------------
class ActiveTrades(Base):
    """
    Stores currently active trades.

    Attributes:
        trade_id (Integer): Unique trade identifier (primary key).
        symbol (String): Trading symbol.
        entry_time (BigInteger): Entry time as Unix timestamp.
        entry_price (Float): Price at which the trade was entered.
        stop_loss (Float): Stop loss price.
        position_size (Integer): Number of units traded.
        allocated (Float): Capital allocated to the trade.
        margin_availed (Integer): Margin availed for the trade.
    """

    __tablename__ = "active_trades"

    trade_id = Column(
        Integer, primary_key=True, doc="Unique identifier for an active trade."
    )
    symbol = Column(String, doc="Trading symbol.")
    entry_time = Column(TIMESTAMP, doc="Trade entry time (Unix timestamp).")
    entry_price = Column(Float, doc="Entry price of the trade.")
    stop_loss = Column(Float, doc="Stop loss price.")
    position_size = Column(Integer, doc="Position size (number of units).")
    allocated = Column(Float, doc="Capital allocated to this trade.")
    margin_availed = Column(Integer, doc="Margin availed for the trade.")
    order_id = Column(String, doc="Identifier to connect to Orders table")
    instrument = Column(String, doc="Instrument that was traded")
    take_profit_price = Column(Float, doc="Take profit price.", default=None)
    realized_pnl_tp = Column(Float, doc="Realized PnL at take profit.", default=None)
    position_closed_tp = Column(
        Integer, doc="Position closed at take profit.", default=None
    )
    position_remaining_tp = Column(
        Integer, doc="Position remaining at take profit.", default=None
    )


# ------------------------------------------------------------------------------
# TradeLog Table
# ------------------------------------------------------------------------------
class TradeLog(Base):
    """
    Logs historical trade data.

    Note: A surrogate primary key 'id' is added for uniqueness.

    Attributes:
        id (Integer): Auto-increment primary key.
        trade_date (Date): Date when the trade was executed.
        trade_id (Integer): Identifier linking to the trade.
        symbol (String): Trading symbol.
        entry_time (BigInteger): Entry time (Unix timestamp).
        entry_price (Float): Price at trade entry.
        exit_time (BigInteger): Exit time (Unix timestamp).
        exit_price (Float): Price at trade exit.
        capital_employed (Float): Capital employed in the trade.
        capital_at_exit (Float): Capital available at exit.
        position_size (Integer): Position size.
        pl (Float): Profit or loss amount.
        percent_change (Float): Percentage change in value.
        capital_employed_pct (Float): Percentage of capital employed.
        global_profit (Float): Overall profit measure.
        reason (Text): Reason or note regarding the trade.
        margin_availed (Integer): Margin availed for the trade.
        trade_type (String): Type of trade (e.g., long, short).
    """

    __tablename__ = "trade_log"

    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc="Primary key for trade log records.",
    )
    trade_date = Column(Date, doc="Date of the trade.")
    trade_id = Column(Integer, doc="Trade identifier.")
    symbol = Column(String, doc="Trading symbol.")
    entry_time = Column(TIMESTAMP, doc="Trade entry time (Unix timestamp).")
    entry_price = Column(Float, doc="Price at entry.")
    exit_time = Column(TIMESTAMP, doc="Trade exit time (Unix timestamp).")
    exit_price = Column(Float, doc="Price at exit.")
    capital_employed = Column(Float, doc="Capital employed in the trade.")
    capital_at_exit = Column(Float, doc="Capital at the time of exit.")
    position_size = Column(Integer, doc="Position size (number of units).")
    pl = Column(Float, doc="Profit or loss.")
    percent_change = Column(Float, doc="Percentage change in value.")
    capital_employed_pct = Column(Float, doc="Percentage of capital employed.")
    global_profit = Column(Float, doc="Global profit metric.")
    margin_availed = Column(Integer, doc="Margin availed during the trade.")
    trade_type = Column(String, doc="Type of trade (e.g., 'long', 'short').")
    order_id = Column(String, doc="Identifier to connect to Orders table")
    instrument = Column(String, doc="Instrument that was traded")


# ------------------------------------------------------------------------------
# StockSignals Table
# ------------------------------------------------------------------------------
class StockSignals(Base):
    """
    Contains signals generated for stock trading.

    Attributes:
        trade_id (Integer): Auto-increment signal identifier (primary key).
        symbol (String): Trading symbol.
        time (String): Time of the signal (as a string; can be changed to a DateTime if desired).
        price (Float): Price at the signal time.
        signal (String): Signal type.
        type (String): Entry/Exit/SquareOff/TakeProfit.
        position_size (Float): Position size.
        starting_position_size (Float): Starting position size.
        current_position_size (Float): Current position size.
        position_to_close (Float): Position to close.
        closed_position_size (Float): Closed position size.
        remaining_position_size (Float): Remaining position size.
        current_position (String): Current position.
        status (String): Status of the signal.
        take_profit_price (Float): Take profit price.
        stop_loss_price (Float): Stop loss price.
        note (String): Note for the signal.
    """

    __tablename__ = "stock_signals"

    id = Column(
        Integer, primary_key=True, autoincrement=True, doc="Unique signal identifier."
    )
    symbol = Column(String, doc="Trading symbol.")
    time = Column(String, doc="Time when the signal was generated.")
    price = Column(Float, doc="Price at the time of the signal.")
    signal = Column(String, doc="Type of signal (e.g., 'buy', 'sell').")
    type = Column(String, doc="Entry/Exit/SquareOff/TakeProfit.")
    position_size = Column(Integer, doc="Recommended position size.")
    starting_position_size = Column(Integer, doc="Starting position size.")
    current_position_size = Column(Integer, doc="Current position size.")
    position_to_close = Column(Integer, doc="Position to close.")
    closed_position_size = Column(Integer, doc="Closed position size.")
    remaining_position_size = Column(Integer, doc="Remaining position size.")
    current_position = Column(String, doc="Current position.")
    status = Column(String, doc="Status of the signal.")
    take_profit_price = Column(Float, doc="Take profit price.")
    stop_loss = Column(Float, doc="Stop loss price.")
    note = Column(String, doc="Note for the signal.")
    trailing_stop_triggered = Column(Boolean, doc="Trailing stop triggered.")
    trailing_stop_level = Column(Float, doc="Trailing stop level.")
    auto_square_off = Column(Boolean, doc="Auto square off.")


# ------------------------------------------------------------------------------
# MarketData Table
# ------------------------------------------------------------------------------
class MarketData(Base):
    """
    Stores market data snapshots.

    Attributes:
        id (Integer): Auto-increment primary key.
        symbol (String): Trading symbol (non-nullable).
        ltp (Float): Last traded price.
        vol_traded_today (Integer): Today's traded volume.
        last_traded_time (BigInteger): Last traded time (Unix timestamp).
        exch_feed_time (BigInteger): Exchange feed time (Unix timestamp).
        bid_size (Integer): Bid size.
        ask_size (Integer): Ask size.
        bid_price (Float): Bid price.
        ask_price (Float): Ask price.
        last_traded_qty (Integer): Last traded quantity.
        tot_buy_qty (Integer): Total buy quantity.
        tot_sell_qty (Integer): Total sell quantity.
        avg_trade_price (Float): Average trade price.
        low_price (Float): Low price for the period.
        high_price (Float): High price for the period.
        open_price (Float): Opening price.
        prev_close_price (Float): Previous closing price.
        type (String): Type of market data.
        ch (Float): Price change.
        chp (Float): Percentage price change.
    """

    __tablename__ = "market_data"

    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc="Primary key for market data entries.",
    )
    symbol = Column(String, nullable=False, doc="Trading symbol.")
    ltp = Column(Float, doc="Last traded price.")
    vol_traded_today = Column(Integer, doc="Volume traded today.")
    last_traded_time = Column(BigInteger, doc="Last traded time (Unix timestamp).")
    exch_feed_time = Column(BigInteger, doc="Exchange feed time (Unix timestamp).")
    bid_size = Column(Integer, doc="Bid size.")
    ask_size = Column(Integer, doc="Ask size.")
    bid_price = Column(Float, doc="Bid price.")
    ask_price = Column(Float, doc="Ask price.")
    last_traded_qty = Column(Integer, doc="Last traded quantity.")
    tot_buy_qty = Column(Integer, doc="Total buy quantity.")
    tot_sell_qty = Column(Integer, doc="Total sell quantity.")
    avg_trade_price = Column(Float, doc="Average trade price.")
    low_price = Column(Float, doc="Low price.")
    high_price = Column(Float, doc="High price.")
    open_price = Column(Float, doc="Opening price.")
    prev_close_price = Column(Float, doc="Previous closing price.")
    type = Column(String, doc="Market data type.")
    ch = Column(Float, doc="Absolute price change.")
    chp = Column(Float, doc="Percentage price change.")
    instrument = Column(String, doc="Instrument that was traded")
    lower_ckt = Column(Float, doc="Lower circuit price")
    upper_ckt = Column(Float, doc="Upper circuit price")


# ------------------------------------------------------------------------------
# Orders Table
# ------------------------------------------------------------------------------
class Orders(Base):
    """
    Stores order details placed by users.

    Attributes:
        order_id (Integer): Unique identifier for each order (primary key).
        order_date (Date): The date when the order was placed.
        order_time (BigInteger): The time the order was placed, as a Unix timestamp.
        symbol (String): Trading symbol for which the order is placed.
        quantity (Integer): Number of shares/units ordered.
        order_type (String): Type of order (e.g., 'buy', 'sell').
        price (Float): The price at which the order is to be executed.
        status (String): Current status of the order (e.g., 'pending', 'executed', 'cancelled').
    """

    __tablename__ = "orders"

    order_id = Column(
        Integer, primary_key=True, autoincrement=True, doc="Unique order identifier."
    )
    order_date = Column(Date, nullable=False, doc="Date when the order was placed.")
    order_time = Column(
        BigInteger,
        nullable=False,
        doc="Time when the order was placed (Unix timestamp).",
    )
    symbol = Column(String, nullable=False, doc="Trading symbol for the order.")
    quantity = Column(
        Integer, nullable=False, doc="Number of shares/units in the order."
    )
    order_type = Column(String, nullable=False, doc="Type of order ('buy' or 'sell').")
    price = Column(Float, nullable=False, doc="Price at which the order is executed.")
    status = Column(
        String,
        nullable=False,
        doc="Current status of the order (e.g., 'pending', 'executed', 'cancelled').",
    )
    broker = Column(String, nullable=False, doc="Broker used")
    broker_order_id = Column(
        String, nullable=False, doc="Identifier to trace back to Order ID with broker"
    )
    # instrument = Column(String, doc="Instrument that was traded")


# ------------------------------------------------------------------------------
# Database Engine and Session Setup
# ------------------------------------------------------------------------------

# # Replace with your actual PostgreSQL connection string
# engine = create_engine(
#     "postgresql+psycopg2://username:password@localhost/dbname",
#     echo=True  # Set to False in production
# )

# # Create all tables in the PostgreSQL database
# Base.metadata.create_all(engine)

# # Create a configured "Session" class and a session instance.
# Session = sessionmaker(bind=engine)
# session = Session()

# # ------------------------------------------------------------------------------
# # Notes on Concurrency:
# # ------------------------------------------------------------------------------
# # - SQLAlchemy's engine is thread-safe. Each thread should create its own Session
# #   instance from the sessionmaker.
# # - For multiprocessing, do not share the engine or session objects between
# #   processes. Instead, instantiate a new engine and session in each process.
# #
# # With proper session management, this design is safe and scalable for use with
# # PostgreSQL in both multithreading and multiprocessing contexts.
# # Create all tables in the PostgreSQL database


def initialize_database():
    url = os.environ["DATABASE_URL"]
    parsed = urlparse(url)

    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    db = parsed.path.lstrip("/")

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        echo=False,  # Set to False in production
    )
    global Base
    Base.metadata.create_all(engine)


def delete_database_tables(table_names=None):
    """
    Drops specified tables from the database. If table_names is None, drops all tables.
    Args:
        table_names (list or None): List of table names to drop. If None, drops all tables.
    """
    url = os.environ["DATABASE_URL"]
    parsed = urlparse(url)

    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    db = parsed.path.lstrip("/")
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        echo=False,  # Set to False in production
    )
    global Base
    metadata = MetaData()
    metadata.reflect(bind=engine)
    if table_names is None:
        # Drop all tables
        Base.metadata.drop_all(engine)
        print("All tables have been dropped.")
    else:
        for table_name in table_names:
            if table_name in metadata.tables:
                table = Table(table_name, metadata, autoload_with=engine)
                table.drop(engine, checkfirst=True)
                print(f"Dropped table: {table_name}")
            else:
                print(f"Table not found: {table_name}")


def setup(echo=False):
    """
    Setup the database engine and session.
    """
    url = os.environ["DATABASE_URL"]
    parsed = urlparse(url)

    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    db = parsed.path.lstrip("/")
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        echo=echo,  # Set to False in production
        pool_size=200,
        max_overflow=100,
        pool_timeout=10,
        pool_recycle=1800,
        pool_pre_ping=True
    )
    # # Create a configured "Session" class and a session instance.
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session


class DBHandler:
    """
    A helper class for performing generic insert and delete operations across
    all SQLAlchemy ORM–mapped tables. This class is designed to be used with
    PostgreSQL (or any SQLAlchemy-supported database) and supports safe usage
    in multithreading via scoped sessions.

    Usage:
        # Create an engine first (example for PostgreSQL):
        # engine = create_engine("postgresql+psycopg2://username:password@localhost/dbname", echo=True)

        db_helper = DBHelper(engine)

        # Inserting a single record:
        db_helper.insert_records(new_order)

        # Inserting multiple records:
        db_helper.insert_records([record1, record2, record3])

        # Deleting a record (by instance):
        db_helper.delete_records(records=order_instance)

        # Bulk deletion using filter criteria:
        from datetime import date
        criteria = [Orders.status == 'cancelled', Orders.order_date < date.today()]
        db_helper.delete_records(model=Orders, criteria=criteria)
    """

    def __init__(self, engine):
        # Create a session factory and a scoped session (thread-local)
        self.engine = engine
        self.SessionFactory = sessionmaker(bind=engine)
        self.Session = scoped_session(self.SessionFactory)

    def insert_records(self, records):
        """
        Inserts one or more records into the database.

        Args:
            records: Either a single ORM model instance or a list/tuple of instances.
        """
        session = self.Session()
        try:
            # Get the record or first record from a list to determine table name
            record = records[0] if isinstance(records, (list, tuple)) else records
            table_name = record.__table__.name

            if isinstance(records, (list, tuple)):
                session.add_all(records)
                logger.debug(
                    f"Inserting {len(records)} records into table '{table_name}'"
                )
            else:
                session.add(records)
                logger.debug(f"Inserting 1 record into table '{table_name}'")

            session.commit()
            logger.debug("Insert successful.")
        except Exception as e:
            session.rollback()
            logger.error("Error inserting records:", e)
        finally:
            # Remove the session from thread-local storage.
            session.close()
            self.Session.remove()

    def delete_records(self, *, model=None, criteria=None, records=None):
        """
        Deletes records from the database.

        You can delete records in two ways:

        1. Directly via ORM instance(s):
             delete_records(records=record_instance)
             delete_records(records=[record1, record2])

        2. Bulk deletion via model and filter criteria:
             delete_records(model=Orders, criteria=(Orders.status == 'cancelled', Orders.order_date < some_date))
             OR
             delete_records(model=Orders, criteria=[Orders.status == 'cancelled', Orders.order_date < some_date])

        Args:
            model: The ORM model class from which to delete records (required for bulk deletion).
            criteria: A single SQLAlchemy filter condition or a list/tuple of conditions.
            records: A single record instance or a list/tuple of instances to delete.
        """
        session = self.Session()
        try:
            if records is not None:
                # Get table name from the record(s)
                record = records[0] if isinstance(records, (list, tuple)) else records
                table_name = record.__table__.name

                # Delete provided record(s)
                if isinstance(records, (list, tuple)):
                    logger.debug(
                        f"Deleting {len(records)} records from table '{table_name}'"
                    )
                    for record in records:
                        session.delete(record)
                else:
                    logger.debug(f"Deleting 1 record from table '{table_name}'")
                    session.delete(records)
            elif model is not None and criteria is not None:
                # Get table name from the model
                table_name = model.__table__.name

                # Combine multiple criteria using AND if needed
                if isinstance(criteria, (list, tuple)):
                    combined_criteria = and_(*criteria)
                else:
                    combined_criteria = criteria

                logger.debug(
                    f"Performing bulk deletion from table '{table_name}' with criteria"
                )
                session.query(model).filter(combined_criteria).delete(
                    synchronize_session=False
                )
            else:
                raise ValueError(
                    "Either provide 'records' or both 'model' and 'criteria' for deletion."
                )

            session.commit()
            logger.debug("Deletion successful.")
        except Exception as e:
            session.rollback()
            print("Error deleting records:", e)
        finally:
            # Clean up the thread-local session.
            session.close()
            self.Session.remove()

    def update_records(self, model, record_id, update_data, id_column=None):
        """
        Updates records in the database.

        Args:
            model: The ORM model class to update records in.
            record_id: The ID value to filter by.
            update_data: A dictionary of fields to update with their new values.
            id_column: The column name to use for filtering. If None, defaults to 'trade_id'.
        """
        session = self.Session()
        try:
            # Get table name from the model
            table_name = model.__table__.name

            # Determine which column to use for filtering
            if id_column is None:
                id_column = "trade_id"  # Default to trade_id for backward compatibility

            # Build filter criteria dynamically
            filter_criteria = {id_column: record_id}

            logger.debug(
                f"Attempting to update record in table '{table_name}' with {id_column}={record_id}"
            )

            # Find the record(s) with the specified ID
            record = session.query(model).filter_by(**filter_criteria).one_or_none()
            if record is None:
                logger.warning(
                    f"No record found in table '{table_name}' with {id_column}={record_id}"
                )
                return

            # Update the record with the new data
            for key, value in update_data.items():
                setattr(record, key, value)

            session.commit()
            logger.debug(f"Successfully updated record in table '{table_name}'")
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating records: {e}")
        finally:
            # Clean up the thread-local session.
            session.close()
            self.Session.remove()

    def query_records(self, model, criteria=None, limit=None):
        """
        Queries records from the database.

        Args:
            model: The ORM model class to query.
            criteria: A single SQLAlchemy filter condition or a list/tuple of conditions.
            limit: Optional limit on the number of records returned.

        Returns:
            list: A list of records matching the criteria.
        """

        session = self.Session()
        try:
            query = session.query(model)

            if criteria is not None:
                # Combine multiple criteria using AND if needed
                if isinstance(criteria, (list, tuple)):
                    combined_criteria = and_(*criteria)
                else:
                    combined_criteria = criteria
                query = query.filter(combined_criteria)

            if limit is not None:
                query = query.limit(limit)

            result = query.all()
            return result
        except Exception as e:
            logger.error(f"Error querying records: {e}")
            return []
        finally:
            # Clean up the thread-local session.
            session.close()
            self.Session.remove()

    def get_max_trade_id(self, model):
        """
        Get the maximum trade_id from a table.

        Args:
            model: The ORM model class to query.

        Returns:
            int: The maximum trade_id value, or 0 if no records exist.
        """
        session = self.Session()
        try:
            max_id = session.query(func.max(model.trade_id)).scalar()
            return max_id if max_id is not None else 0
        except Exception as e:
            logger.error(f"Error getting max trade_id: {e}")
            return 0
        finally:
            session.close()
            self.Session.remove()

    def execute_sql_query(self, query, params=None, fetch=True):
        """
        Execute a raw SQL query with optional parameters.

        Args:
            query (str): The SQL query to execute
            params (dict or tuple, optional): Parameters for the query. Can be a dictionary for named parameters
                                             or a tuple for positional parameters.
            fetch (bool, optional): If True, returns the query results. If False, just executes the query
                                  (useful for INSERT, UPDATE, DELETE operations).

        Returns:
            If fetch is True:
                list: A list of dictionaries containing the query results
            If fetch is False:
                int: Number of rows affected by the query
        """
        if params is None:
            params = {}

        # Wrap the query in text() for proper SQLAlchemy handling
        sql_text = text(query)

        try:
            # Use the stored engine from initialization
            with self.engine.connect() as connection:
                result = connection.execute(sql_text, params)

                if fetch:
                    # Convert result to list of dictionaries
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in result]
                else:
                    # Return number of rows affected
                    return result.rowcount

        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            raise
