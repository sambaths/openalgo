if __name__ == "__main__":
    import os, sys
    import logging

    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Custom imports
    from system_utils import load_config
    from system.misc.db_cleanup import delete_day_data
    from logger import logger

    os.environ["DEBUG_LEVEL"] = "DEBUG"
    logger.setLevel(getattr(logging, os.environ["DEBUG_LEVEL"].upper(), None))
    from driver import Driver
    from db import initialize_database, delete_database_tables
    from dotenv import load_dotenv

    load_dotenv()

    CONFIG_PATH = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "config.yaml"
    )
    config = load_config(CONFIG_PATH)


    if config["trading_setting"]["simulation_type"].lower() in ['historical', 'live']:
        os.environ["SIMULATION_TYPE"] = config["trading_setting"]["simulation_type"].lower()
    else:
        raise ValueError("Simulation type must be either historical or live")

    os.environ["LIVE_DATA"] = str(not config["trading_setting"]["simulation"]).lower()
    trading_date = (
        config["trading_setting"]["trading_date"]
        if os.environ["LIVE_DATA"]
        else config["trading_setting"]["simulation_date"]
    )

    # Set the simulation type to live or historical
    os.environ["SIMULATION_TYPE"] = config["trading_setting"]["simulation_type"].lower()
    delete_database_tables(['market_data', 'stock_signals', 'trade_log', 'active_trades', 'capital_log'])
    initialize_database()
    delete_day_data(trading_date)
    driver = Driver(config)

    try:
        driver.run()
    except KeyboardInterrupt:
        logger.info("Main process interrupted. Shutting down...")
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
    finally:
        # Ensure shutdown is called even if there's an exception
        if "driver" in locals():
            driver.shutdown()
