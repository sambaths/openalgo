import importlib
import logging
import traceback
from typing import Tuple, Dict, Any, Optional, List, Union
from database.auth_db import get_auth_token_broker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def import_broker_module(broker_name: str) -> Optional[Any]:
    """
    Dynamically import the broker-specific data module.
    Args:
        broker_name: Name of the broker
    Returns:
        The imported module or None if import fails
    """
    try:
        module_path = f'broker.{broker_name}.api.data'
        broker_module = importlib.import_module(module_path)
        return broker_module
    except ImportError as error:
        logger.error(f"Error importing broker module '{module_path}': {error}")
        return None

def get_intraday_margin_with_auth(
    auth_token: str,
    feed_token: Optional[str],
    broker: str,
    symbols: List[str],
) -> Tuple[bool, Dict[str, Any], int]:
    """
    Get margin info for a list of symbols using provided auth tokens.
    Args:
        auth_token: Authentication token for the broker API
        feed_token: Feed token for market data (if required by broker)
        broker: Name of the broker
        symbols: List of trading symbols
    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    broker_module = import_broker_module(broker)
    if broker_module is None:
        return False, {
            'status': 'error',
            'message': 'Broker-specific module not found'
        }, 404

    try:
        # Initialize broker's data handler based on broker's requirements
        if hasattr(broker_module.BrokerData.__init__, '__code__'):
            param_count = broker_module.BrokerData.__init__.__code__.co_argcount
            if param_count > 2:
                data_handler = broker_module.BrokerData(auth_token, feed_token)
            else:
                data_handler = broker_module.BrokerData(auth_token)
        else:
            data_handler = broker_module.BrokerData(auth_token)

        margin_dict = data_handler.get_intraday_margin(symbols)
        return True, {
            'status': 'success',
            'data': margin_dict
        }, 200
    except Exception as e:
        logger.error(f"Error in broker_module.get_margin: {e}")
        traceback.print_exc()
        return False, {
            'status': 'error',
            'message': str(e)
        }, 500

def get_intraday_margin(
    symbols: List[str],
    api_key: Optional[str] = None,
    auth_token: Optional[str] = None,
    feed_token: Optional[str] = None,
    broker: Optional[str] = None
) -> Tuple[bool, Dict[str, Any], int]:
    """
    Get margin info for a list of symbols.
    Supports both API-based authentication and direct internal calls.
    Args:
        symbols: List of trading symbols
        api_key: OpenAlgo API key (for API-based calls)
        auth_token: Direct broker authentication token (for internal calls)
        feed_token: Direct broker feed token (for internal calls)
        broker: Direct broker name (for internal calls)
    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, FEED_TOKEN, broker_name = get_auth_token_broker(api_key, include_feed_token=True)
        if AUTH_TOKEN is None:
            return False, {
                'status': 'error',
                'message': 'Invalid openalgo apikey'
            }, 403
        return get_intraday_margin_with_auth(
            AUTH_TOKEN,
            FEED_TOKEN,
            broker_name,
            symbols
        )
    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        return get_intraday_margin_with_auth(
            auth_token,
            feed_token,
            broker,
            symbols
        )
    # Case 3: Invalid parameters
    else:
        return False, {
            'status': 'error',
            'message': 'Either api_key or both auth_token and broker must be provided'
        }, 400
