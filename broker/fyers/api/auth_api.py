import os
import json
import hashlib
import base64
import time
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import requests
import pyotp
from urllib.parse import parse_qs, urlparse
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)


def getEncodedString(string):
    return base64.b64encode(str(string).encode("ascii")).decode("ascii")

def authenticate_broker_normal(request_token: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Authenticate with FYERS API using request token and return access token with user details.
    
    Args:
        request_token: The authorization code received from FYERS
        
    Returns:
        Tuple of (access_token, response_data). 
        - access_token: The authentication token if successful, None otherwise
        - response_data: Full response data or error details
    """
    # Initialize response data
    response_data = {
        'status': 'error',
        'message': 'Authentication failed',
        'data': None
    }
    
    # Get environment variables
    broker_api_key = os.getenv('BROKER_API_KEY')
    broker_api_secret = os.getenv('BROKER_API_SECRET')
    
    # Validate environment variables
    if not broker_api_key or not broker_api_secret:
        error_msg = "Missing BROKER_API_KEY or BROKER_API_SECRET in environment variables"
        logger.error(error_msg)
        response_data['message'] = error_msg
        return None, response_data
    
    if not request_token:
        error_msg = "No request token provided"
        logger.error(error_msg)
        response_data['message'] = error_msg
        return None, response_data
    
    # FYERS's endpoint for session token exchange
    url = 'https://api-t1.fyers.in/api/v3/validate-authcode'
    
    try:
        # Generate the checksum as a SHA-256 hash of concatenated api_key and api_secret
        checksum_input = f"{broker_api_key}:{broker_api_secret}"
        app_id_hash = hashlib.sha256(checksum_input.encode('utf-8')).hexdigest()
        
        # Prepare the request payload
        payload = {
            'grant_type': 'authorization_code',
            'appIdHash': app_id_hash,
            'code': request_token
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Get shared HTTP client with connection pooling
        client = get_httpx_client()
        
        logger.debug(f"Authenticating with FYERS API. Request: {json.dumps(payload, indent=2)}")
        
        # Make the authentication request
        response = client.post(
            url,
            headers=headers,
            json=payload,
            timeout=30.0  # Increased timeout for auth requests
        )
        
        # Process the response
        response.raise_for_status()
        auth_data = response.json()
        logger.debug(f"FYERS auth API response: {json.dumps(auth_data, indent=2)}")
        
        if auth_data.get('s') == 'ok':
            access_token = auth_data.get('access_token')
            if not access_token:
                error_msg = "Authentication succeeded but no access token was returned"
                logger.error(error_msg)
                response_data['message'] = error_msg
                return None, response_data
                
            # Prepare success response
            response_data.update({
                'status': 'success',
                'message': 'Authentication successful',
                'data': {
                    'access_token': access_token,
                    'refresh_token': auth_data.get('refresh_token'),
                    'expires_in': auth_data.get('expires_in')
                }
            })
            
            logger.debug("Successfully authenticated with FYERS API")
            return access_token, response_data
            
        else:
            # Handle API error response
            error_msg = auth_data.get('message', 'Authentication failed')
            logger.error(f"FYERS API error: {error_msg}")
            response_data['message'] = f"API error: {error_msg}"
            return None, response_data
            
    except Exception as e:
        error_msg = f"Authentication failed: {e}"
        logger.exception("Authentication failed due to an unexpected error")
        response_data['message'] = error_msg
        return None, response_data
    
def authenticate_broker(request_token: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Authenticate with FYERS API using TOTP and return access token with user details.
    Args:
        request_token: Ignored (kept for signature compatibility)
    Returns:
        Tuple of (access_token, response_data).
    """
    response_data = {
        'status': 'error',
        'message': 'Authentication failed',
        'data': None
    }
    try:
        # Required env vars
        fy_id = os.environ['BROKER_ID']
        totp_key = os.environ['BROKER_TOTP_KEY']
        pin = os.environ['BROKER_TOTP_PIN']
        client_id = os.environ['BROKER_API_KEY']
        secret_key = os.environ['BROKER_API_SECRET']
        redirect_uri = os.environ['BROKER_TOTP_REDIDRECT_URI']
        response_type = "code" # Should be always `code`
        grant_type = "authorization_code" # Should be always `authorization_code`
        # Step 1: Send login OTP
        URL_SEND_LOGIN_OTP = "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2"
        res = requests.post(url=URL_SEND_LOGIN_OTP, json={
            "fy_id": getEncodedString(fy_id),
            "app_id": "2"
        }).json()
        if datetime.now().second % 30 > 27:
            time.sleep(5)
        # Step 2: Verify OTP
        URL_VERIFY_OTP = "https://api-t2.fyers.in/vagator/v2/verify_otp"
        res2 = requests.post(url=URL_VERIFY_OTP, json={
            "request_key": res["request_key"],
            "otp": pyotp.TOTP(totp_key).now()
        }).json()
        # Step 3: Verify PIN
        ses = requests.Session()
        URL_VERIFY_OTP2 = "https://api-t2.fyers.in/vagator/v2/verify_pin_v2"
        payload2 = {
            "request_key": res2["request_key"],
            "identity_type": "pin",
            "identifier": getEncodedString(pin)
        }
        res3 = ses.post(url=URL_VERIFY_OTP2, json=payload2).json()
        ses.headers.update({
            'authorization': f"Bearer {res3['data']['access_token']}"
        })
        # Step 4: Get auth code
        TOKENURL = "https://api-t1.fyers.in/api/v3/token"
        payload3 = {
            "fyers_id": fy_id,
            "app_id": client_id[:-4],
            "redirect_uri": redirect_uri,
            "appType": "100",
            "code_challenge": "",
            "state": "None",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        res4 = ses.post(url=TOKENURL, json=payload3).json()
        parsed = urlparse(res4['Url'])
        auth_code = parse_qs(parsed.query)['auth_code'][0]
        # Step 5: Exchange auth code for access token
        import hashlib
        url = 'https://api-t1.fyers.in/api/v3/validate-authcode'
        checksum_input = f"{client_id}:{secret_key}"
        app_id_hash = hashlib.sha256(checksum_input.encode('utf-8')).hexdigest()
        payload = {
            'grant_type': grant_type,
            'appIdHash': app_id_hash,
            'code': auth_code
        }
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = ses.post(url, headers=headers, json=payload, timeout=30.0)
        response.raise_for_status()
        auth_data = response.json()
        if auth_data.get('s') == 'ok':
            access_token = auth_data.get('access_token')
            if not access_token:
                response_data['message'] = "Authentication succeeded but no access token was returned"
                return None, response_data
            response_data.update({
                'status': 'success',
                'message': 'Authentication successful',
                'data': {
                    'access_token': access_token,
                    'refresh_token': auth_data.get('refresh_token'),
                    'expires_in': auth_data.get('expires_in')
                }
            })
            return access_token, response_data
        else:
            error_msg = auth_data.get('message', 'Authentication failed')
            response_data['message'] = f"API error: {error_msg}"
            return None, response_data
    except Exception as e:
        response_data['message'] = f"Authentication failed: {str(e)}"
        return None, response_data