from flask_restx import Namespace, Resource
from flask import request, jsonify, make_response
from marshmallow import ValidationError
from limiter import limiter
import os
import logging
import traceback

from .data_schemas import IntradayMarginSchema
from services.intraday_margin_service import get_intraday_margin

API_RATE_LIMIT = os.getenv("API_RATE_LIMIT", "10 per second")
api = Namespace('margin', description='Margin Data API')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize schema
margin_schema = IntradayMarginSchema()

@api.route('/', strict_slashes=False)
class Margin(Resource):
    @limiter.limit(API_RATE_LIMIT)
    def post(self):
        """Get margin info for given symbols"""
        try:
            # Validate request data
            margin_data = margin_schema.load(request.get_json())
            api_key = margin_data['apikey']
            symbols = margin_data['symbols']
            # Call the service function to get margin info with API key
            success, response_data, status_code = get_intraday_margin(
                symbols=symbols,
                api_key=api_key
            )
            return make_response(jsonify(response_data), status_code)

        except ValidationError as err:
            return make_response(jsonify({
                'status': 'error',
                'message': err.messages
            }), 400)
        except Exception as e:
            logger.error(f"Unexpected error in margin endpoint: {e}")
            traceback.print_exc()
            return make_response(jsonify({
                'status': 'error',
                'message': 'An unexpected error occurred'
            }), 500)
