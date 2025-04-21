from flask import Flask, request, jsonify
import requests
import base64
import urllib.parse
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
import time
import ssl
import logging
import signal
import sys
import os
import json
import datetime
import hashlib
import random
from pymongo import MongoClient
from confluent_kafka import Producer
from datetime import datetime, timedelta
from bson import ObjectId
import atexit
from flask_cors import CORS  # Import Flask-CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# OAuth configuration – update these as needed
CLIENT_ID = os.getenv("SCHWAB_CLIENT_ID")
CLIENT_SECRET = os.getenv("SCHWAB_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SCHWAB_REDIRECT_URI")
AUTHORIZATION_URL = os.getenv("SCHWAB_AUTHORIZATION_URL")
TOKEN_URL = os.getenv("SCHWAB_TOKEN_URL")
ACCOUNTS_URL = os.getenv("SCHWAB_ACCOUNTS_URL")

# Add debug logging for OAuth configuration
logger.debug("OAuth Configuration:")
logger.debug(f"CLIENT_ID: {CLIENT_ID}")
logger.debug(f"CLIENT_SECRET: {'*' * len(CLIENT_SECRET) if CLIENT_SECRET else 'Not Set'}")
logger.debug(f"REDIRECT_URI: {REDIRECT_URI}")
logger.debug(f"AUTHORIZATION_URL: {AUTHORIZATION_URL}")
logger.debug(f"TOKEN_URL: {TOKEN_URL}")
logger.debug(f"ACCOUNTS_URL: {ACCOUNTS_URL}")

# Global variables - used only for OAuth flow within a single request
auth_code = None
callback_url = None

# Add a custom JSON encoder class to handle MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(MongoJSONEncoder, self).default(obj)

# Configure Flask to use our custom JSON encoder
app.json_encoder = MongoJSONEncoder

def get_stored_tokens(user_id):
    """
    Retrieve stored tokens for a user from MongoDB
    
    Args:
        user_id (str): The hashed user ID
        
    Returns:
        dict: Containing access_token, refresh_token, and expiry time or None if not found
    """
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client["Integrations"]
        tokens_collection = db["SchwabTokens"]
        
        # Find tokens for this user
        token_doc = tokens_collection.find_one({"user_id": user_id})
        return token_doc
    except Exception as e:
        logger.error(f"Error retrieving stored tokens: {e}")
        return None

def store_tokens(user_id, access_token, refresh_token, expires_in):
    """
    Store tokens in MongoDB for future use
    
    Args:
        user_id (str): The hashed user ID
        access_token (str): The OAuth access token
        refresh_token (str): The OAuth refresh token
        expires_in (int): Token expiry time in seconds
    """
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client["Integrations"]
        tokens_collection = db["SchwabTokens"]
        
        # Calculate expiry time
        expiry_time = datetime.utcnow() + timedelta(seconds=expires_in)
        
        # Prepare token document
        token_doc = {
            "user_id": user_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expiry_time": expiry_time
        }
        
        # Upsert the token document
        tokens_collection.update_one(
            {"user_id": user_id},
            {"$set": token_doc},
            upsert=True
        )
        logger.info(f"Tokens stored for user {user_id}")
    except Exception as e:
        logger.error(f"Error storing tokens: {e}")

def get_access_token(code=None, refresh_token=None):
    """
    Get an access token either using an authorization code or a refresh token
    
    Args:
        code (str, optional): The authorization code
        refresh_token (str, optional): The OAuth refresh token
        
    Returns:
        dict: Containing access_token, refresh_token, and expires_in
    """
    try:
        # Prepare the Basic Authorization header
        credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        # Determine grant type and prepare data
        if code:
            data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI
            }
            logger.debug(f"Using authorization code flow with code: {code}")
        elif refresh_token:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
            logger.debug("Using refresh token flow")
        else:
            raise ValueError("Either code or refresh_token must be provided")
        
        # Log request details
        logger.debug(f"Token request URL: {TOKEN_URL}")
        logger.debug(f"Token request headers: {headers}")
        logger.debug(f"Token request data: {data}")
        
        # Make the token request
        response = requests.post(TOKEN_URL, headers=headers, data=data, verify=False)
        
        # Log response details
        logger.debug(f"Token response status code: {response.status_code}")
        logger.debug(f"Token response headers: {response.headers}")
        
        if response.status_code != 200:
            logger.error(f"Error fetching token: {response.status_code}")
            logger.error(f"Response headers: {response.headers}")
            logger.error(f"Response body: {response.text}")
            response.raise_for_status()
        
        token_data = response.json()
        logger.debug("Successfully obtained token data")
        
        return {
            "access_token": token_data.get("access_token"),
            "refresh_token": token_data.get("refresh_token"),
            "expires_in": token_data.get("expires_in", 1800)  # Default to 30 minutes
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed during token exchange: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode token response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during token exchange: {str(e)}")
        raise

def get_account_positions(access_token):
    """
    Retrieve account positions from Schwab API using the provided access token
    
    Args:
        access_token (str): The OAuth access token specific to the user
        
    Returns:
        list: Account positions data
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    params = {
        "fields": "positions"
    }
    response = requests.get(ACCOUNTS_URL, headers=headers, params=params, verify=False)
    
    if response.status_code != 200:
        logger.error("Error fetching account positions:")
        logger.error("Status code: %s", response.status_code)
        logger.error("Response text: %s", response.text)
        response.raise_for_status()
    
    return response.json()

def convert_schwab_positions_to_portfolio(schwab_data, user_params):
    """
    Convert Schwab positions data to a portfolio format with enhanced fields
    """
    holdings = []
    current_time = datetime.utcnow()
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    try:
        # Process each account in the response
        for account in schwab_data:
            if 'securitiesAccount' in account:
                account_data = account['securitiesAccount']
                
                if 'positions' in account_data:
                    for position in account_data['positions']:
                        if position.get('longQuantity', 0) > 0 or position.get('shortQuantity', 0) > 0:
                            instrument = position.get('instrument', {})
                            
                            quantity = float(position.get('longQuantity', 0))
                            cost_basis = float(position.get('averageLongPrice', 0))
                            market_value = float(position.get('marketValue', 0))
                            
                            # Calculate current price safely
                            current_price = 0
                            if quantity > 0:
                                current_price = market_value / quantity

                            # Get category from user_params or assign default
                            symbol = instrument.get('symbol', '')
                            category = "Unknown"
                            for cat, symbols in user_params.get("categories", {}).items():
                                if symbol in symbols:
                                    category = cat
                                    break

                            # Enhanced holding with new fields
                            holding = {
                                "symbol": symbol,
                                "name": instrument.get('description', symbol),
                                "quantity": quantity,
                                "costBasis": cost_basis,
                                "currentPrice": current_price,
                                "value": market_value,
                                "currency": "USD",
                                # New fields
                                "category": category,
                                "beta": user_params.get("betas", {}).get(symbol, 1.0),
                                "startMarketValue": quantity * cost_basis,
                                "endMarketValue": market_value,
                                "rebalancedShares": quantity,  # Initial value same as current
                                "rebalanceCash": 0.0,  # Will be calculated during rebalancing
                                "valueDifference": market_value - (quantity * cost_basis),
                                "targetWeight": 0.0  # Will be set during optimization
                            }
                            holdings.append(holding)

        # Hash user_id from user_params
        user_id = hashlib.sha256(user_params.get("userId", "").encode('utf-8')).hexdigest()

        # Calculate total portfolio value
        total_value = sum(holding.get("value", 0) for holding in holdings)
        total_investment = sum(holding.get("startMarketValue", 0) for holding in holdings)
        
        # Initialize performance metrics
        performance_metrics = {
            "meanReturn": 0.0,
            "stdDeviation": 0.0,
            "outperformers": [],
            "underperformers": [],
            "zScores": {}
        }

        portfolio_obj = {
            "userId": user_id,
            "name": user_params.get("name", "Schwab Portfolio"),
            "holdings": holdings,
            "totalValue": total_value,
            "totalInvestment": total_investment,
            "startDate": user_params.get("startDate", formatted_time),
            "endDate": formatted_time,
            "policy": userParamsToUserPolicy(user_params),
            "performance": performance_metrics,
            "lastUpdated": formatted_time
        }
        
        return portfolio_obj
        
    except Exception as e:
        logger.exception("Error converting Schwab positions to portfolio: %s", e)
        raise

def userParamsToUserPolicy(user_params):
    """
    Convert user parameters to an enhanced user policy.
    """
    # Extract values with defaults
    user_id = hashlib.sha256(user_params.get("userId", "").encode('utf-8')).hexdigest()
    portfolio_name = user_params.get("name", "Schwab Portfolio")
    
    # Policy-specific parameters with defaults
    policy = {
        "brokerType": 1,  # SCHWAB = 1 in the updated enum
        "investmentHorizon": user_params.get("investmentHorizon", 2),
        "rebalanceFrequency": user_params.get("rebalanceFrequency", "monthly"),
        "riskTolerance": user_params.get("riskTolerance", 0.5),
        "filingStatus": user_params.get("filingStatus", "Single"),
        "annualIncome": user_params.get("annualIncome", 75000.0),
        "equitiesPercent": user_params.get("equitiesPercent", 1.0),
        "categories": user_params.get("categories", {
            "Tech": [],
            "Financials": [],
            "Consumer Goods": [],
            "Energy": [],
            "Entertainment": [],
            "Industrials": []
        }),
        "sectorCaps": user_params.get("sectorCaps", {
            "Tech": 0.35,
            "Financials": 0.20,
            "Consumer Goods": 0.30,
            "Energy": 0.15,
            "Entertainment": 0.15,
            "Industrials": 0.20
        }),
        "targetAllocation": {
            "name": portfolio_name,
            "allocations": user_params.get("allocations", []),
            "lastUpdated": datetime.utcnow().isoformat() + "Z"
        }
    }
    
    return policy

def encrypt_password(password):
    """
    Encrypt a password using Fernet symmetric encryption.
    
    Args:
        password (str): The plain text password to encrypt
        
    Returns:
        str: The encrypted password (base64-encoded) or None if encryption fails
    """
    try:
        # Get encryption key from environment variable or use a default for development
        encryption_key = os.getenv("PASSWORD_ENCRYPTION_KEY", "schwab_encryption_key_must_be_32_bytes!=")
        
        if not encryption_key:
            logging.error("Password encryption key not found in environment variables")
            # For development, use a default key instead of returning None
            encryption_key = "schwab_encryption_key_must_be_32_bytes!="
            
        # Ensure the key is properly formatted for Fernet (32 url-safe base64-encoded bytes)
        if len(encryption_key) != 44 or not encryption_key.endswith('='):
            # Generate a URL-safe base64-encoded 32-byte key from the provided key
            key_bytes = encryption_key.encode('utf-8')
            encryption_key = base64.urlsafe_b64encode(key_bytes.ljust(32)[:32]).decode('utf-8')
        
        # Create a Fernet cipher with the key
        from cryptography.fernet import Fernet
        cipher = Fernet(encryption_key.encode('utf-8'))
        
        # Encrypt the password
        encrypted_password = cipher.encrypt(password.encode('utf-8')).decode('utf-8')
        return encrypted_password
        
    except Exception as e:
        logging.exception("Password encryption failed: %s", e)
        # Return a placeholder value for development instead of None
        return "encrypted_password_placeholder"

def uploadPortfolioToMongo(portfolio_obj):
    """
    Upload the portfolio object to MongoDB using transactions for atomic operations.
    """
    try:
        # Connect to MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        
        # Access the Integrations database and Portfolios collection
        db = client["Integrations"]
        collection = db["Portfolios"]
        
        # Extract identifying fields from the portfolio object
        user_id = portfolio_obj.get("userId", "")
        broker_type = portfolio_obj.get("policy", {}).get("brokerType")
        
        # Start a session for the transaction
        with client.start_session() as session:
            with session.start_transaction():
                # Check if document with same user ID and broker type already exists
                existing_doc = collection.find_one({
                    "userId": user_id,
                    "policy.brokerType": broker_type
                }, session=session)
                
                if existing_doc:
                    # Preserve the original _id
                    portfolio_obj["_id"] = existing_doc["_id"]
                    # Update existing document
                    result = collection.replace_one(
                        {"_id": existing_doc["_id"]},
                        portfolio_obj,
                        session=session
                    )
                    logger.info(f"Portfolio updated in MongoDB with ID: {existing_doc['_id']}")
                    return {"success": True, "message": "Portfolio successfully updated", "id": str(existing_doc["_id"])}
                else:
                    # Insert new portfolio
                    result = collection.insert_one(portfolio_obj, session=session)
                    logger.info("Portfolio successfully uploaded to MongoDB with ID: %s", result.inserted_id)
                    return {"success": True, "message": "Portfolio successfully uploaded", "id": str(result.inserted_id)}
        
    except Exception as e:
        logger.exception("Failed to upload portfolio to MongoDB: %s", e)
        return {"success": False, "error_code": "DB_ERROR", "message": str(e)}

def delivery_report(err, msg):
    """
    Callback for Kafka producer to report message delivery result
    """
    if err is not None:
        logger.error('Message delivery failed: %s', err)
    else:
        logger.info('Message delivered to %s [%s]', msg.topic(), msg.partition())

def build_authorization_url(user_id):
    """
    Build the authorization URL for the OAuth flow.
    
    Args:
        user_id (str): The user ID to include as state parameter
        
    Returns:
        str: The complete authorization URL
    """
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "trade",
        "state": user_id  # Include user_id as state parameter
    }
    auth_url = f"{AUTHORIZATION_URL}?{urllib.parse.urlencode(params)}"
    logger.debug("Built authorization URL: %s", auth_url)
    logger.debug("Redirect URI being used: %s", REDIRECT_URI)
    return auth_url

@app.route('/connect-schwab', methods=['POST'])
def connect_schwab():
    """
    Endpoint to handle Schwab connection requests.
    
    Expects JSON payload with:
    - id_hash: User identifier
    - user_params: Additional user parameters
    
    Returns:
        JSON response with success status and authorization URL
    """
    logging.info("Starting Schwab connection process")
    try:
        # Get data from request body
        data = request.json
        logger.info(f"Received request data: {data}")
        id_hash = data.get("id_hash")
        user_params = data.get("user_params", {})
        
        if not id_hash:
            return jsonify({"success": False, "message": "Missing required id_hash parameter"})
        
        if not user_params:
            return jsonify({"success": False, "message": "Missing required user_params"})
        
        # If userId not provided, use id_hash
        if not user_params.get("userId"):
            user_params["userId"] = id_hash
            
        # Hash the user_id from user_params for storage
        user_id = hashlib.sha256(user_params.get("userId", "").encode('utf-8')).hexdigest()
        logger.info(f"Processing request for user ID: {user_id}")
        
        # Try to get stored tokens for this specific user
        stored_tokens = get_stored_tokens(user_id)
        
        # If we have stored tokens and they're still valid, use them
        if stored_tokens and stored_tokens.get("expiry_time") > datetime.utcnow():
            logger.info(f"Using stored access token for user {user_id}")
            access_token = stored_tokens.get("access_token")
            
            # Get account positions and return portfolio data
            try:
                positions_data = get_account_positions(access_token)
                portfolio_obj = convert_schwab_positions_to_portfolio(positions_data, user_params)
                upload_result = uploadPortfolioToMongo(portfolio_obj)
                
                if not upload_result.get("success", False):
                    return jsonify({"success": False, "message": upload_result.get("message", "Failed to upload portfolio")})
                
                # Publish to Kafka if configured
                if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
                    publish_to_kafka(portfolio_obj)
                
                return jsonify({
                    "success": True,
                    "message": "Portfolio data retrieved successfully",
                    "portfolio_id": upload_result.get("id", ""),
                    "holdings_count": len(portfolio_obj["holdings"]),
                    "total_value": portfolio_obj.get("totalValue", 0)
                })
                
            except Exception as e:
                logger.error(f"Failed to retrieve portfolio data: {e}")
                return jsonify({"success": False, "message": f"Failed to retrieve portfolio data: {str(e)}"})
        
        # If we have a refresh token but access token expired, try to refresh
        elif stored_tokens and stored_tokens.get("refresh_token"):
            logger.info(f"Refreshing access token for user {user_id}")
            try:
                token_data = get_access_token(refresh_token=stored_tokens.get("refresh_token"))
                access_token = token_data["access_token"]
                
                # Store the updated tokens for this user
                store_tokens(
                    user_id, 
                    token_data["access_token"],
                    token_data["refresh_token"],
                    token_data["expires_in"]
                )
                
                # Get account positions and return portfolio data
                positions_data = get_account_positions(access_token)
                portfolio_obj = convert_schwab_positions_to_portfolio(positions_data, user_params)
                upload_result = uploadPortfolioToMongo(portfolio_obj)
                
                if not upload_result.get("success", False):
                    return jsonify({"success": False, "message": upload_result.get("message", "Failed to upload portfolio")})
                
                # Publish to Kafka if configured
                if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
                    publish_to_kafka(portfolio_obj)
                
                return jsonify({
                    "success": True,
                    "message": "Portfolio data retrieved successfully",
                    "portfolio_id": upload_result.get("id", ""),
                    "holdings_count": len(portfolio_obj["holdings"]),
                    "total_value": portfolio_obj.get("totalValue", 0)
                })
                
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                # If refresh fails, we need to do a full OAuth flow
                pass
        
        # If we don't have valid tokens, initiate OAuth flow
        auth_url = build_authorization_url(user_id)  # Pass user_id to the function
        return jsonify({
            "success": True,
            "message": "Authorization required",
            "auth_url": auth_url,
            "user_id": user_id
        })
        
    except Exception as e:
        logging.exception("An error occurred during Schwab connection: %s", e)
        return jsonify({"success": False, "message": f"Error: {str(e)}"})

@app.route('/', methods=['GET'])
def schwab_callback():
    """
    Endpoint to handle the OAuth callback from Schwab.
    
    Expects query parameters:
    - code: The authorization code
    - state: Optional state parameter for security
    
    Returns:
        HTML page with success message and close button
    """
    try:
        # Get the authorization code
        code = request.args.get('code')
        if not code:
            return jsonify({"success": False, "message": "No authorization code provided"})
        
        # Get user_id from state parameter
        user_id = request.args.get('state')
        if not user_id:
            return jsonify({"success": False, "message": "No user ID provided"})
        
        # Exchange code for tokens
        try:
            token_data = get_access_token(code=code)
            access_token = token_data["access_token"]
            
            # Store the tokens
            store_tokens(
                user_id,
                token_data["access_token"],
                token_data["refresh_token"],
                token_data["expires_in"]
            )
            
            # Get account positions
            positions_data = get_account_positions(access_token)
            portfolio_obj = convert_schwab_positions_to_portfolio(positions_data, {"userId": user_id})
            
            # Upload to MongoDB
            upload_result = uploadPortfolioToMongo(portfolio_obj)
            if not upload_result.get("success", False):
                return jsonify({"success": False, "message": upload_result.get("message", "Failed to upload portfolio")})
            
            # Publish to Kafka if configured
            if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
                publish_to_kafka(portfolio_obj)
            
            # Return success HTML page
            return '''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Schwab Connection Successful</title>
                <style>
                    body {
                        font-family: Arial, sans-serif;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        height: 100vh;
                        margin: 0;
                        background-color: #0a0b1e;
                        color: #fff;
                    }
                    .container {
                        text-align: center;
                        padding: 2rem;
                        background-color: #151633;
                        border-radius: 12px;
                        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
                        max-width: 400px;
                    }
                    .success-icon {
                        width: 48px;
                        height: 48px;
                        margin: 0 auto 1rem auto;
                    }
                    .success-icon svg {
                        width: 100%;
                        height: 100%;
                    }
                    .success-icon svg path {
                        fill: #a855f7;
                    }
                    h1 {
                        color: #fff;
                        margin-bottom: 1rem;
                        font-weight: 500;
                    }
                    p {
                        color: #a9a9c7;
                        margin-bottom: 2rem;
                        line-height: 1.5;
                    }
                    button {
                        background-color: #a855f7;
                        color: white;
                        border: none;
                        padding: 12px 24px;
                        border-radius: 8px;
                        cursor: pointer;
                        font-size: 16px;
                        transition: all 0.2s ease;
                    }
                    button:hover {
                        background-color: #9333ea;
                        transform: translateY(-1px);
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="success-icon">
                        <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                            <path d="M20.285 2l-11.285 11.567-5.286-5.011-3.714 3.716 9 8.728 15-15.285z"/>
                        </svg>
                    </div>
                    <h1>Connection Successful!</h1>
                    <p>Your Schwab account has been successfully connected. You can now close this window and return to the application.</p>
                    <button onclick="window.close()">Close Window</button>
                </div>
            </body>
            </html>
            '''
            
        except Exception as e:
            logger.error(f"Failed to exchange code for token: {e}")
            return jsonify({"success": False, "message": f"Failed to obtain access token: {str(e)}"})
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        return jsonify({"success": False, "message": f"Callback error: {str(e)}"})

def publish_to_kafka(portfolio_obj):
    """Helper function to publish portfolio data to Kafka"""
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_key = os.getenv("KAFKA_KEY")
        kafka_secret = os.getenv("KAFKA_SECRET")
        
        if kafka_bootstrap_servers and kafka_key and kafka_secret:
            config = {
                'bootstrap.servers': kafka_bootstrap_servers,
                'sasl.username': kafka_key,
                'sasl.password': kafka_secret,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'acks': 'all'
            }
            
            producer = Producer(config)
            portfolio_payload = json.dumps(portfolio_obj, cls=MongoJSONEncoder).encode('utf-8')
            producer.produce("alert_rebalance", portfolio_payload, callback=delivery_report)
            producer.flush()
            logger.info("Portfolio data sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to publish to Kafka: {e}")

if __name__ == "__main__":
    # Create SSL context
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        ssl_context.load_cert_chain('server.crt', 'server.key')
        logger.info("SSL certificates loaded successfully")
    except FileNotFoundError:
        logger.error("SSL certificates not found. Generating self-signed certificates...")
        import subprocess
        subprocess.run([
            'openssl', 'req', '-x509', '-newkey', 'rsa:4096', '-nodes',
            '-out', 'server.crt', '-keyout', 'server.key',
            '-days', '365', '-subj', '/CN=schwab.fineasapp.io'
        ], check=True)
        ssl_context.load_cert_chain('server.crt', 'server.key')
    
    # Run the Flask app with SSL
    app.run(
        debug=True,
        host='0.0.0.0',
        port=5003,
        ssl_context=ssl_context
    )
