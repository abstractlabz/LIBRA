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

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# OAuth configuration – update these as needed
CLIENT_ID = os.environ["SCHWAB_CLIENT_ID"]
CLIENT_SECRET = os.environ["SCHWAB_CLIENT_SECRET"]
REDIRECT_URI = os.environ["SCHWAB_REDIRECT_URI"]
AUTHORIZATION_URL = os.environ["SCHWAB_AUTHORIZATION_URL"]
TOKEN_URL = os.environ["SCHWAB_TOKEN_URL"]
ACCOUNTS_URL = os.environ["SCHWAB_ACCOUNTS_URL"]

# Global variables - used only for OAuth flow within a single request
auth_code = None
callback_url = None
# Removed the global access_token variable to prevent token reuse across requests

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
        refresh_token (str, optional): The refresh token
        
    Returns:
        dict: Containing access_token, refresh_token, and expires_in
    """
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
            "code": code+'@',  # Add @ as required by Schwab
            "redirect_uri": REDIRECT_URI
        }
    elif refresh_token:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    else:
        raise ValueError("Either code or refresh_token must be provided")
    
    # Make the token request
    logger.debug(f"Sending token request with data: {data}")
    response = requests.post(TOKEN_URL, headers=headers, data=data, verify=False)
    
    if response.status_code != 200:
        logger.error(f"Error fetching token: {response.status_code} - {response.text}")
        response.raise_for_status()
    
    token_data = response.json()
    return {
        "access_token": token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "expires_in": token_data.get("expires_in", 1800)  # Default to 30 minutes
    }

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
    Convert Schwab positions data to a portfolio format
    
    Args:
        schwab_data (list): List of account data from Schwab API
        user_params (dict): User parameters including userId, etc.
        
    Returns:
        dict: Portfolio object in the required format
    """
    holdings = []
    
    try:
        # Process each account in the response
        for account in schwab_data:
            if 'securitiesAccount' in account:
                account_data = account['securitiesAccount']
                
                # Extract positions from the account
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
                            
                            holding = {
                                "symbol": instrument.get('symbol', ''),
                                "name": instrument.get('description', instrument.get('symbol', '')),
                                "quantity": quantity,
                                "costBasis": cost_basis,
                                "currentPrice": current_price,
                                "value": market_value,
                                "currency": "USD"
                            }
                            holdings.append(holding)
        
        # Hash user_id from user_params
        user_id = hashlib.sha256(user_params.get("userId", "").encode('utf-8')).hexdigest()

        # Calculate total portfolio value
        total_value = sum(holding.get("value", 0) for holding in holdings)
        
        # Add percentage of portfolio to each holding
        for holding in holdings:
            if total_value > 0:
                holding["percentage"] = (holding.get("value", 0) / total_value) * 100
            else:
                holding["percentage"] = 0

        portfolio_obj = {
            "userId": user_id,
            "name": user_params.get("name", "Schwab Portfolio"),
            "holdings": holdings,
            "totalValue": total_value,
            "policy": userParamsToUserPolicy(user_params),
            "lastUpdated": datetime.utcnow().isoformat() + "Z"
        }
        
        return portfolio_obj
        
    except Exception as e:
        logger.exception("Error converting Schwab positions to portfolio: %s", e)
        raise

def userParamsToUserPolicy(user_params):
    """
    Convert user parameters to a user policy.
    
    Args:
        user_params (dict): Dictionary containing user parameters
        
    Returns:
        dict: A structured policy object for portfolio management
    """
    # Extract values with defaults
    user_id = hashlib.sha256(user_params.get("userId", "").encode('utf-8')).hexdigest()
    portfolio_name = user_params.get("name"+str(random.randint(0, 100)), "Schwab Portfolio")
    
    # Extract policy-specific parameters with defaults
    broker_type = 3  # Schwab broker type
    investment_horizon = user_params.get("investmentHorizon", 2)
    rebalance_frequency = user_params.get("rebalanceFrequency", "monthly")
    risk_tolerance = user_params.get("riskTolerance", 0.5)
    
    # Process target allocations if provided, or use empty array
    allocations = user_params.get("allocations", [])
    if not isinstance(allocations, list):
        allocations = []
    
    # Format the allocations properly
    formatted_allocations = []
    for alloc in allocations:
        if isinstance(alloc, dict) and "symbol" in alloc and "targetWeight" in alloc:
            formatted_allocations.append({
                "symbol": alloc["symbol"],
                "targetWeight": {"$numberDouble": str(alloc["targetWeight"])}
            })
    
    # Create the policy object
    policy = {
        "brokerType": {"$numberInt": str(broker_type)},
        "investmentHorizon": {"$numberInt": str(investment_horizon)},
        "rebalanceFrequency": rebalance_frequency,
        "riskTolerance": {"$numberDouble": str(risk_tolerance)},
        "targetAllocation": {
            "allocations": formatted_allocations,
            "lastUpdated": datetime.utcnow().isoformat() + "Z",
            "name": portfolio_name,
        },
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
    Upload the portfolio object to MongoDB.
    
    Args:
        portfolio_obj (dict): Portfolio object to be uploaded
        
    Returns:
        dict: Response containing success status and message
    """
    try:
        # Connect to MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        
        # Access the Integrations database and Portfolios collection
        db = client["Integrations"]
        collection = db["Portfolios"]
        
        # Extract identifying fields from the portfolio object
        user_id = portfolio_obj.get("userId", "")
        broker_type = portfolio_obj.get("policy", {}).get("brokerType", {}).get("$numberInt", "")
        
        # Check if document with same user ID and broker type already exists
        existing_doc = collection.find_one({
            "userId": user_id,
            "policy.brokerType.$numberInt": broker_type
        })
        
        if existing_doc:
            # Update existing document instead of returning an error
            result = collection.replace_one(
                {"_id": existing_doc["_id"]},
                portfolio_obj
            )
            logger.info(f"Portfolio updated in MongoDB with ID: {existing_doc['_id']}")
            return {"success": True, "message": "Portfolio successfully updated", "id": str(existing_doc["_id"])}
        else:
            # Insert the portfolio if no duplicate exists
            result = collection.insert_one(portfolio_obj)
            logger.info("Portfolio successfully uploaded to MongoDB with ID: %s", result.inserted_id)
            # Convert ObjectId to string explicitly
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

class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code, callback_url
        
        # Store the full request path
        logger.info(f"Received request: {self.path}")
        callback_url = f"@https://127.0.0.1:8050{self.path}"
        logger.info(f"Stored callback URL: {callback_url}")
        
        try:
            # Parse the URL to get the query parameters
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            logger.debug(f"Query parameters: {query_params}")
            
            # Extract code parameter
            if 'code' in query_params:
                raw_code = query_params['code'][0]
                # URL decode and clean up
                auth_code = unquote(raw_code).rstrip('@')
                logger.info(f"Extracted and stored auth code: {auth_code}")
                
                # Send success response
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authorization Successful</h1><p>You can close this window.</p></body></html>")
            else:
                logger.warning("No code parameter found in request")
                self.send_response(400)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Error: No code parameter found</h1></body></html>")
        
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Server Error</h1></body></html>")

def start_server(port=8050):
    try:
        server_address = ('127.0.0.1', port)
        httpd = HTTPServer(server_address, OAuthHandler)
        
        # Configure SSL with more detailed error handling
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_context.load_cert_chain('server.crt', 'server.key')
            logger.info("SSL certificates loaded successfully")
        except FileNotFoundError:
            logger.error("SSL certificates not found. Please generate them using:")
            logger.error("openssl req -x509 -newkey rsa:4096 -nodes -out server.crt -keyout server.key -days 365 -subj '/CN=localhost'")
            raise
        except ssl.SSLError as ssl_err:
            logger.error(f"SSL Error: {ssl_err}")
            raise
        
        # Wrap the socket with SSL
        httpd.socket = ssl_context.wrap_socket(httpd.socket, server_side=True)
        logger.info(f"HTTPS Server started successfully on https://127.0.0.1:{port}")
        return httpd
    
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        raise

def wait_for_callback(httpd, timeout_seconds=300):
    global auth_code, callback_url
    
    logger.info("Waiting for authorization callback...")
    logger.info(f"You have {timeout_seconds} seconds to complete the authorization in your browser")
    
    # Set a timeout for the server socket so handle_request() doesn't block forever
    httpd.timeout = 1  # Check every second
    
    # Track the start time
    start_time = time.time()
    
    # Continue trying to handle a request until we get a code or time out
    while auth_code is None:
        # Check for timeout
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            logger.info(f"Timeout after {int(elapsed_time)} seconds")
            return False
        
        # Print a waiting message every 10 seconds
        if int(elapsed_time) % 10 == 0 and int(elapsed_time) > 0:
            logger.info(f"Still waiting for callback... ({int(elapsed_time)} seconds elapsed)")
        
        try:
            # This will wait for up to 1 second for a request
            httpd.handle_request()
            
            # If we got a code, we're done
            if auth_code is not None:
                logger.info("Authorization code received!")
                logger.info(f"Code: {auth_code}")
                logger.info(f"Full callback URL: {callback_url}")
                return True
            
        except Exception as e:
            logger.error(f"Error handling request: {e}")
    
    # This should only happen if the auth_code was set by some other means
    return auth_code is not None

def build_authorization_url():
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "trade"
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
        JSON response with success status and message
    """
    global auth_code
    auth_code = None  # Reset for each request
    
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
        
        # Access token for this user only
        access_token = None
        
        # Try to get stored tokens for this specific user
        stored_tokens = get_stored_tokens(user_id)
        
        # If we have stored tokens and they're still valid, use them
        if stored_tokens and stored_tokens.get("expiry_time") > datetime.utcnow():
            logger.info(f"Using stored access token for user {user_id}")
            access_token = stored_tokens.get("access_token")
        
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
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                # If refresh fails, we need to do a full OAuth flow
                access_token = None
        
        # If we don't have valid tokens, do the full OAuth flow
        if not access_token:
            # Start the OAuth flow
            httpd = None
            try:
                # Start the server
                httpd = start_server(8050)
                # Build and open the authorization URL
                auth_url = build_authorization_url()
                logger.info("Please authorize the application by visiting the following URL:")
                logger.info(auth_url)
                # Open the browser for the user
                webbrowser.open(auth_url)
                logger.info("Opening your browser...")
                
                # Wait for the callback with a timeout
                success = wait_for_callback(httpd, timeout_seconds=300)
                if not success:
                    return jsonify({"success": False, "message": "Authorization failed - no callback received"})
                
                # Exchange authorization code for access token
                logger.debug("Exchanging authorization code for access token...")
                try:
                    token_data = get_access_token(code=auth_code)
                    access_token = token_data["access_token"]
                    
                    # Store the tokens for this specific user
                    store_tokens(
                        user_id,
                        token_data["access_token"],
                        token_data["refresh_token"],
                        token_data["expires_in"]
                    )
                except Exception as e:
                    logger.error(f"Failed to exchange code for token: {e}")
                    return jsonify({"success": False, "message": f"Failed to obtain access token: {str(e)}"})
            finally:
                # Shutdown the server
                if httpd:
                    httpd.server_close()
                    logger.info("Server shut down")
        
        # With access token ready, now get the account positions for this user
        try:
            logger.info("Retrieving account positions...")
            positions_data = get_account_positions(access_token)
            logger.info(f"Account positions received: {len(positions_data)} accounts")
            
            # Convert positions to portfolio format
            portfolio_obj = convert_schwab_positions_to_portfolio(positions_data, user_params)
            logger.info(f"Converted to portfolio format with {len(portfolio_obj['holdings'])} holdings")
            
            # Upload to MongoDB
            upload_result = uploadPortfolioToMongo(portfolio_obj)
            if not upload_result.get("success", False):
                return jsonify({"success": False, "message": upload_result.get("message", "Failed to upload portfolio")})
            
            # Publish to Kafka
            kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
            kafka_key = os.getenv("KAFKA_KEY")
            kafka_secret = os.getenv("KAFKA_SECRET")
            
            if kafka_bootstrap_servers and kafka_key and kafka_secret:
                # Kafka Producer configuration
                config = {
                    'bootstrap.servers': kafka_bootstrap_servers,
                    'sasl.username': kafka_key,
                    'sasl.password': kafka_secret,
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'PLAIN',
                    'acks': 'all'
                }
                
                # Create Kafka Producer
                producer = Producer(config)
                
                # Produce message to topic - use custom JSON encoder for serialization
                portfolio_payload = json.dumps(portfolio_obj, cls=MongoJSONEncoder).encode('utf-8')
                producer.produce("alert_rebalance", portfolio_payload, callback=delivery_report)
                producer.flush()
                logger.info("Portfolio data sent to Kafka")
            
            # Use a serializable ID (string)
            portfolio_id = upload_result.get("id", "")
            if isinstance(portfolio_id, ObjectId):
                portfolio_id = str(portfolio_id)
            
            return jsonify({
                "success": True, 
                "message": "Schwab connection successful",
                "portfolio_id": portfolio_id,
                "holdings_count": len(portfolio_obj["holdings"]),
                "total_value": portfolio_obj.get("totalValue", 0)
            })
        except Exception as e:
            logger.error(f"Failed to retrieve or process account positions: {e}")
            return jsonify({"success": False, "message": f"Failed to retrieve account data: {str(e)}"})
    except Exception as e:
        logging.exception("An error occurred during Schwab connection: %s", e)
        return jsonify({"success": False, "message": f"Error: {str(e)}"})

if __name__ == '__main__':
    app.run(debug=True, port=5001)
