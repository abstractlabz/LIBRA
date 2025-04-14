import os
import json
import threading
import datetime
import logging
import time
import ssl
import base64
import urllib.parse
import webbrowser
import signal
import sys
import hashlib
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
import requests
from pymongo import MongoClient
from confluent_kafka import Consumer, Producer
from datetime import datetime, timedelta
from bson import ObjectId
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# OAuth configuration from environment
def get_config():

    # OAuth configuration – update these as needed
    CLIENT_ID = os.environ["SCHWAB_CLIENT_ID"] 
    CLIENT_SECRET = os.environ["SCHWAB_CLIENT_SECRET"]
    REDIRECT_URI = os.environ["SCHWAB_REDIRECT_URI"] 
    AUTHORIZATION_URL = os.environ["SCHWAB_AUTHORIZATION_URL"]
    TOKEN_URL = os.environ["SCHWAB_TOKEN_URL"]
    ACCOUNTS_URL = os.environ["SCHWAB_ACCOUNTS_URL"]

    return {
        "CLIENT_ID": CLIENT_ID, 
        "CLIENT_SECRET": CLIENT_SECRET,
        "REDIRECT_URI": REDIRECT_URI,
        "AUTHORIZATION_URL": AUTHORIZATION_URL,
        "TOKEN_URL": TOKEN_URL,
        "ACCOUNTS_URL": ACCOUNTS_URL
    }

# Global variables for OAuth flow
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

def delivery_report(err, msg):
    """
    Callback for Kafka Producer's produce method.
    Reports whether the message delivery was successful or not.
    """
    if err is not None:
        logger.error("Delivery report: Message delivery failed: %s", err)
    else:
        logger.info("Delivery report: Message delivered to %s partition [%d] at offset %d",
                     msg.topic(), msg.partition(), msg.offset())

def get_mongo_client():
    """Get MongoDB client"""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return MongoClient(mongo_uri)

# Token management functions
def get_stored_tokens(user_id):
    """Retrieve stored tokens for a user from MongoDB"""
    try:
        client = get_mongo_client()
        db = client["Integrations"]
        tokens_collection = db["SchwabTokens"]
        
        token_doc = tokens_collection.find_one({"user_id": user_id})
        return token_doc
    except Exception as e:
        logger.error(f"Error retrieving stored tokens: {e}")
        return None

def store_tokens(user_id, access_token, refresh_token, expires_in):
    """Store tokens in MongoDB for future use"""
    try:
        client = get_mongo_client()
        db = client["Integrations"]
        tokens_collection = db["SchwabTokens"]
        
        expiry_time = datetime.utcnow() + timedelta(seconds=expires_in)
        
        token_doc = {
            "user_id": user_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expiry_time": expiry_time
        }
        
        tokens_collection.update_one(
            {"user_id": user_id},
            {"$set": token_doc},
            upsert=True
        )
        logger.info(f"Tokens stored for user {user_id}")
    except Exception as e:
        logger.error(f"Error storing tokens: {e}")

def get_access_token(code=None, refresh_token=None):
    """Get an access token either using an authorization code or a refresh token"""
    config = get_config()
    credentials = f"{config['CLIENT_ID']}:{config['CLIENT_SECRET']}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    if code:
        data = {
            "grant_type": "authorization_code",
            "code": code+'@',  # Add @ as required by Schwab
            "redirect_uri": config['REDIRECT_URI']
        }
    elif refresh_token:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    else:
        raise ValueError("Either code or refresh_token must be provided")
    
    logger.debug(f"Sending token request with data: {data}")
    response = requests.post(config['TOKEN_URL'], headers=headers, data=data, verify=False)
    
    if response.status_code != 200:
        logger.error(f"Error fetching token: {response.status_code} - {response.text}")
        response.raise_for_status()
    
    token_data = response.json()
    return {
        "access_token": token_data.get("access_token"),
        "refresh_token": token_data.get("refresh_token"),
        "expires_in": token_data.get("expires_in", 1800)
    }

def get_account_positions(access_token):
    """Retrieve account positions from Schwab API"""
    config = get_config()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    params = {
        "fields": "positions"
    }
    response = requests.get(config['ACCOUNTS_URL'], headers=headers, params=params, verify=False)
    
    if response.status_code != 200:
        logger.error(f"Error fetching account positions: {response.status_code} - {response.text}")
        response.raise_for_status()
    
    return response.json()

def convert_schwab_positions_to_portfolio(schwab_data, base_portfolio):
    """
    Convert Schwab positions data to a portfolio format
    
    Args:
        schwab_data (list): List of account data from Schwab API
        base_portfolio (dict): Original portfolio data with user info
        
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

        # Calculate total portfolio value
        total_value = sum(holding.get("value", 0) for holding in holdings)
        
        # Add percentage of portfolio to each holding
        for holding in holdings:
            if total_value > 0:
                holding["percentage"] = (holding.get("value", 0) / total_value) * 100
            else:
                holding["percentage"] = 0

        portfolio_obj = {
            "userId": base_portfolio.get("userId", ""),
            "name": base_portfolio.get("name", "Schwab Portfolio"),
            "holdings": holdings,
            "totalValue": total_value,
            "policy": base_portfolio.get("policy", {}),
            "lastUpdated": datetime.utcnow().isoformat() + "Z"
        }
        
        return portfolio_obj
        
    except Exception as e:
        logger.exception(f"Error converting Schwab positions to portfolio: {e}")
        raise

# OAuth server components
class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code, callback_url
        
        logger.info(f"Received request: {self.path}")
        callback_url = f"@https://127.0.0.1:5003{self.path}"
        
        try:
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            
            if 'code' in query_params:
                raw_code = query_params['code'][0]
                auth_code = unquote(raw_code).rstrip('@')
                logger.info(f"Extracted auth code: {auth_code}")
                
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authorization Successful</h1><p>You can close this window.</p></body></html>")
            else:
                logger.warning("No code parameter found in request")
                self.send_response(400)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Error: Authorization code not found</h1></body></html>")
        
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Server Error</h1></body></html>")
    
    # Suppress server logs to avoid cluttering the output
    def log_message(self, format, *args):
        return

def start_server(port=5003):
    """Start the OAuth callback server"""
    try:
        server_address = ('127.0.0.1', port)
        httpd = HTTPServer(server_address, OAuthHandler)
        
        # Configure SSL
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_context.load_cert_chain('server.crt', 'server.key')
            logger.info("SSL certificates loaded successfully")
        except FileNotFoundError:
            logger.error("SSL certificates not found. Please generate them using:")
            logger.error("openssl req -x509 -newkey rsa:4096 -nodes -out server.crt -keyout server.key -days 365 -subj '/CN=localhost'")
            raise
        
        httpd.socket = ssl_context.wrap_socket(httpd.socket, server_side=True)
        logger.info(f"HTTPS Server started on https://127.0.0.1:{port}")
        return httpd
    
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        raise

def wait_for_callback(httpd, timeout_seconds=300):
    """Wait for the OAuth callback with timeout"""
    global auth_code
    
    logger.info("Waiting for authorization callback...")
    logger.info(f"You have {timeout_seconds} seconds to complete the authorization")
    
    httpd.timeout = 1  # Check every second
    start_time = time.time()
    
    while auth_code is None:
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            logger.warning(f"Timeout after {int(elapsed_time)} seconds")
            return False
        
        if int(elapsed_time) % 10 == 0 and int(elapsed_time) > 0:
            logger.info(f"Still waiting... ({int(elapsed_time)} seconds elapsed)")
        
        try:
            httpd.handle_request()
            if auth_code is not None:
                logger.info("Authorization code received!")
                return True
            
        except Exception as e:
            logger.error(f"Error handling request: {e}")
    
    return auth_code is not None

def build_authorization_url():
    """Build the OAuth authorization URL"""
    config = get_config()
    params = {
        "client_id": config['CLIENT_ID'],
        "redirect_uri": config['REDIRECT_URI'],
        "response_type": "code",
        "scope": "trade"
    }
    return f"{config['AUTHORIZATION_URL']}?{urllib.parse.urlencode(params)}"

def upload_portfolio_to_mongo(portfolio_obj):
    """Upload the portfolio object to MongoDB"""
    try:
        client = get_mongo_client()
        db = client["Integrations"]
        collection = db["Portfolios"]
        
        # Extract identifying fields
        user_id = portfolio_obj.get("userId", "")
        
        # Handle broker type differently now that it's a direct integer
        broker_type = portfolio_obj.get("policy", {}).get("brokerType")
        
        # Check if document already exists
        existing_doc = collection.find_one({
            "userId": user_id,
            "policy.brokerType": broker_type  # Updated query to match new format
        })
        
        if existing_doc:
            # Update existing document
            result = collection.replace_one(
                {"_id": existing_doc["_id"]},
                portfolio_obj
            )
            logger.info(f"Portfolio updated in MongoDB with ID: {existing_doc['_id']}")
            return {"success": True, "message": "Portfolio successfully updated", "id": str(existing_doc["_id"])}
        else:
            # Insert new document
            result = collection.insert_one(portfolio_obj)
            logger.info(f"Portfolio inserted into MongoDB with ID: {result.inserted_id}")
            return {"success": True, "message": "Portfolio successfully uploaded", "id": str(result.inserted_id)}
        
    except Exception as e:
        logger.exception(f"Failed to upload portfolio to MongoDB: {e}")
        return {"success": False, "error_code": "DB_ERROR", "message": str(e)}

def run_schwab_operations(message):
    """
    Process an incoming message to authenticate with Schwab, retrieve positions data,
    convert it into a Portfolio-like structure, and produce a message with that data
    to the 'alert_rebalance' Kafka topic.
    """
    global auth_code
    auth_code = None  # Reset the auth code for each request
    
    logger.info("Starting Schwab operations")
    try:
        # Parse the incoming message
        portfolio = json.loads(message)
        logger.info("Portfolio JSON parsed successfully")
        
        # Extract ID
        user_id = portfolio.get("userId")
        
        if not user_id:
            logger.error("User ID is missing in the portfolio")
            return
            
        logger.info(f"Processing request for user: {user_id}")
        
        # Get access token (from storage or refresh only - no new OAuth flow)
        access_token = None
        
        # Try to get stored tokens - this is the primary method
        stored_tokens = get_stored_tokens(user_id)
        
        # If we have stored tokens and they're still valid, use them
        if stored_tokens and stored_tokens.get("expiry_time") > datetime.utcnow():
            logger.info(f"Using stored access token for user {user_id}")
            access_token = stored_tokens.get("access_token")
        
        # If tokens expired but we have refresh token, try to refresh
        elif stored_tokens and stored_tokens.get("refresh_token"):
            logger.info(f"Refreshing access token for user {user_id}")
            try:
                token_data = get_access_token(refresh_token=stored_tokens.get("refresh_token"))
                access_token = token_data["access_token"]
                
                # Store updated tokens
                store_tokens(
                    user_id, 
                    token_data["access_token"],
                    token_data["refresh_token"],
                    token_data["expires_in"]
                )
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                logger.error("Authentication failed - no valid tokens and refresh failed")
                # Send a message to another topic to notify about auth failure if needed
                send_auth_failure_notification(user_id)
                return
        
        # If we still don't have a token, gracefully handle the error
        if not access_token:
            logger.error("No valid authentication tokens found for user")
            logger.error("Please ensure user has authorized the application previously")
            # Send a message to another topic to notify about auth failure if needed
            send_auth_failure_notification(user_id)
            return
        
        # Proceed with getting account data and creating portfolio
        logger.info("Retrieving account positions...")
        try:
            positions_data = get_account_positions(access_token)
            logger.info(f"Account positions received: {len(positions_data)} accounts")
            
            # Convert to portfolio format
            portfolio_obj = convert_schwab_positions_to_portfolio(positions_data, portfolio)
            logger.info(f"Portfolio created with {len(portfolio_obj['holdings'])} holdings")
            
            # Upload to MongoDB
            upload_result = upload_portfolio_to_mongo(portfolio_obj)
            if not upload_result.get("success", False):
                logger.error(f"Failed to upload portfolio: {upload_result.get('message')}")
                return
            
            # Publish to Kafka alert_rebalance topic
            publish_portfolio_to_kafka(portfolio_obj)
            
        except Exception as e:
            logger.exception(f"Failed to process account data: {e}")
            return
    
    except Exception as e:
        logger.exception(f"An error occurred during Schwab operations: {e}")
        return

def send_auth_failure_notification(user_id):
    """
    Send a notification about authentication failure to a Kafka topic
    """
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_key = os.getenv("KAFKA_KEY")
        kafka_secret = os.getenv("KAFKA_SECRET")
        
        if not all([kafka_bootstrap_servers, kafka_key, kafka_secret]):
            logger.warning("Kafka environment variables not set, skipping notification")
            return
            
        # Kafka Producer configuration
        config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'sasl.username': kafka_key,
            'sasl.password': kafka_secret,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'acks': 'all'
        }
        
        producer = Producer(config)
        
        # Create notification message
        notification = {
            "userId": user_id,
            "service": "schwab",
            "event": "auth_failure",
            "message": "Authentication failed - no valid tokens available",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        # Serialize and send
        payload = json.dumps(notification).encode('utf-8')
        producer.produce("auth_notifications", payload, callback=delivery_report)
        producer.flush()
        
        logger.info(f"Auth failure notification sent for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to send auth failure notification: {e}")

def publish_portfolio_to_kafka(portfolio_obj):
    """
    Publish portfolio data to the alert_rebalance Kafka topic
    """
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_key = os.getenv("KAFKA_KEY")
        kafka_secret = os.getenv("KAFKA_SECRET")
        
        if not all([kafka_bootstrap_servers, kafka_key, kafka_secret]):
            logger.warning("Kafka environment variables not set, skipping message publishing")
            return False
            
        # Kafka Producer configuration
        config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'sasl.username': kafka_key,
            'sasl.password': kafka_secret,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'acks': 'all'
        }
        
        producer = Producer(config)
        
        # Serialize the portfolio object to JSON and encode it as bytes
        portfolio_payload = json.dumps(portfolio_obj, cls=MongoJSONEncoder).encode('utf-8')
        
        # Send the portfolio data as a message to the "alert_rebalance" topic
        producer.produce("alert_rebalance", portfolio_payload, callback=delivery_report)
        logger.info("Produced message to 'alert_rebalance' topic. Flushing producer...")
        producer.flush()
        logger.info("Kafka producer flush complete")
        return True
        
    except Exception as e:
        logger.exception(f"Failed to publish portfolio to Kafka: {e}")
        return False

def listen_to_kafka(kafka_bootstrap_servers, kafka_key, kafka_secret):
    """
    Set up a Kafka consumer that listens to the 'alert_schwab' topic.
    When a message is received, spawn a thread to handle Schwab operations.
    """
    logger.info("Starting Kafka consumer for topic 'alert_schwab'")
    config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'sasl.username': kafka_key,
        'sasl.password': kafka_secret,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'group.id': 'schwab-group',
        'auto.offset.reset': 'earliest'
    }

    # Create Kafka Consumer instance
    consumer = Consumer(config)
    consumer.subscribe(['alert_schwab'])
    logger.info("Kafka consumer subscribed to 'alert_schwab' topic")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        payload = msg.value()  # Get the actual message payload
        logger.info("Received Kafka message")
        # Run the Schwab operations in a separate thread
        thread = threading.Thread(target=run_schwab_operations, args=(payload,))
        thread.start()

if __name__ == "__main__":
    # Load environment variables from the .env file
    load_dotenv()

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_key = os.getenv("KAFKA_KEY")
    kafka_secret = os.getenv("KAFKA_SECRET")

    if not kafka_bootstrap_servers or not kafka_key or not kafka_secret:
        logger.error("Kafka configuration environment variables are missing")
        exit(1)

    logger.info("Kafka environment variables loaded successfully")
    listen_to_kafka(kafka_bootstrap_servers, kafka_key, kafka_secret)
