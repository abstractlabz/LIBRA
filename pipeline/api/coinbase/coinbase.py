import logging
import threading
import time
import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests
from pymongo import MongoClient
from confluent_kafka import Consumer, Producer
from datetime import datetime, timedelta
from bson import ObjectId
from dotenv import load_dotenv
import hashlib

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                   format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# OAuth configuration from environment
def get_config():
    return {
        "CLIENT_ID": os.environ["COINBASE_OAUTH_CLIENT_ID"],
        "CLIENT_SECRET": os.environ["COINBASE_OAUTH_SECRET"],
        "REDIRECT_URI": os.environ["COINBASE_REDIRECT_URI"],
        "AUTH_URL": "https://www.coinbase.com/oauth/authorize",
        "TOKEN_URL": "https://api.coinbase.com/oauth/token",
        "ACCOUNTS_URL": "https://api.coinbase.com/v2/accounts"
    }

class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(MongoJSONEncoder, self).default(obj)

def get_mongo_client():
    """Get MongoDB client"""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return MongoClient(mongo_uri)

def get_stored_tokens(user_id):
    """Retrieve stored tokens for a user from MongoDB"""
    try:
        client = get_mongo_client()
        db = client["Integrations"]
        tokens_collection = db["CoinbaseTokens"]
        
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
        tokens_collection = db["CoinbaseTokens"]
        
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

def refresh_access_token(refresh_token):
    """Refresh the access token using the refresh token"""
    config = get_config()
    
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": config["CLIENT_ID"],
        "client_secret": config["CLIENT_SECRET"]
    }
    
    response = requests.post(config["TOKEN_URL"], data=data)
    if response.status_code != 200:
        raise Exception(f"Token refresh failed: {response.text}")
    
    token_data = response.json()
    return {
        "access_token": token_data["access_token"],
        "refresh_token": token_data["refresh_token"],
        "expires_in": token_data.get("expires_in", 7200)  # Coinbase default is 2 hours
    }

def get_account_positions(access_token):
    """Retrieve account positions from Coinbase API"""
    config = get_config()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    response = requests.get(config["ACCOUNTS_URL"], headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch account data: {response.text}")
    
    return response.json()

def fetch_crypto_price(symbol):
    """Fetch current price of a cryptocurrency from Polygon.io"""
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    if not polygon_api_key:
        logger.error("POLYGON_API_KEY not set")
        return 1.0  # Default fallback price
        
    # Format symbol for Polygon.io (e.g., BTC -> X:BTCUSD)
    formatted_symbol = f"X:{symbol}USD"
    
    # Get current date in YYYY-MM-DD format
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Construct Polygon.io API URL
    url = f"https://api.polygon.io/v2/aggs/ticker/{formatted_symbol}/range/1/day/{current_date}/{current_date}?apiKey={polygon_api_key}"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Error response from Polygon.io: {response.status_code}")
            return 1.0
            
        data = response.json()
        if not data.get("results"):
            logger.error(f"No price data available for {symbol}")
            return 1.0
            
        return data["results"][0]["c"]  # Return closing price
    except Exception as e:
        logger.error(f"Error fetching price for {symbol}: {e}")
        return 1.0  # Default fallback price

def convert_coinbase_positions_to_portfolio(coinbase_data, user_params):
    """Convert Coinbase positions data to our portfolio format with enhanced fields"""
    holdings = []
    total_value = 0
    total_investment = 0
    current_time = datetime.utcnow()
    formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    try:
        # Use the user ID directly from parameters without hashing
        user_id = user_params.get("userId", "")
        if not user_id:
            raise ValueError("User ID is required")
            
        for account in coinbase_data["data"]:
            amount = float(account["balance"]["amount"])
            if amount > 0:
                currency_info = account["currency"]
                symbol = currency_info["code"]
                
                # Get category from user_params or assign default
                category = "Crypto"  # Default category for Coinbase assets
                for cat, symbols in user_params.get("categories", {}).items():
                    if symbol in symbols:
                        category = cat
                        break

                # Fetch current price from Polygon.io
                current_price = fetch_crypto_price(symbol)
                
                # Get cost basis from user_params or use current price
                cost_basis = user_params.get("costBasis", {}).get(symbol, current_price * amount)
                
                # Calculate market values
                market_value = amount * current_price
                start_market_value = cost_basis
                end_market_value = market_value

                holding = {
                    "symbol": symbol,
                    "name": currency_info["name"],
                    "quantity": amount,
                    "costBasis": cost_basis,
                    "currentPrice": current_price,
                    "value": market_value,
                    "currency": "USD",
                    # New fields
                    "category": category,
                    "beta": user_params.get("betas", {}).get(symbol, 1.0),
                    "startMarketValue": start_market_value,
                    "endMarketValue": end_market_value,
                    "rebalancedShares": amount,  # Initial value same as current
                    "rebalanceCash": 0.0,  # Will be calculated during rebalancing
                    "valueDifference": market_value - start_market_value,
                    "targetWeight": 0.0  # Will be set during optimization
                }
                holdings.append(holding)
                total_value += market_value
                total_investment += start_market_value

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
            "name": user_params.get("name", "Coinbase Portfolio"),
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
        logger.exception(f"Error converting Coinbase positions: {e}")
        raise

def userParamsToUserPolicy(user_params):
    """
    Convert user parameters to an enhanced user policy for Coinbase.
    """
    # Use the user ID directly from parameters without hashing
    user_id = user_params.get("userId", "")
    portfolio_name = user_params.get("name", "Coinbase Portfolio")
    
    # Policy-specific parameters with defaults
    policy = {
        "brokerType": 2,  # COINBASE = 2 in the updated enum
        "investmentHorizon": user_params.get("investmentHorizon", 2),
        "rebalanceFrequency": user_params.get("rebalanceFrequency", "monthly"),
        "riskTolerance": user_params.get("riskTolerance", 0.5),
        "filingStatus": user_params.get("filingStatus", "Single"),
        "annualIncome": user_params.get("annualIncome", 75000.0),
        "equitiesPercent": user_params.get("equitiesPercent", 1.0),
        "categories": user_params.get("categories", {
            "Crypto": [],
            "Stablecoins": [],
            "DeFi": [],
            "Smart Contract": [],
            "Layer2": [],
            "Gaming": []
        }),
        "sectorCaps": user_params.get("sectorCaps", {
            "Crypto": 0.40,
            "Stablecoins": 0.30,
            "DeFi": 0.20,
            "Smart Contract": 0.25,
            "Layer2": 0.15,
            "Gaming": 0.10
        }),
        "targetAllocation": {
            "name": portfolio_name,
            "allocations": user_params.get("allocations", []),
            "lastUpdated": datetime.utcnow().isoformat() + "Z"
        }
    }
    
    return policy

def upload_portfolio_to_mongo(portfolio_obj):
    """Upload the portfolio object to MongoDB"""
    try:
        client = get_mongo_client()
        db = client["Integrations"]
        collection = db["Portfolios"]
        
        filter_query = {
            "userId": portfolio_obj["userId"],
            "policy.brokerType": portfolio_obj["policy"]["brokerType"]
        }
        
        result = collection.replace_one(
            filter_query,
            portfolio_obj,
            upsert=True
        )
        
        return {
            "success": True,
            "message": "Portfolio successfully updated",
            "id": str(result.upserted_id if result.upserted_id else result.matched_count)
        }
        
    except Exception as e:
        logger.exception(f"Failed to upload portfolio to MongoDB: {e}")
        return {"success": False, "error": str(e)}

def publish_to_kafka(portfolio_obj):
    """Publish portfolio data to Kafka"""
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_key = os.getenv("KAFKA_KEY")
        kafka_secret = os.getenv("KAFKA_SECRET")
        
        if not all([kafka_bootstrap_servers, kafka_key, kafka_secret]):
            logger.warning("Kafka environment variables not set")
            return False
            
        config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'sasl.username': kafka_key,
            'sasl.password': kafka_secret,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'acks': 'all'
        }
        
        producer = Producer(config)
        
        portfolio_json = json.dumps(portfolio_obj, cls=MongoJSONEncoder)
        producer.produce("alert_rebalance", portfolio_json.encode('utf-8'))
        producer.flush()
        
        return True
        
    except Exception as e:
        logger.exception(f"Failed to publish to Kafka: {e}")
        return False

def run_coinbase_operations(message):
    """Process incoming message to handle Coinbase data retrieval and processing"""
    try:
        portfolio = json.loads(message)
        user_id = portfolio.get("userId")
        
        if not user_id:
            logger.error("User ID missing in portfolio")
            return
            
        # Get stored tokens
        stored_tokens = get_stored_tokens(user_id)
        if not stored_tokens:
            logger.error(f"No stored tokens found for user {user_id}")
            return
            
        # Check if tokens need refresh
        access_token = stored_tokens["access_token"]
        if datetime.utcnow() >= stored_tokens["expiry_time"]:
            try:
                token_data = refresh_access_token(stored_tokens["refresh_token"])
                access_token = token_data["access_token"]
                store_tokens(
                    user_id,
                    token_data["access_token"],
                    token_data["refresh_token"],
                    token_data["expires_in"]
                )
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                return
                
        # Get account data
        try:
            positions_data = get_account_positions(access_token)
            portfolio_obj = convert_coinbase_positions_to_portfolio(positions_data, portfolio)
            
            # Upload to MongoDB
            upload_result = upload_portfolio_to_mongo(portfolio_obj)
            if not upload_result["success"]:
                logger.error(f"Failed to upload portfolio: {upload_result.get('error')}")
                return
                
            # Publish to Kafka
            publish_to_kafka(portfolio_obj)
            
        except Exception as e:
            logger.exception(f"Failed to process account data: {e}")
            return
            
    except Exception as e:
        logger.exception(f"Error in Coinbase operations: {e}")
        return

def listen_to_kafka():
    """Listen to Kafka for incoming messages"""
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_key = os.getenv("KAFKA_KEY")
    kafka_secret = os.getenv("KAFKA_SECRET")
    
    if not all([kafka_bootstrap_servers, kafka_key, kafka_secret]):
        logger.error("Kafka configuration missing")
        return
        
    config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'sasl.username': kafka_key,
        'sasl.password': kafka_secret,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'group.id': 'coinbase-group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(config)
    consumer.subscribe(['alert_coinbase'])
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue
            
        thread = threading.Thread(
            target=run_coinbase_operations,
            args=(msg.value(),)
        )
        thread.start()

if __name__ == "__main__":
    load_dotenv()
    listen_to_kafka()
