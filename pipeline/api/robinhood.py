import os
import json
import threading
import datetime
import robin_stocks.robinhood as r
import pyotp
import logging
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# Configure logging: you can change the level to DEBUG to see more details.
logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(message)s")

def delivery_report(err, msg):
    """
    Callback for Kafka Producer's produce method.
    Reports whether the message delivery was successful or not.
    """
    if err is not None:
        logging.error("Delivery report: Message delivery failed: %s", err)
    else:
        logging.info("Delivery report: Message delivered to %s partition [%d] at offset %d",
                     msg.topic(), msg.partition(), msg.offset())

def convert_robinhood_positions_to_portfolio(positions_data, base_portfolio):
    """
    Convert the Robinhood positions (raw dictionary) to a portfolio
    object that maps to your Go Portfolio struct.
    
    The Go Portfolio struct expects:
      - holdings: a list of holding objects, each with:
          • symbol       (e.g., "NVDA")
          • name         (e.g., "NVIDIA")
          • quantity     (as a float)
          • costBasis    (as a float, e.g., average_buy_price)
          • currentPrice (as a float)
          • currency     (e.g., "USD")
      - policy and other data can be reused from the original portfolio JSON.
      - lastUpdated: current timestamp in ISO8601 format.
    """
    holdings = []
    for symbol, data in positions_data.items():
        holding = {
            "symbol": symbol,
            "name": data.get("name"),
            "quantity": float(data.get("quantity", "0")),
            "costBasis": float(data.get("average_buy_price", "0")),
            "currentPrice": float(data.get("price", "0")),
            "currency": "USD"  # default value if not provided
        }
        holdings.append(holding)

    portfolio_obj = {
        "userId": base_portfolio.get("userId", ""),
        "name": base_portfolio.get("name", "Robinhood Portfolio"),
        "holdings": holdings,
        "policy": base_portfolio.get("policy", {}),
        "lastUpdated": datetime.datetime.utcnow().isoformat() + "Z"
    }
    return portfolio_obj

def run_robinhood_operations(message):
    """
    Process an incoming message to log into Robinhood, retrieve positions data,
    convert it into a Portfolio-like structure, and produce a message with that data 
    to the 'alert_rebalance' Kafka topic.
    """
    logging.info("Starting Robinhood operations")
    try:
        logging.debug("Received message: %s", message)
        
        # Convert the incoming message into a JSON object.
        portfolio = json.loads(message)
        logging.info("Portfolio JSON parsed successfully: %s", portfolio)
        
        # Extract the policy section which contains credentials.
        policy = portfolio.get("policy", {})
        if not policy:
            logging.error("No policy found in the portfolio message: %s", portfolio)
            return

        # Extract credentials.
        user_name = policy.get("userName")
        user_pass = policy.get("userPass")
        if not user_name or not user_pass:
            logging.error("User credentials are missing in the policy: %s", policy)
            return

        login = r.login(username=user_name,
                        password=user_pass,
                        expiresIn=86400,
                        by_sms=True)
        if login:
            logging.info("Robinhood login successful for user: %s", user_name)
            
            # Retrieve positions (holdings) data from Robinhood.
            positions_data = r.build_holdings()
            logging.info("Retrieved positions data: %s", positions_data)

            # Convert the Robinhood data into a portfolio structure that matches your Go type.
            portfolio_obj = convert_robinhood_positions_to_portfolio(positions_data, portfolio)
            logging.info("Restructured portfolio: %s", portfolio_obj)

            # Retrieve Kafka connection parameters from environment variables.
            kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
            kafka_key = os.getenv("KAFKA_KEY")
            kafka_secret = os.getenv("KAFKA_SECRET")
            logging.debug("Loaded Kafka parameters: bootstrap_servers=%s, key=%s", kafka_bootstrap_servers, kafka_key)

            # Kafka Producer configuration.
            config = {
                'bootstrap.servers': kafka_bootstrap_servers,
                'sasl.username':     kafka_key,
                'sasl.password':     kafka_secret,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms':   'PLAIN',
                'acks':              'all'
            }

            # Create Kafka Producer instance.
            producer = Producer(config)
            logging.info("Kafka producer created successfully")

            # Serialize the portfolio object to JSON and encode it as bytes.
            positions_payload = json.dumps(portfolio_obj).encode('utf-8')

            # Send the portfolio data as a message to the "alert_rebalance" topic.
            producer.produce("alert_rebalance", positions_payload, callback=delivery_report)
            logging.info("Produced message to 'alert_rebalance' topic. Flushing producer...")
            producer.flush()
            logging.info("Kafka producer flush complete")
        else:
            logging.error("Robinhood login failed for user: %s", user_name)
    except Exception as e:
        # logging.exception prints the error message along with the stack trace.
        logging.exception("An error occurred during Robinhood operations: %s", e)
        return

def listen_to_kafka(kafka_bootstrap_servers, kafka_key, kafka_secret):
    """
    Set up a Kafka consumer that listens to the 'alert_robinhood' topic.
    When a message is received, spawn a thread to handle Robinhood operations.
    """
    logging.info("Starting Kafka consumer for topic 'alert_robinhood'")
    config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'sasl.username':     kafka_key,
        'sasl.password':     kafka_secret,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'group.id':          'robinhood-group',
        'auto.offset.reset': 'earliest'
    }

    # Create Kafka Consumer instance.
    consumer = Consumer(config)
    consumer.subscribe(['alert_robinhood'])
    logging.info("Kafka consumer subscribed to 'alert_robinhood' topic")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error("Kafka error: %s", msg.error())
            continue

        payload = msg.value()  # Get the actual message payload.
        logging.info("Received Kafka message: %s", payload)
        # Run the Robinhood operations in a separate thread.
        thread = threading.Thread(target=run_robinhood_operations, args=(payload,))
        thread.start()

if __name__ == "__main__":
    # Load environment variables from the .env file.
    load_dotenv()

    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_key = os.getenv("KAFKA_KEY")
    kafka_secret = os.getenv("KAFKA_SECRET")

    if not kafka_bootstrap_servers or not kafka_key or not kafka_secret:
        logging.error("Kafka configuration environment variables are missing")
        exit(1)

    logging.info("Kafka environment variables loaded successfully")
    listen_to_kafka(kafka_bootstrap_servers, kafka_key, kafka_secret)
