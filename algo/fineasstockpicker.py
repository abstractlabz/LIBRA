import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from scipy import stats
import os
import json
import time
import yfinance as yf

API_KEY = "z4epQgnaQOSU_YnazHYeKpUKpJjoMRKN"  # Replace with your API key

# Add caching directory
CACHE_DIR = "stock_cache"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# Define major indices and well-known stocks
major_tickers = [
    # Major ETFs
    'SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'VOO', 'IVV', 'VEA', 'VWO',
    
    # Tech Giants
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'INTC', 'AMD', 'CRM',
    'ADBE', 'ORCL', 'IBM', 'CSCO', 'QCOM', 'TXN', 'AVGO', 'INTU', 'NOW', 'ADP',
    
    # Financial Services
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'SCHW', 'AXP', 'V', 'MA',
    'PYPL', 'SQ', 'COF', 'USB', 'PNC', 'C', 'BK', 'STT', 'DFS',
    
    # Healthcare
    'JNJ', 'PFE', 'UNH', 'MRK', 'ABBV', 'TMO', 'AMGN', 'BMY', 'LLY', 'GILD',
    'CVS', 'ABT', 'MDT', 'DHR', 'SYK', 'BDX', 'ZTS', 'HCA', 'CI', 'ANTM',
    
    # Consumer
    'PG', 'KO', 'PEP', 'MCD', 'NKE', 'SBUX', 'DIS', 'MKC', 'CL', 'KMB',
    'EL', 'PM', 'MO', 'TGT', 'WMT', 'COST', 'HD', 'LOW', 'TJX', 'ROST',
    
    # Industrials
    'BA', 'CAT', 'GE', 'HON', 'MMM', 'UNP', 'UPS', 'FDX', 'DE', 'LMT',
    'RTX', 'GD', 'NOC', 'EMR', 'ITW', 'PH', 'ETN', 'WM', 'RSG', 'ROK',
    
    # Energy
    'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PXD', 'VLO', 'MPC', 'PSX', 'KMI',
    'WMB', 'OKE', 'APA', 'DVN', 'HES', 'OXY', 'MRO', 'HAL', 'BP', 'EPD',
    
    # Communication
    'T', 'VZ', 'CMCSA', 'NFLX', 'CHTR', 'TMUS', 'ATVI', 'EA', 'TTWO', 'MTCH',
    'DISH', 'IPG', 'OMC', 'FOX', 'FOXA', 'DIS', 'SIRI', 'GOOG', 'PARA', 'ROKU',
    
    # Materials
    'APD', 'ALB', 'VMC', 'MLM', 'FMC', 'EMN', 'IFF', 'CE', 'PKG', 'SEE', 'AVY',
    
    # Utilities
    'NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'XEL', 'PEG', 'WEC', 'ED',
    'EIX', 'AEE', 'ES', 'FE', 'PPL', 'ETR', 'CMS', 'ATO', 'LNT', 'AWK',
    
    # Real Estate
    'AMT', 'PLD', 'CCI', 'EQIX', 'PSA', 'O', 'DLR', 'AVB', 'EQR', 'SPG',
    'SBAC', 'VTR', 'ARE', 'BXP', 'UDR', 'EXR', 'MAA', 'IRM', 'REG', 'KIM'
]

def random_date(start_date, end_date):
    """Generate a random date between two dates"""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)

def calculate_returns(price, cost_basis, purchase_date, ticker, shares_held):
    """Calculate various return metrics using actual historical data"""
    current_date = datetime.now()
    purchase_date = datetime.strptime(purchase_date, '%Y-%m-%d')
    
    # Calculate days held
    days_held = (current_date - purchase_date).days
    
    # Fetch historical data for the stock
    start_date = purchase_date.strftime('%Y-%m-%d')
    end_date = current_date.strftime('%Y-%m-%d')
    historical_data = fetch_historical_prices_polygon(ticker, start_date, end_date)
    
    if historical_data.empty:
        print(f"Warning: No historical data available for {ticker}")
        return {
            'weekly_return': 0,
            'weekly_dollar_change': 0,
            'monthly_return': 0,
            'monthly_dollar_change': 0,
            'ytd_return': 0,
            'ytd_dollar_change': 0,
            'one_year_return': 0,
            'one_year_dollar_change': 0
        }
    
    # Calculate actual returns
    initial_price = historical_data.iloc[0]['close']
    current_price = historical_data.iloc[-1]['close']
    
    # Calculate percent changes and dollar changes for each period
    percent_change = (current_price - cost_basis) / cost_basis
    position_value = shares_held * current_price
    initial_position_value = shares_held * cost_basis
    
    # Calculate period returns and dollar changes
    returns = {
        'weekly_return': percent_change if days_held <= 7 else percent_change * (7/days_held),
        'weekly_dollar_change': (position_value - initial_position_value) if days_held <= 7 else (position_value - initial_position_value) * (7/days_held),
        'monthly_return': percent_change if days_held <= 30 else percent_change * (30/days_held),
        'monthly_dollar_change': (position_value - initial_position_value) if days_held <= 30 else (position_value - initial_position_value) * (30/days_held),
        'ytd_return': percent_change if days_held <= 365 else percent_change * (365/days_held),
        'ytd_dollar_change': (position_value - initial_position_value) if days_held <= 365 else (position_value - initial_position_value) * (365/days_held),
        'one_year_return': percent_change,
        'one_year_dollar_change': position_value - initial_position_value
    }
    
    return returns

def determine_investment_horizon(purchase_date):
    """Determine investment horizon based on purchase date"""
    current_date = datetime.now()
    purchase_date = datetime.strptime(purchase_date, '%Y-%m-%d')
    days_held = (current_date - purchase_date).days
    
    if days_held <= 90:
        return "Short Term (3 months)"
    elif days_held <= 365:
        return "Medium Term (5-12 months)"
    else:
        return "Long Term (1+ years)"

def calculate_portfolio_beta(portfolio_daily_returns, market_returns):
    """Calculate portfolio beta using actual historical returns"""
    try:
        # Ensure we have enough data points
        if len(portfolio_daily_returns) < 2 or len(market_returns) < 2:
            return None
            
        # Ensure arrays are the same length
        min_length = min(len(portfolio_daily_returns), len(market_returns))
        portfolio_returns = portfolio_daily_returns[:min_length]
        market_returns = market_returns[:min_length]
        
        # Remove any NaN or infinite values
        valid_mask = ~(np.isnan(portfolio_returns) | np.isnan(market_returns) | 
                      np.isinf(portfolio_returns) | np.isinf(market_returns))
        portfolio_returns = portfolio_returns[valid_mask]
        market_returns = market_returns[valid_mask]
        
        if len(portfolio_returns) < 2 or len(market_returns) < 2:
            return None
            
        # Calculate covariance between portfolio and market returns
        covariance = np.cov(portfolio_returns, market_returns)[0][1]
        
        # Calculate market variance
        market_variance = np.var(market_returns)
        
        # Avoid division by zero
        if market_variance == 0:
            return None
            
        # Calculate beta using the formula: beta = covariance / market_variance
        beta = covariance / market_variance
        
        # Ensure beta is within reasonable bounds
        if abs(beta) > 10:  # Unrealistic beta values
            return None
            
        return round(beta, 2)
    except Exception as e:
        print(f"Error calculating beta: {str(e)}")
        return None

def fetch_company_sector(ticker_symbol):
    """
    Fetch detailed company info for a given ticker and return the sector or industry.
    If neither is available or the API call fails, return None.
    """
    url = f"https://api.polygon.io/v1/meta/symbols/{ticker_symbol}/company?apiKey={API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Try to get 'sector', falling back to 'industry'
            sector = data.get('sector') or data.get('industry')
            if sector and sector.strip():
                return sector.strip()
            else:
                print(f"Info: No sector/industry data available for {ticker_symbol}")
        elif response.status_code == 404:
            print(f"Info: Ticker {ticker_symbol} not found in Polygon.io database")
        else:
            print(f"Warning: Failed to fetch company details for {ticker_symbol} (status code: {response.status_code})")
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching details for {ticker_symbol}: {e}")
    except Exception as e:
        print(f"Unexpected error fetching details for {ticker_symbol}: {e}")
    return None

def fetch_stocks_from_polygon(limit=1000, cache_days=7):
    """
    Fetch stocks from Polygon.io API and cache the results.
    Args:
        limit: Maximum number of stocks to fetch
        cache_days: Number of days to cache the results
    Returns:
        DataFrame containing stock information
    """
    cache_file = os.path.join(CACHE_DIR, "polygon_stocks.json")
    
    # Check if cache exists and is recent enough
    if os.path.exists(cache_file):
        cache_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if (datetime.now() - cache_time).days < cache_days:
            print("Loading stocks from cache...")
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                return pd.DataFrame(cached_data)
    
    print("Fetching stocks from Polygon.io...")
    stocks = []
    next_url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={API_KEY}"
    
    while next_url and len(stocks) < limit:
        try:
            response = requests.get(next_url)
            if response.status_code == 200:
                data = response.json()
                
                for ticker in data.get('results', []):
                    # Only include stocks from major exchanges
                    if ticker['primary_exchange'] in ['XNYS', 'XNAS', 'NYSE', 'NASDAQ']:
                        stock_info = {
                            'ticker': ticker['ticker'],
                            'name': ticker['name'],
                            'primary_exchange': ticker['primary_exchange'],
                            'sector': ticker.get('sector', 'Unknown'),
                            'current_price': None,  # Will be updated later
                            'cost_basis': None,     # Will be updated later
                            'purchase_date': None   # Will be updated later
                        }
                        stocks.append(stock_info)
                
                # Check for next page
                next_url = data.get('next_url')
                if next_url:
                    next_url = next_url + f"&apiKey={API_KEY}"
                
                print(f"Fetched {len(stocks)} stocks so far...")
                
                if len(stocks) >= limit:
                    break
            else:
                print(f"Error fetching stocks: {response.status_code}")
                break
                
            # Add a small delay to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching stocks: {e}")
            break
    
    # Cache the results
    if stocks:
        with open(cache_file, 'w') as f:
            json.dump(stocks, f)
    
    return pd.DataFrame(stocks)

def fetch_stock_list(limit=1000):
    """
    Fetch stocks from both predefined list and cached Polygon.io data, combining them.
    """
    # Get predefined stocks
    etf_sectors = {
        'SPY': 'S&P 500 Index ETF',
        'QQQ': 'Nasdaq-100 Index ETF',
        'DIA': 'Dow Jones Industrial ETF',
        'IWM': 'Russell 2000 Small-Cap ETF',
        'VTI': 'Total US Market ETF',
        'VOO': 'S&P 500 Index ETF',
        'IVV': 'S&P 500 Index ETF',
        'VEA': 'Developed Markets ETF',
        'VWO': 'Emerging Markets ETF'
    }
    
    # First try to load from cache
    cache_file = os.path.join(CACHE_DIR, "complete_stock_list.json")
    if os.path.exists(cache_file):
        print("Loading complete stock list from cache...")
        with open(cache_file, 'r') as f:
            cached_data = json.load(f)
            if len(cached_data) > 500:  # Only use cache if it has enough stocks
                return pd.DataFrame(cached_data)
    
    # If no cache exists or cache is too small, create the complete list
    print("Creating complete stock list...")
    stock_data = []
    start_date = datetime(2007, 1, 1)
    end_date = datetime(2025, 4, 8)
    
    # Add predefined stocks first
    for ticker in major_tickers:
        if ticker in etf_sectors:
            sector = etf_sectors[ticker]
        else:
            sector = fetch_company_sector(ticker)
            if not sector:
                sector = "Unknown"
        
        current_price = round(random.uniform(10, 1000), 2)
        cost_basis = round(current_price * random.uniform(0.5, 1.5), 2)
        purchase_date = random_date(start_date, end_date).strftime('%Y-%m-%d')
        
        stock_data.append({
            'ticker': ticker,
            'name': ticker,
            'primary_exchange': 'NYSE/NASDAQ',
            'sector': sector,
            'current_price': current_price,
            'cost_basis': cost_basis,
            'purchase_date': purchase_date
        })
    
    # Fetch additional stocks from Polygon
    polygon_stocks = fetch_stocks_from_polygon(limit=limit)
    if not polygon_stocks.empty:
        existing_tickers = set(item['ticker'] for item in stock_data)
        
        for _, row in polygon_stocks.iterrows():
            if row['ticker'] not in existing_tickers:
                current_price = round(random.uniform(10, 1000), 2)
                cost_basis = round(current_price * random.uniform(0.5, 1.5), 2)
                purchase_date = random_date(start_date, end_date).strftime('%Y-%m-%d')
                
                stock_data.append({
                    'ticker': row['ticker'],
                    'name': row['name'],
                    'primary_exchange': row['primary_exchange'],
                    'sector': row['sector'] if row['sector'] != 'Unknown' else fetch_company_sector(row['ticker']) or 'Unknown',
                    'current_price': current_price,
                    'cost_basis': cost_basis,
                    'purchase_date': purchase_date
                })
    
    print(f"Total stocks gathered: {len(stock_data)}")
    
    # Cache the complete list
    with open(cache_file, 'w') as f:
        json.dump(stock_data, f)
    
    return pd.DataFrame(stock_data)

def random_date_by_horizon(horizon_type):
    """Generate a random date based on investment horizon"""
    current_date = datetime.now()
    
    if horizon_type == "Short Term":
        # 0-3 months
        start_date = current_date - timedelta(days=90)
        end_date = current_date
    elif horizon_type == "Medium Term":
        # 5-12 months
        start_date = current_date - timedelta(days=365)
        end_date = current_date - timedelta(days=150)
    else:  # Long Term
        # 1+ years
        start_date = datetime(2007, 1, 1)
        end_date = current_date - timedelta(days=365)
    
    return random_date(start_date, end_date)

def fetch_risk_free_rate(cache_days=1):
    """
    Fetch the current 10-year Treasury yield from Polygon.io
    Uses US10Y as the ticker for 10-year Treasury
    """
    cache_file = os.path.join(CACHE_DIR, "risk_free_rate.json")
    
    # Check if cache exists and is recent enough
    if os.path.exists(cache_file):
        cache_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if (datetime.now() - cache_time).days < cache_days:
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                return cached_data['risk_free_rate']
    
    # If no cache or expired, fetch from Polygon
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        url = f"https://api.polygon.io/v2/aggs/ticker/US10Y/range/1/day/{start_date}/{end_date}?apiKey={API_KEY}"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data.get('results'):
                # Get the most recent closing value and convert to decimal
                # Treasury yields are in percentage points, so we divide by 100
                risk_free_rate = data['results'][-1]['c'] / 100
                
                # Cache the result
                with open(cache_file, 'w') as f:
                    json.dump({'risk_free_rate': risk_free_rate, 'date': end_date}, f)
                
                return risk_free_rate
    except Exception as e:
        print(f"Error fetching risk-free rate: {e}")
    
    # Default to 10-year Treasury average if fetch fails
    return 0.0425  # 4.25% as fallback

def calculate_sharpe_ratio(returns, risk_free_rate=None):
    """
    Calculate Sharpe ratio using return data and current risk-free rate
    Args:
        returns: numpy array of daily returns
        risk_free_rate: annual risk-free rate (if None, fetches current 10Y Treasury)
    Returns:
        Annualized Sharpe ratio
    """
    if len(returns) < 30:  # Require at least 30 days of data
        return 0
        
    if risk_free_rate is None:
        risk_free_rate = fetch_risk_free_rate()
    
    try:
        # Convert annual risk-free rate to daily
        daily_rf_rate = risk_free_rate / 252
        
        # Calculate excess returns
        excess_returns = returns - daily_rf_rate
        
        # Calculate annualized Sharpe ratio
        if np.std(excess_returns) == 0:
            return 0
            
        sharpe = (np.mean(excess_returns) / np.std(excess_returns)) * np.sqrt(252)
        
        # Cap extreme values
        return max(min(sharpe, 10), -10)
        
    except Exception as e:
        print(f"Error calculating Sharpe ratio: {e}")
        return 0

def assign_stock_betas(num_stocks, target_beta, horizon_type):
    """Assign individual stock betas that will approximate the target portfolio beta"""
    # Define beta ranges based on sector and horizon
    if horizon_type == "Short Term":
        min_beta, max_beta = 0.6, 1.4
    elif horizon_type == "Medium Term":
        min_beta, max_beta = 0.5, 1.6
    else:  # Long Term
        min_beta, max_beta = 0.4, 1.8
    
    # Generate random betas that will average to target_beta
    betas = []
    for i in range(num_stocks - 1):
        beta = np.random.uniform(min_beta, max_beta)
        betas.append(beta)
    
    # Calculate the last beta to achieve target average
    last_beta = num_stocks * target_beta - sum(betas)
    # Ensure last beta is within reasonable bounds
    last_beta = max(min_beta, min(max_beta, last_beta))
    betas.append(last_beta)
    
    return betas

def generate_portfolios(stock_list, num_portfolios=5000, min_sector_weight=0.05, max_sector_weight=0.5):
    """Generate random portfolios with additional metrics using real historical data"""
    # Fetch risk-free rate once at the start
    risk_free_rate = fetch_risk_free_rate()
    print(f"Using risk-free rate of {risk_free_rate*100:.2f}%")
    
    portfolios = []
    # Filter out unknown sectors and ensure we have valid data
    stock_list = stock_list[stock_list['sector'] != "Unknown"].dropna(subset=['sector'])
    # Remove duplicate tickers
    stock_list = stock_list.drop_duplicates(subset=['ticker'])
    
    # Define ETF categories and their characteristics
    etf_categories = {
        'S&P 500 Index ETF': {'beta_range': (0.95, 1.05), 'horizon_weights': {'Short Term': 0.4, 'Medium Term': 0.35, 'Long Term': 0.3}},
        'Nasdaq-100 Index ETF': {'beta_range': (1.1, 1.3), 'horizon_weights': {'Short Term': 0.1, 'Medium Term': 0.2, 'Long Term': 0.25}},
        'Dow Jones Industrial ETF': {'beta_range': (0.9, 1.0), 'horizon_weights': {'Short Term': 0.3, 'Medium Term': 0.25, 'Long Term': 0.2}},
        'Russell 2000 Small-Cap ETF': {'beta_range': (1.2, 1.4), 'horizon_weights': {'Short Term': 0.05, 'Medium Term': 0.1, 'Long Term': 0.15}},
        'Total US Market ETF': {'beta_range': (0.95, 1.05), 'horizon_weights': {'Short Term': 0.35, 'Medium Term': 0.3, 'Long Term': 0.25}},
        'Developed Markets ETF': {'beta_range': (0.8, 1.1), 'horizon_weights': {'Short Term': 0.15, 'Medium Term': 0.2, 'Long Term': 0.3}},
        'Emerging Markets ETF': {'beta_range': (1.1, 1.5), 'horizon_weights': {'Short Term': 0.05, 'Medium Term': 0.15, 'Long Term': 0.25}}
    }
    
    # Separate ETFs and stocks
    etf_list = stock_list[stock_list['sector'].isin(etf_categories.keys())]
    stock_list = stock_list[~stock_list['sector'].isin(etf_categories.keys())]
    
    sectors = stock_list['sector'].unique()
    
    if len(sectors) == 0:
        print("Error: No valid sectors found in stock list")
        return []
    
    # Define investment horizon distribution
    horizon_types = ["Short Term", "Medium Term", "Long Term"]
    horizon_weights = [0.25, 0.35, 0.40]
    
    # Define beta ranges for different investment horizons
    beta_ranges = {
        "Short Term": (0.7, 1.2),    # More conservative for short-term
        "Medium Term": (0.6, 1.4),   # Moderate range for medium-term
        "Long Term": (0.5, 1.7)      # Wider range for long-term
    }
    
    # Get market data (using SPY as market proxy)
    market_ticker = "SPY"
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    market_data = fetch_historical_prices_polygon(market_ticker, start_date, end_date)
    
    if market_data.empty:
        print("Error: Could not fetch market data")
        return []
    
    market_returns = market_data['close'].pct_change().dropna().values
    
    # Pre-fetch and cache historical data for all stocks
    print("Pre-fetching historical data for all stocks...")
    historical_data_cache = {}
    valid_stocks = []
    
    for ticker in pd.concat([stock_list['ticker'], etf_list['ticker']]).unique():
        historical_data = fetch_historical_prices_polygon(ticker, start_date, end_date)
        if not historical_data.empty:
            historical_data_cache[ticker] = historical_data
            valid_stocks.append(ticker)
    
    print(f"Cached historical data for {len(historical_data_cache)} stocks")
    
    # Filter lists to only include stocks with valid historical data
    stock_list = stock_list[stock_list['ticker'].isin(valid_stocks)]
    etf_list = etf_list[etf_list['ticker'].isin(valid_stocks)]
    
    # Define portfolio size distribution with more realistic ranges
    portfolio_sizes = {
        'small': (5000, 25000, 0.40),    # 40% of portfolios
        'medium': (25000, 100000, 0.45),  # 45% of portfolios
        'large': (100000, 500000, 0.14),  # 14% of portfolios
        'very_large': (500000, 1000000, 0.01)  # 1% of portfolios
    }
    
    for portfolio_id in range(num_portfolios):
        try:
            # Select portfolio size based on distribution
            size_category = np.random.choice(
                list(portfolio_sizes.keys()),
                p=[p[2] for p in portfolio_sizes.values()]
            )
            min_val, max_val, _ = portfolio_sizes[size_category]
            portfolio_value = np.random.uniform(min_val, max_val)
            
            # Select investment horizon and corresponding target beta range
            portfolio_horizon = np.random.choice(horizon_types, p=horizon_weights)
            beta_range = beta_ranges[portfolio_horizon]
            target_beta = round(np.random.uniform(beta_range[0], beta_range[1]), 2)
            
            portfolio = {
                'portfolio_id': portfolio_id + 1,
                'tickers': [], 'weights': [], 'sectors': [],
                'stock_betas': [], 'stock_sharpe_ratios': [],
                'target_beta': target_beta,
                'actual_beta': None,
                'portfolio_sharpe': None,
                'total_value': portfolio_value,
                'investment_horizon': portfolio_horizon
            }
            
            # Determine number of stocks based on portfolio size
            if portfolio_value < 25000:
                # Small portfolios: 5-10 stocks
                num_stocks = np.random.randint(5, 11)
                etf_allocation = np.random.uniform(0.2, 0.4)  # 20-40% in ETFs
            elif portfolio_value < 100000:
                # Medium portfolios: 8-15 stocks
                num_stocks = np.random.randint(8, 16)
                etf_allocation = np.random.uniform(0.25, 0.45)  # 25-45% in ETFs
            else:
                # Large portfolios: 10-20 stocks
                num_stocks = np.random.randint(10, 21)
                etf_allocation = np.random.uniform(0.3, 0.5)  # 30-50% in ETFs
            
            # First, select and weight ETFs based on horizon and target beta
            if not etf_list.empty:
                # Calculate how many ETFs to include based on portfolio size
                max_etfs = min(4, max(2, int(num_stocks * 0.25)))  # 25% of total positions can be ETFs
                
                # Score each ETF based on how well it matches the strategy
                etf_scores = []
                for _, etf in etf_list.iterrows():
                    etf_category = etf['sector']
                    category_info = etf_categories[etf_category]
                    
                    # Get historical data and calculate beta
                    historical_data = historical_data_cache[etf['ticker']]
                    returns = historical_data['close'].pct_change().dropna().values
                    beta = calculate_portfolio_beta(returns, market_returns)
                    
                    if beta is not None:
                        # Score based on how well beta matches target
                        beta_match = 1 - abs(beta - target_beta) / 2  # Normalize difference
                        
                        # Score based on horizon preference
                        horizon_score = category_info['horizon_weights'][portfolio_horizon]
                        
                        # Combine scores
                        total_score = (beta_match + horizon_score) / 2
                        
                        etf_scores.append({
                            'etf': etf,
                            'score': total_score,
                            'beta': beta,
                            'returns': returns
                        })
                
                # Sort ETFs by score and select top ones
                etf_scores.sort(key=lambda x: x['score'], reverse=True)
                selected_etfs = etf_scores[:max_etfs]
                
                # Distribute ETF allocation based on scores
                if selected_etfs:
                    total_score = sum(e['score'] for e in selected_etfs)
                    for etf_info in selected_etfs:
                        weight = (etf_info['score'] / total_score) * etf_allocation
                        
                        portfolio['tickers'].append(etf_info['etf']['ticker'])
                        portfolio['weights'].append(weight)
                        portfolio['sectors'].append(etf_info['etf']['sector'])
                        portfolio['stock_betas'].append(etf_info['beta'])
                        portfolio['stock_sharpe_ratios'].append(calculate_sharpe_ratio(etf_info['returns'], risk_free_rate))
            
            # Then select individual stocks
            remaining_weight = 1 - sum(portfolio['weights'])
            num_sectors = min(np.random.randint(3, 7), num_stocks - len(portfolio['tickers']))
            selected_sectors = np.random.choice(sectors, size=min(num_sectors, len(sectors)), replace=False)
            
            # Allocate remaining weight among sectors
            sector_weights = np.random.dirichlet(np.ones(len(selected_sectors))) * remaining_weight
            
            for sector, sector_weight in zip(selected_sectors, sector_weights):
                sector_stocks = stock_list[stock_list['sector'] == sector]
                if len(sector_stocks) == 0:
                    continue
                
                # Select 1-4 stocks per sector based on portfolio size
                num_sector_stocks = np.random.randint(1, min(5, len(sector_stocks) + 1))
                
                # Score stocks based on beta alignment with target
                sector_stock_scores = []
                for _, stock in sector_stocks.iterrows():
                    historical_data = historical_data_cache[stock['ticker']]
                    returns = historical_data['close'].pct_change().dropna().values
                    beta = calculate_portfolio_beta(returns, market_returns)
                    
                    if beta is not None:
                        # Score based on how well beta matches target
                        beta_match = 1 - abs(beta - target_beta) / 2
                        sector_stock_scores.append({
                            'stock': stock,
                            'score': beta_match,
                            'beta': beta,
                            'returns': returns
                        })
                
                # Sort and select top scoring stocks
                sector_stock_scores.sort(key=lambda x: x['score'], reverse=True)
                selected_stocks = sector_stock_scores[:num_sector_stocks]
                
                if selected_stocks:
                    # Distribute sector weight based on scores
                    total_score = sum(s['score'] for s in selected_stocks)
                    stock_weights = [s['score'] / total_score * sector_weight for s in selected_stocks]
                    
                    for stock_info, weight in zip(selected_stocks, stock_weights):
                        portfolio['tickers'].append(stock_info['stock']['ticker'])
                        portfolio['weights'].append(weight)
                        portfolio['sectors'].append(sector)
                        portfolio['stock_betas'].append(stock_info['beta'])
                        portfolio['stock_sharpe_ratios'].append(calculate_sharpe_ratio(stock_info['returns'], risk_free_rate))
            
            if len(portfolio['weights']) > 0:
                # Normalize weights to ensure they sum to 1
                total_weight = sum(portfolio['weights'])
                portfolio['weights'] = [w / total_weight for w in portfolio['weights']]
                
                # Calculate portfolio returns and metrics
                portfolio_daily_returns = np.zeros(len(market_returns))
                for ticker, weight in zip(portfolio['tickers'], portfolio['weights']):
                    stock_returns = historical_data_cache[ticker]['close'].pct_change().dropna().values
                    if len(stock_returns) >= len(market_returns):
                        portfolio_daily_returns += stock_returns[:len(market_returns)] * weight
                
                portfolio['actual_beta'] = calculate_portfolio_beta(portfolio_daily_returns, market_returns)
                portfolio['portfolio_sharpe'] = calculate_sharpe_ratio(portfolio_daily_returns, risk_free_rate)
                
                portfolios.append(portfolio)
        
        except Exception as e:
            print(f"Error generating portfolio {portfolio_id + 1}: {str(e)}")
            continue
    
    return portfolios

def portfolios_to_dataframe(portfolios):
    rows = []
    for p in portfolios:
        portfolio_id = p['portfolio_id']
        # Find earliest purchase date for the portfolio
        earliest_purchase_date = None
        
        # First pass to find earliest purchase date
        for ticker in p['tickers']:
            purchase_date = stock_df[stock_df['ticker'] == ticker]['purchase_date'].iloc[0]
            date = datetime.strptime(purchase_date, '%Y-%m-%d')
            if earliest_purchase_date is None or date < earliest_purchase_date:
                earliest_purchase_date = date
        
        portfolio_inception = earliest_purchase_date.strftime('%Y-%m-%d')
        
        for ticker, weight, sector, stock_beta, stock_sharpe in zip(
            p['tickers'], p['weights'], p['sectors'], 
            p['stock_betas'], p['stock_sharpe_ratios']
        ):
            position_dollars = weight * p['total_value']
            
            rows.append({
                'portfolio_id': portfolio_id,
                'ticker': ticker,
                'weight': f"{weight*100:.2f}%",  # Convert to percentage with 2 decimals
                'sector': sector,
                'stock_beta': f"{stock_beta:.2f}" if stock_beta is not None else None,
                'stock_sharpe': f"{stock_sharpe:.2f}" if stock_sharpe is not None else None,
                'target_beta': f"{p['target_beta']:.2f}" if p['target_beta'] is not None else None,
                'actual_beta': f"{p['actual_beta']:.2f}" if p['actual_beta'] is not None else None,
                'portfolio_sharpe': f"{p['portfolio_sharpe']:.2f}" if p['portfolio_sharpe'] is not None else None,
                'investment_horizon': p['investment_horizon'],
                'total_portfolio_value': f"${p['total_value']:,.2f}",
                'position_dollars': f"${position_dollars:,.2f}",
                'shares_held': None,
                'cost_basis_per_share': None,
                'total_position_cost': None,
                'portfolio_inception': portfolio_inception
            })
    return pd.DataFrame(rows)

def fetch_historical_prices_polygon(ticker, start_date, end_date, cache_days=7):
    """
    Fetch historical price data from Polygon.io API and cache the results.
    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        cache_days: Number of days to cache the results
    Returns:
        DataFrame containing historical price data
    """
    cache_file = os.path.join(CACHE_DIR, f"{ticker}_historical.json")
    
    # Check if cache exists and is recent enough
    if os.path.exists(cache_file):
        cache_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if (datetime.now() - cache_time).days < cache_days:
            try:
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)
                    return pd.DataFrame(cached_data)
            except:
                pass  # If cache is corrupted, fetch new data
    
    try:
        print(f"Fetching historical data for {ticker} from Polygon.io...")
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?apiKey={API_KEY}"
        
        response = requests.get(url, timeout=10)  # Add timeout
        if response.status_code == 200:
            data = response.json()
            if 'results' in data and data['results']:
                prices = []
                for result in data['results']:
                    prices.append({
                        'date': datetime.fromtimestamp(result['t']/1000).strftime('%Y-%m-%d'),
                        'open': result['o'],
                        'high': result['h'],
                        'low': result['l'],
                        'close': result['c'],
                        'volume': result['v']
                    })
                
                # Cache the results
                with open(cache_file, 'w') as f:
                    json.dump(prices, f)
                
                return pd.DataFrame(prices)
            else:
                print(f"No historical data available for {ticker}")
                return pd.DataFrame()
        else:
            print(f"Error fetching historical data for {ticker}: {response.status_code}")
            return pd.DataFrame()
    except requests.exceptions.Timeout:
        print(f"Timeout fetching data for {ticker}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching historical data for {ticker}: {str(e)}")
        return pd.DataFrame()

if __name__ == "__main__":
    print("Fetching stock metadata from Polygon.io...")
    stock_df = fetch_stock_list()
    print(f"Retrieved {len(stock_df)} tickers.")
    print("Generating synthetic portfolios...")
    portfolios = generate_portfolios(stock_df, num_portfolios=5000)
    print("Converting portfolios to DataFrame...")
    df = portfolios_to_dataframe(portfolios)
    
    # Ensure both DataFrames have the required columns
    if 'ticker' not in stock_df.columns:
        print("Error: 'ticker' column missing from stock_df")
        exit(1)
    if 'ticker' not in df.columns:
        print("Error: 'ticker' column missing from portfolio DataFrame")
        exit(1)
    
    # Merge with stock_df to get price and date information
    df = df.merge(
        stock_df[['ticker', 'current_price', 'cost_basis']], 
        on='ticker', 
        how='left'
    )
    
    # Calculate shares and cost basis (allow fractional shares)
    df['position_dollars'] = df['position_dollars'].str.replace('$', '').str.replace(',', '').astype(float)
    df['shares_held'] = (df['position_dollars'] / df['current_price']).round(4)  # Allow 4 decimal places for fractional shares
    df['cost_basis_per_share'] = df['cost_basis'].round(2)
    df['total_position_cost'] = (df['shares_held'] * df['cost_basis']).round(2)
    
    # Format currency columns
    df['position_dollars'] = df['position_dollars'].apply(lambda x: f"${x:,.2f}")
    df['total_position_cost'] = df['total_position_cost'].apply(lambda x: f"${x:,.2f}")
    df['current_price'] = df['current_price'].apply(lambda x: f"${x:.2f}")
    df['cost_basis_per_share'] = df['cost_basis_per_share'].apply(lambda x: f"${x:.2f}")
    
    # Calculate returns for each position
    returns_data = []
    for _, row in df.iterrows():
        returns = calculate_returns(
            float(row['current_price'].replace('$', '')), 
            float(row['cost_basis_per_share'].replace('$', '')), 
            row['portfolio_inception'],
            row['ticker'],
            float(row['shares_held'])  # Pass the number of shares held
        )
        returns_data.append(returns)
    
    # Convert returns data to DataFrame and merge
    returns_df = pd.DataFrame(returns_data)
    
    # Format return columns as percentages and dollar changes
    for period in ['weekly', 'monthly', 'ytd', 'one_year']:
        returns_df[f'{period}_return'] = returns_df[f'{period}_return'].apply(lambda x: f"{x*100:.2f}%")
        returns_df[f'{period}_dollar_change'] = returns_df[f'{period}_dollar_change'].apply(lambda x: f"${x:,.2f}")
    
    df = pd.concat([df, returns_df], axis=1)
    
    # Format the final columns
    df = df[[
        'portfolio_id',
        'ticker',
        'sector',
        'investment_horizon',
        'shares_held',
        'current_price',
        'cost_basis_per_share',
        'total_position_cost',
        'position_dollars',
        'total_portfolio_value',
        'weight',
        'stock_beta',  # Actual beta from market data
        'target_beta',  # Portfolio-level target beta based on horizon
        'actual_beta',  # Actual portfolio beta from market data
        'stock_sharpe',
        'portfolio_sharpe',
        'weekly_return',
        'weekly_dollar_change',
        'monthly_return',
        'monthly_dollar_change',
        'ytd_return',
        'ytd_dollar_change',
        'one_year_return',
        'one_year_dollar_change',
        'portfolio_inception'
    ]]
    
    output_file = "synthetic_portfolios.csv"
    df.to_csv(output_file, index=False)
    print(f"Saved {len(df)} portfolio entries to {output_file}.")