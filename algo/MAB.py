import pandas as pd
import json
import numpy as np
from collections import defaultdict

csv_path = "FineasPm+/synthetic_portfolios.csv"

def load_data(csv_path):
    """Load raw portfolio data."""
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows.")
    return df

def clean_data(df):
    """Clean and format the raw portfolio data."""
    # Drop rows missing key columns
    df = df.dropna(subset=['portfolio_id', 'ticker', 'weight', 'monthly_return', 'stock_beta', 'investment_horizon'])

    # Convert percentage strings to floats
    def convert_percentage(x):
        if isinstance(x, str) and '%' in x:
            return float(x.strip('%')) / 100
        return float(x)

    # Cast types for safety
    df['weight'] = df['weight'].apply(convert_percentage)
    df['monthly_return'] = df['monthly_return'].apply(convert_percentage)
    df['weekly_return'] = df['weekly_return'].apply(convert_percentage)
    df['ytd_return'] = df['ytd_return'].apply(convert_percentage)
    df['one_year_return'] = df['one_year_return'].apply(convert_percentage)
    df['stock_beta'] = df['stock_beta'].astype(float)
    
    return df

def calculate_stock_metrics(df):
    """Calculate advanced metrics for each stock across different portfolios."""
    stock_metrics = defaultdict(lambda: {
        'appearances': 0,
        'sectors': set(),
        'investment_horizons': set(),
        'betas': [],
        'returns': defaultdict(list),
        'weights': [],
        'success_rate': 0,  # Percentage of times stock outperformed its sector median
        'risk_adjusted_returns': []
    })

    # Calculate sector medians for comparison
    sector_returns = df.groupby('sector')['monthly_return'].median()

    for _, row in df.iterrows():
        metrics = stock_metrics[row['ticker']]
        metrics['appearances'] += 1
        metrics['sectors'].add(row['sector'])
        metrics['investment_horizons'].add(row['investment_horizon'])
        metrics['betas'].append(row['stock_beta'])
        metrics['weights'].append(row['weight'])
        metrics['returns']['weekly'].append(row['weekly_return'])
        metrics['returns']['monthly'].append(row['monthly_return'])
        metrics['returns']['ytd'].append(row['ytd_return'])
        metrics['returns']['yearly'].append(row['one_year_return'])
        
        # Calculate if stock outperformed its sector
        outperformed = row['monthly_return'] > sector_returns[row['sector']]
        metrics['success_rate'] = (metrics['success_rate'] * (metrics['appearances'] - 1) + outperformed) / metrics['appearances']
        
        # Calculate risk-adjusted return (Sharpe-like ratio using beta as risk measure)
        if row['stock_beta'] != 0:
            risk_adj = row['monthly_return'] / abs(row['stock_beta'])
            metrics['risk_adjusted_returns'].append(risk_adj)

    # Convert to DataFrame for easier analysis
    stock_data = []
    for ticker, metrics in stock_metrics.items():
        stock_data.append({
            'ticker': ticker,
            'appearance_count': metrics['appearances'],
            'sectors': list(metrics['sectors']),
            'investment_horizons': list(metrics['investment_horizons']),
            'avg_beta': np.mean(metrics['betas']),
            'beta_std': np.std(metrics['betas']),
            'avg_weight': np.mean(metrics['weights']),
            'weight_std': np.std(metrics['weights']),
            'avg_monthly_return': np.mean(metrics['returns']['monthly']),
            'avg_yearly_return': np.mean(metrics['returns']['yearly']),
            'return_std': np.std(metrics['returns']['monthly']),
            'success_rate': metrics['success_rate'],
            'avg_risk_adjusted_return': np.mean(metrics['risk_adjusted_returns']) if metrics['risk_adjusted_returns'] else 0
        })
    
    return pd.DataFrame(stock_data)

def summarize_portfolios(df):
    """Aggregate portfolio-level metrics for MAB features."""
    summary = df.groupby('portfolio_id').agg({
        'monthly_return': 'mean',
        'stock_beta': 'mean',
        'target_beta': 'first',
        'investment_horizon': 'first',
        'weight': 'sum',
        'weekly_return': 'mean',
        'ytd_return': 'mean',
        'one_year_return': 'mean',
        'portfolio_sharpe': 'first'
    }).rename(columns={
        'monthly_return': 'avg_monthly_return',
        'stock_beta': 'avg_beta',
        'weight': 'total_weight',
        'weekly_return': 'avg_weekly_return',
        'ytd_return': 'avg_ytd_return',
        'one_year_return': 'avg_yearly_return'
    }).reset_index()

    # Calculate portfolio diversity metrics
    diversity_metrics = df.groupby('portfolio_id').agg({
        'sector': 'nunique',
        'ticker': 'count'
    }).rename(columns={
        'sector': 'sector_count',
        'ticker': 'stock_count'
    })
    
    summary = summary.merge(diversity_metrics, on='portfolio_id')
    return summary

def jsonify_portfolios(df):
    """Convert portfolios to a JSON-like dict format with enhanced stock details."""
    portfolios = {}
    for pid, group in df.groupby('portfolio_id'):
        # Get stock details for this portfolio
        stocks = []
        for _, row in group.iterrows():
            stock_info = {
                'ticker': row['ticker'],
                'weight': row['weight'],
                'sector': row['sector'],
                'beta': row['stock_beta'],
                'investment_horizon': row['investment_horizon'],
                'returns': {
                    'weekly': row['weekly_return'],
                    'monthly': row['monthly_return'],
                    'ytd': row['ytd_return'],
                    'yearly': row['one_year_return']
                }
            }
            stocks.append(stock_info)
        
        # Calculate sector weights
        sector_weights = group.groupby('sector')['weight'].sum().to_dict()
        
        # Portfolio level metrics
        portfolios[str(pid)] = {
            'portfolio_metrics': {
                'target_beta': group['target_beta'].iloc[0],
                'actual_beta': group['stock_beta'].mean(),
                'investment_horizon': group['investment_horizon'].iloc[0],
                'returns': {
                    'weekly': group['weekly_return'].mean(),
                    'monthly': group['monthly_return'].mean(),
                    'ytd': group['ytd_return'].mean(),
                    'yearly': group['one_year_return'].mean()
                },
                'sharpe_ratio': group['portfolio_sharpe'].iloc[0],
                'sector_weights': sector_weights,
                'stock_count': len(stocks),
                'sector_count': len(sector_weights)
            },
            'stocks': stocks,
            'total_weight': group['weight'].sum()
        }
    return portfolios

def analyze_stock_persistence(df):
    """Analyze which stocks appear consistently across portfolios and their performance."""
    # Get advanced stock metrics
    stock_metrics = calculate_stock_metrics(df)
    
    # Sort by a composite score (you can adjust these weights based on importance)
    stock_metrics['composite_score'] = (
        0.3 * stock_metrics['success_rate'] +
        0.3 * stock_metrics['avg_risk_adjusted_return'] +
        0.2 * (stock_metrics['appearance_count'] / stock_metrics['appearance_count'].max()) +
        0.2 * (1 - stock_metrics['return_std'])  # Lower volatility is better
    )
    
    # Sort by composite score
    stock_metrics = stock_metrics.sort_values('composite_score', ascending=False)
    
    return stock_metrics

def save_json(data, output_path):
    """Save the portfolio JSON data to file."""
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    raw_df = load_data(csv_path)
    clean_df = clean_data(raw_df)
    
    # Generate portfolio summaries
    summary_df = summarize_portfolios(clean_df)
    print("\nPortfolio Summary:")
    print(summary_df.head())

    # Analyze stock persistence and performance
    stock_analysis = analyze_stock_persistence(clean_df)
    print("\nTop Performing Stocks (Based on Composite Score):")
    print(stock_analysis[['ticker', 'avg_monthly_return', 'success_rate', 'avg_risk_adjusted_return', 'composite_score']].head(10))

    # Save detailed portfolio data as JSON
    portfolio_json = jsonify_portfolios(clean_df)
    save_json(portfolio_json, "processed_portfolios.json")
    
    # Save stock analysis as CSV for easy reference
    stock_analysis.to_csv("stock_analysis.csv", index=False)
    print("\nSaved detailed stock analysis to stock_analysis.csv")

    # Group analysis by investment horizon
    horizon_analysis = clean_df.groupby(['investment_horizon', 'sector']).agg({
        'monthly_return': ['mean', 'std'],
        'stock_beta': 'mean',
        'ticker': 'nunique'
    }).round(4)
    
    horizon_analysis.to_csv("horizon_sector_analysis.csv")
    print("\nSaved horizon-sector analysis to horizon_sector_analysis.csv")