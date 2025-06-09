# backtest.py

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# --- Import configuration from config.py ---
try:
    from config import DATA_DIRECTORY, STOCK_UNIVERSE, ENABLE_SCREENER
except ImportError:
    print("Error: config.py not found or is missing variables.")
    exit()

def prepare_price_data(price_filepath):
    """
    Loads price data and structures it for fast lookups.
    Returns a dictionary: {ticker -> {date -> {open: price, close: price}}}
    """
    try:
        with open(price_filepath, 'r') as f:
            price_data = json.load(f)
        print(f"Loaded price data from: {price_filepath}")
        
        # Restructure for O(1) lookups
        prices_structured = {ticker: {item['date']: {'open': item['open'], 'close': item['close']} for item in daily_data} for ticker, daily_data in price_data.items()}
        return prices_structured
    
    except FileNotFoundError:
        print(f"Error: Price data file not found at {price_filepath}")
        return None

def prepare_sentiment_data(sentiment_filepath):
    """
    Loads sentiment data (now from Polygon.io) and groups signals by date.
    """
    try:
        with open(sentiment_filepath, 'r') as f:
            # The input file is now the raw news file with sentiment included
            sentiment_data = json.load(f)
        print(f"Loaded sentiment data from: {sentiment_filepath}")
        
        signals_by_date = {}
        for article in sentiment_data:
            trade_date = datetime.fromisoformat(article['published_utc'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
            
            if trade_date not in signals_by_date:
                signals_by_date[trade_date] = []
            
            # --- MODIFIED: Use the polygon_sentiment_score ---
            score = article.get('polygon_sentiment_score', 0)
            if score in [1, -1]: # Only consider 'Buy' or 'Sell' signals
                signals_by_date[trade_date].append({
                    'ticker': article['ticker'],
                    'score': score
                })
        return signals_by_date
        
    except FileNotFoundError:
        print(f"Error: Sentiment data file not found at {sentiment_filepath}")
        return None

def run_backtest(prices, signals):
    """
    Runs the main backtest loop and returns a pandas Series of daily portfolio returns.
    """
    # Get a sorted list of unique trading days from the price data
    all_dates = sorted(list(set(date for ticker_prices in prices.values() for date in ticker_prices.keys())))
    
    daily_returns = []

    print(f"\nRunning backtest simulation across {len(all_dates)} trading days...")

    for date in all_dates:
        # If there are no signals for this day, the return is 0
        if date not in signals:
            daily_returns.append(0)
            continue

        long_portfolio = [s['ticker'] for s in signals[date] if s['score'] == 1]
        short_portfolio = [s['ticker'] for s in signals[date] if s['score'] == -1]

        daily_long_return = 0.0
        if long_portfolio:
            long_returns = []
            for ticker in long_portfolio:
                if ticker in prices and date in prices[ticker] and prices[ticker][date].get('open') > 0:
                    day_prices = prices[ticker][date]
                    # Return for a long position = (close / open) - 1
                    ret = (day_prices['close'] / day_prices['open']) - 1
                    long_returns.append(ret)
            if long_returns:
                daily_long_return = np.mean(long_returns)

        daily_short_return = 0.0
        if short_portfolio:
            short_returns = []
            for ticker in short_portfolio:
                if ticker in prices and date in prices[ticker] and prices[ticker][date].get('close') > 0:
                    day_prices = prices[ticker][date]
                    # Return for a short position = (open / close) - 1
                    ret = (day_prices['open'] / day_prices['close']) - 1
                    short_returns.append(ret)
            if short_returns:
                daily_short_return = np.mean(short_returns)

        # Assuming equal capital allocation to long and short books.
        # If one book is empty, its return is 0.
        total_daily_return = (daily_long_return + daily_short_return) / 2.0
        daily_returns.append(total_daily_return)

    return pd.Series(daily_returns, index=pd.to_datetime(all_dates))

def evaluate_performance(daily_returns, universe_name):
    """
    Calculates and prints performance metrics and plots cumulative returns.
    """
    if daily_returns.empty or daily_returns.abs().sum() == 0:
        print("\nNo trades were made or returns were all zero. Cannot evaluate performance.")
        return

    # --- Calculate Metrics ---
    # Create a DataFrame for easy calculation
    portfolio_df = pd.DataFrame({'daily_return': daily_returns})
    portfolio_df['cumulative_return'] = (1 + portfolio_df['daily_return']).cumprod()
    
    total_return = (portfolio_df['cumulative_return'].iloc[-1] - 1) * 100
    
    # Sharpe Ratio (assuming risk-free rate is 0)
    # Annualized by multiplying by sqrt(252) trading days
    if portfolio_df['daily_return'].std() > 0:
        sharpe_ratio = (portfolio_df['daily_return'].mean() / portfolio_df['daily_return'].std()) * np.sqrt(252)
    else:
        sharpe_ratio = 0.0

    print("\n--- Backtest Performance Results ---")
    print(f"Total Cumulative Return: {total_return:.2f}%")
    print(f"Annualized Sharpe Ratio: {sharpe_ratio:.2f}")
    
    # --- Plotting ---
    plt.style.use('seaborn-v0_8-darkgrid')
    plt.figure(figsize=(12, 7))
    portfolio_df['cumulative_return'].plot(title=f'Strategy Cumulative Returns ({universe_name})', legend=True)
    plt.xlabel('Date')
    plt.ylabel('Cumulative Returns (1 = starting capital)')
    
    plot_filename = f'strategy_performance_{universe_name.lower().replace(" ", "_")}.png'
    plt.savefig(plot_filename)
    print(f"\nPerformance chart saved to {plot_filename}")
    plt.show()

if __name__ == "__main__":
    file_suffix = "_screened" if ENABLE_SCREENER else ""
    universe_name_str = f"{STOCK_UNIVERSE.upper()}{file_suffix.replace('_', ' ').title()}"
    
    print(f"--- Starting Backtest for: {universe_name_str} ---")
    
    print("\nDisclaimer: This is a simplified educational model and is NOT financial advice.")
    print("Results do not account for transaction costs, slippage, or other market frictions.\n")

    # Define file paths dynamically
    price_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_price_data.json")
    sentiment_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_gemini_sentiment.json")

    # --- Run Pipeline ---
    price_data = prepare_price_data(price_filepath)
    sentiment_signals = prepare_sentiment_data(sentiment_filepath)
    
    if price_data and sentiment_signals:
        portfolio_returns = run_backtest(price_data, sentiment_signals)
        evaluate_performance(portfolio_returns, universe_name_str)
    else:
        print("\nCould not run backtest due to missing data. Please ensure previous scripts ran successfully.")