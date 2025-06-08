# backtest.py

import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# --- Import configuration from config.py ---
try:
    from config import DATA_DIRECTORY, STOCK_UNIVERSE
except ImportError:
    print("Error: config.py not found or is missing variables.")
    exit()

def prepare_price_data(price_filepath):
    """Loads price data and structures it for fast lookups."""
    try:
        with open(price_filepath, 'r') as f: price_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Price data file not found at {price_filepath}")
        return None
    prices_structured = {ticker: {item['date']: {'open': item['open'], 'close': item['close']} for item in daily_data} for ticker, daily_data in price_data.items()}
    return prices_structured

def prepare_sentiment_data(sentiment_filepath):
    """Loads sentiment data and groups signals by date."""
    try:
        with open(sentiment_filepath, 'r') as f: sentiment_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Sentiment data file not found at {sentiment_filepath}")
        return None
    signals_by_date = {}
    for article in sentiment_data:
        trade_date = datetime.fromisoformat(article['published_utc'].replace('Z', '+00:00')).strftime('%Y-%m-%d')
        if trade_date not in signals_by_date:
            signals_by_date[trade_date] = []
        signals_by_date[trade_date].append({'ticker': article['ticker'], 'score': article['gemini_score']})
    return signals_by_date

def run_backtest(prices, signals):
    """Runs the main backtest loop and returns daily portfolio returns."""
    all_dates = sorted(list(set(date for ticker_prices in prices.values() for date in ticker_prices.keys())))
    daily_returns = []
    print(f"\nRunning backtest across {len(all_dates)} trading days...")

    for date in all_dates:
        if date not in signals:
            daily_returns.append(0)
            continue
        
        long_portfolio = [s['ticker'] for s in signals[date] if s['score'] == 1]
        short_portfolio = [s['ticker'] for s in signals[date] if s['score'] == -1]

        daily_long_return, daily_short_return = 0, 0
        if long_portfolio:
            long_returns = []
            for ticker in long_portfolio:
                if ticker in prices and date in prices[ticker] and prices[ticker][date]['open'] > 0:
                    day_prices = prices[ticker][date]
                    long_returns.append((day_prices['close'] / day_prices['open']) - 1)
            if long_returns: daily_long_return = np.mean(long_returns)

        if short_portfolio:
            short_returns = []
            for ticker in short_portfolio:
                if ticker in prices and date in prices[ticker] and prices[ticker][date]['close'] > 0:
                    day_prices = prices[ticker][date]
                    short_returns.append((day_prices['open'] / day_prices['close']) - 1)
            if short_returns: daily_short_return = np.mean(short_returns)

        total_daily_return = (daily_long_return + daily_short_return) / 2.0
        daily_returns.append(total_daily_return)

    return pd.Series(daily_returns, index=pd.to_datetime(all_dates))

def evaluate_performance(daily_returns):
    """Calculates and prints performance metrics and plots cumulative returns."""
    if daily_returns.empty or daily_returns.sum() == 0:
        print("No returns were generated, cannot evaluate performance.")
        return

    cumulative_returns = (1 + daily_returns).cumprod()
    total_return = (cumulative_returns.iloc[-1] - 1) * 100
    if daily_returns.std() > 0:
        sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)
    else:
        sharpe_ratio = 0

    print("\n--- Backtest Performance Results ---")
    print(f"Total Return: {total_return:.2f}%")
    print(f"Annualized Sharpe Ratio: {sharpe_ratio:.2f}")
    
    plt.figure(figsize=(12, 7))
    cumulative_returns.plot(title=f'Strategy Cumulative Returns ({STOCK_UNIVERSE.upper()})', grid=True)
    plt.xlabel('Date')
    plt.ylabel('Cumulative Returns (1 = starting capital)')
    plot_filename = f'strategy_performance_{STOCK_UNIVERSE}.png'
    plt.savefig(plot_filename)
    print(f"\nPerformance chart saved to {plot_filename}")

if __name__ == "__main__":
    print(f"--- Starting Backtest for: {STOCK_UNIVERSE.upper()} ---")
    print("\nDisclaimer: This is a simplified educational model and is NOT financial advice.")
    
    price_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}_price_data.json")
    sentiment_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}_gemini_sentiment.json")

    price_data = prepare_price_data(price_filepath)
    sentiment_signals = prepare_sentiment_data(sentiment_filepath)
    
    if price_data and sentiment_signals:
        portfolio_returns = run_backtest(price_data, sentiment_signals)
        evaluate_performance(portfolio_returns)
    else:
        print("Could not run backtest due to missing data. Please ensure previous scripts ran successfully.")