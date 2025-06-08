# generate_recommendations.py

import os
import json
import pandas as pd

# --- Import configuration from config.py ---
try:
    # ADDED ENABLE_SCREENER to the import
    from config import (
        DATA_DIRECTORY, STOCK_UNIVERSE,
        BUY_THRESHOLD, SELL_THRESHOLD, ENABLE_SCREENER
    )
except ImportError:
    print("Error: config.py not found or is missing variables.")
    exit()

def generate_recommendations():
    """
    Loads the final sentiment data and generates a Buy/Sell/No Action
    recommendation for each ticker based on its average sentiment score.
    """
    # --- IMPORTANT DISCLAIMER ---
    print("="*60)
    print("IMPORTANT DISCLAIMER".center(60))
    print("="*60)
    print("This analysis is for educational and illustrative purposes ONLY.")
    print("It is NOT financial advice. Recommendations are based on a")
    print("simplified analysis of recent news headlines.")
    print("="*60 + "\n")

    # --- MODIFIED: Dynamically set filenames based on screener status ---
    file_suffix = "_screened" if ENABLE_SCREENER else ""
    sentiment_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_gemini_sentiment.json")
    
    # --- Load the final sentiment data ---
    try:
        df = pd.read_json(sentiment_filepath)
        print(f"--- Loading data from: {sentiment_filepath} ---")
        print(f"Loaded {len(df)} analyzed articles for {STOCK_UNIVERSE.upper()}{file_suffix.replace('_', ' ').title()}.")
    except ValueError:
        print(f"Error: Could not read or parse the file at {sentiment_filepath}.")
        print("Please ensure sentiment_analysis.py ran successfully.")
        return
    except FileNotFoundError:
        print(f"Error: Sentiment data file not found at {sentiment_filepath}.")
        print(f"Please run data_collection.py and sentiment_analysis.py with ENABLE_SCREENER={ENABLE_SCREENER} first.")
        return

    if df.empty:
        print("Sentiment data file is empty. No recommendations to generate.")
        return

    # --- Calculate average sentiment score for each ticker ---
    avg_scores = df.groupby('ticker')['gemini_score'].mean()
    
    print(f"\n--- Recommendations based on Average Sentiment ---")
    print(f"(Buy >= {BUY_THRESHOLD}, Sell <= {SELL_THRESHOLD})\n")

    recommendations = []
    for ticker, score in avg_scores.items():
        recommendation = "No Action"
        if score >= BUY_THRESHOLD:
            recommendation = "Buy"
        elif score <= SELL_THRESHOLD:
            recommendation = "Sell"
        
        recommendations.append({
            "Ticker": ticker,
            "Avg Score": f"{score:.4f}",
            "Recommendation": recommendation
        })

    # --- Print the results in a clean table format ---
    if recommendations:
        recs_df = pd.DataFrame(recommendations)
        # Sort by ticker for consistent output
        recs_df = recs_df.sort_values(by="Ticker").reset_index(drop=True)
        print(recs_df.to_string(index=False))
    else:
        print("No tickers were processed.")


if __name__ == "__main__":
    generate_recommendations()