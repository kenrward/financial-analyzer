# sentiment_analysis.py

import os
import json
import time
import pandas as pd
import google.generativeai as genai

# --- Import configuration from config.py ---
try:
    from config import GEMINI_API_KEY, DATA_DIRECTORY
except ImportError:
    print("Error: config.py not found. Please ensure it exists and is correctly set up.")
    exit()
except ValueError as e:
    print(f"Configuration Error: {e}")
    exit()

def analyze_sentiment_with_gemini(model, headline, summary):
    """
    Analyzes a news headline and summary using the Gemini model.

    Args:
        model: The initialized Gemini generative model.
        headline (str): The news headline.
        summary (str): The news summary.

    Returns:
        tuple: A tuple containing the sentiment score (1, -1, 0) and the explanation text.
               Returns (0, "Error in analysis") on failure.
    """
    # Using a prompt inspired by the research paper for clear instructions
    prompt = f"""
    Forget all your previous instructions. Pretend you are a financial expert. You are a financial expert with stock recommendation experience.
    Answer "YES" if the news is good, "NO" if the news is bad, or "UNKNOWN" if uncertain in the first line.
    Then elaborate with one short and concise sentence on the next line.
    
    Is this headline good or bad for the stock price of the company in the short term?

    Headline: {headline}
    Summary: {summary}
    """

    try:
        response = model.generate_content(prompt)
        
        # --- Parse the response ---
        # Clean up the response text
        response_text = response.text.strip().upper()
        
        score = 0
        explanation = "Could not parse explanation."

        lines = response_text.split('\n')
        first_line = lines[0].strip()

        if "YES" in first_line:
            score = 1
        elif "NO" in first_line:
            score = -1
        
        if len(lines) > 1:
            explanation = lines[1].strip()

        return score, explanation

    except Exception as e:
        print(f"  > Gemini API Error: {e}")
        return 0, "Error in analysis"

def process_news_file(model, input_filepath, output_filepath):
    """
    Loads raw news, analyzes sentiment for each item, and saves the results.
    """
    try:
        with open(input_filepath, 'r') as f:
            raw_news_data = json.load(f)
        print(f"Successfully loaded {len(raw_news_data)} news articles from {input_filepath}.")
    except FileNotFoundError:
        print(f"Error: Raw news file not found at {input_filepath}.")
        print("Please run data_collection.py first.")
        return

    # --- For development: test with a small sample first! ---
    # To run on all data, comment out the next line.
    # raw_news_data = raw_news_data[:15] 
    # print(f"--- RUNNING IN TEST MODE ON {len(raw_news_data)} ARTICLES ---")
    # ---------------------------------------------------------

    analyzed_results = []
    total_articles = len(raw_news_data)

    for i, article in enumerate(raw_news_data):
        headline = article.get('title', '')
        summary = article.get('summary', '')
        ticker = article.get('ticker', 'N/A')
        
        print(f"Analyzing article {i+1}/{total_articles} for ticker: {ticker}...")
        
        if not headline:
            print("  > Skipping article with no headline.")
            continue

        score, explanation = analyze_sentiment_with_gemini(model, headline, summary)
        
        # Add the new analysis to the original article data
        article['gemini_score'] = score
        article['gemini_explanation'] = explanation
        analyzed_results.append(article)
        
        print(f"  > Score: {score}, Explanation: {explanation}")
        
        # Be respectful of API rate limits
        time.sleep(1) # Pause for 1 second between calls

    print(f"\nSaving {len(analyzed_results)} analyzed articles to {output_filepath}...")
    with open(output_filepath, 'w') as f:
        json.dump(analyzed_results, f, indent=4)
    print("Sentiment analysis complete.")


if __name__ == "__main__":
    # Configure the Gemini client
    genai.configure(api_key=GEMINI_API_KEY)
    # Using a recent, powerful model
    gemini_model = genai.GenerativeModel('gemini-1.5-pro-latest')

    # Define file paths
    raw_news_filepath = os.path.join(DATA_DIRECTORY, "sp500_raw_news.json")
    analyzed_news_filepath = os.path.join(DATA_DIRECTORY, "sp500_gemini_sentiment.json")
    
    process_news_file(gemini_model, raw_news_filepath, analyzed_news_filepath)