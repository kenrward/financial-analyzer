# sentiment_analysis.py

import os
import json
import time
import pandas as pd
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import configuration from config.py ---
try:
    from config import GEMINI_API_KEY, DATA_DIRECTORY
except ImportError:
    print("Error: config.py not found. Please ensure it exists and is correctly set up.")
    exit()
except ValueError as e:
    print(f"Configuration Error: {e}")
    exit()

# --- Concurrency Configuration ---
# Set the number of parallel workers. Be mindful of your API's rate limits.
# A good starting point is between 10 and 30.
MAX_WORKERS = 20

def get_unprocessed_articles(raw_articles, existing_results):
    """
    Filters out articles that have already been processed and saved.

    Returns:
        list: A list of articles that still need to be analyzed.
    """
    existing_ids = set()
    # Create a unique identifier for each existing article
    # Using title and publication date is a robust way to do this.
    for article in existing_results:
        unique_id = (article.get('title'), article.get('published_utc'))
        existing_ids.add(unique_id)
        
    unprocessed = []
    for article in raw_articles:
        unique_id = (article.get('title'), article.get('published_utc'))
        if unique_id not in existing_ids:
            unprocessed.append(article)
            
    return unprocessed


def analyze_single_article(model, article):
    """
    Analyzes a single news article with Gemini and returns the enriched article.
    This function is designed to be called by a thread pool executor.
    """
    headline = article.get('title', '')
    summary = article.get('summary', '')
    
    if not headline:
        # If there's no headline, we can't analyze. Return the original article.
        return article

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
        response_text = response.text.strip().upper()
        
        score = 0
        explanation = "Could not parse explanation."

        lines = response_text.split('\n')
        first_line = lines[0].strip()

        if "YES" in first_line: score = 1
        elif "NO" in first_line: score = -1
        
        if len(lines) > 1: explanation = lines[1].strip()

        # Add the analysis to the article dictionary
        article['gemini_score'] = score
        article['gemini_explanation'] = explanation
        return article

    except Exception as e:
        print(f"  > Gemini API Error for '{headline[:30]}...': {e}")
        article['gemini_score'] = 0
        article['gemini_explanation'] = "Error in analysis"
        return article


if __name__ == "__main__":
    # Configure the Gemini client
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel('gemini-1.5-pro-latest')

    # Define file paths
    raw_news_filepath = os.path.join(DATA_DIRECTORY, "sp500_raw_news.json")
    analyzed_news_filepath = os.path.join(DATA_DIRECTORY, "sp500_gemini_sentiment.json")

    # --- Load Raw and Existing Data ---
    try:
        with open(raw_news_filepath, 'r') as f:
            all_raw_articles = json.load(f)
    except FileNotFoundError:
        print(f"Error: Raw news file not found at {raw_news_filepath}. Run data_collection.py first.")
        exit()

    existing_analyzed_articles = []
    if os.path.exists(analyzed_news_filepath):
        with open(analyzed_news_filepath, 'r') as f:
            existing_analyzed_articles = json.load(f)
        print(f"Found {len(existing_analyzed_articles)} previously analyzed articles.")

    # --- Caching Logic ---
    articles_to_process = get_unprocessed_articles(all_raw_articles, existing_analyzed_articles)
    
    if not articles_to_process:
        print("All articles have already been analyzed. Nothing to do.")
        exit()
        
    print(f"Starting analysis on {len(articles_to_process)} new articles...")
    
    # List to hold all results, both old and new
    final_results = existing_analyzed_articles
    
    # --- Parallel Processing ---
    # Using ThreadPoolExecutor to make concurrent API calls
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all analysis tasks to the executor
        future_to_article = {executor.submit(analyze_single_article, gemini_model, article): article for article in articles_to_process}
        
        # Process results as they are completed
        for i, future in enumerate(as_completed(future_to_article)):
            try:
                processed_article = future.result()
                final_results.append(processed_article)
                ticker = processed_article.get('ticker', 'N/A')
                score = processed_article.get('gemini_score', 'N/A')
                print(f"  ({i+1}/{len(articles_to_process)}) COMPLETED: Ticker {ticker}, Score: {score}")

            except Exception as e:
                print(f"An error occurred processing a future result: {e}")

    # --- Save Final Combined Results ---
    print(f"\nSaving a total of {len(final_results)} analyzed articles to {analyzed_news_filepath}...")
    # Sort by ticker and date for consistency
    final_results.sort(key=lambda x: (x.get('ticker'), x.get('published_utc')))
    with open(analyzed_news_filepath, 'w') as f:
        json.dump(final_results, f, indent=4)
        
    print("Sentiment analysis complete.")