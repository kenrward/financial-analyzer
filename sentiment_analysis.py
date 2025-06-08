# sentiment_analysis.py

import os
import json
import time
import random
import pandas as pd
import google.generativeai as genai
import google.api_core.exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import configuration from config.py ---
try:
    # ADDED ENABLE_SCREENER to the import
    from config import GEMINI_API_KEY, DATA_DIRECTORY, STOCK_UNIVERSE, ENABLE_SCREENER
except ImportError:
    print("Error: config.py not found or is missing variables.")
    exit()
except ValueError as e:
    print(f"Configuration Error: {e}")
    exit()

# --- Concurrency Configuration ---
MAX_WORKERS = 10

def get_unprocessed_articles(raw_articles, existing_results):
    """Filters out articles that have already been processed and saved."""
    existing_ids = {(article.get('title'), article.get('published_utc')) for article in existing_results}
    unprocessed = [article for article in raw_articles if (article.get('title'), article.get('published_utc')) not in existing_ids]
    return unprocessed

def analyze_single_article(model, article):
    """Analyzes a single news article with Gemini and includes robust retry logic."""
    headline = article.get('title', '')
    summary = article.get('summary', '')
    if not headline: return article

    prompt = f"""
    Forget all your previous instructions. Pretend you are a financial expert. You are a financial expert with stock recommendation experience.
    Answer "YES" if the news is good, "NO" if the news is bad, or "UNKNOWN" if uncertain in the first line.
    Then elaborate with one short and concise sentence on the next line.
    Is this headline good or bad for the stock price of the company in the short term?
    Headline: {headline}
    Summary: {summary}
    """
    
    max_retries = 4
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            response_text = response.text.strip().upper()
            score, explanation = 0, "Could not parse explanation."
            lines = response_text.split('\n')
            first_line = lines[0].strip()
            if "YES" in first_line: score = 1
            elif "NO" in first_line: score = -1
            if len(lines) > 1: explanation = lines[1].strip()
            article['gemini_score'] = score
            article['gemini_explanation'] = explanation
            return article
        except google.api_core.exceptions.ResourceExhausted as e:
            wait_time = (2 ** attempt) * 5 + random.uniform(0, 1)
            print(f"  > Rate limit hit for '{headline[:30]}...'. Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"  > An unexpected Gemini API Error for '{headline[:30]}...': {e}")
            break
            
    print(f"  > Failed to process article for '{headline[:30]}...' after {max_retries} attempts.")
    article['gemini_score'] = 0
    article['gemini_explanation'] = "Failed after multiple retries"
    return article

if __name__ == "__main__":
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel('gemini-1.5-pro-latest')

    # --- MODIFIED: Dynamically set filenames based on screener status ---
    file_suffix = "_screened" if ENABLE_SCREENER else ""
    raw_news_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_raw_news.json")
    analyzed_news_filepath = os.path.join(DATA_DIRECTORY, f"{STOCK_UNIVERSE}{file_suffix}_gemini_sentiment.json")
    
    print(f"--- Starting Sentiment Analysis for: {STOCK_UNIVERSE.upper()}{file_suffix.replace('_', ' ').title()} ---")
    print(f"Input file: {raw_news_filepath}")

    try:
        with open(raw_news_filepath, 'r') as f: all_raw_articles = json.load(f)
    except FileNotFoundError:
        print(f"Error: Raw news file not found. Run data_collection.py with ENABLE_SCREENER={ENABLE_SCREENER} first.")
        exit()

    existing_analyzed_articles = []
    if os.path.exists(analyzed_news_filepath):
        with open(analyzed_news_filepath, 'r') as f: existing_analyzed_articles = json.load(f)
        print(f"Found {len(existing_analyzed_articles)} previously analyzed articles in {analyzed_news_filepath}.")

    articles_to_process = get_unprocessed_articles(all_raw_articles, existing_analyzed_articles)
    
    if not articles_to_process:
        print("All articles have already been analyzed. Nothing to do.")
        exit()
        
    print(f"Starting analysis on {len(articles_to_process)} new articles...")
    final_results = existing_analyzed_articles
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_article = {executor.submit(analyze_single_article, gemini_model, article): article for article in articles_to_process}
        for i, future in enumerate(as_completed(future_to_article)):
            try:
                processed_article = future.result()
                final_results.append(processed_article)
                ticker = processed_article.get('ticker', 'N/A')
                score = processed_article.get('gemini_score', 'N/A')
                print(f"  ({i+1}/{len(articles_to_process)}) COMPLETED: Ticker {ticker}, Score: {score}")
            except Exception as e:
                print(f"An error occurred processing a future result: {e}")

    print(f"\nSaving a total of {len(final_results)} analyzed articles to {analyzed_news_filepath}...")
    final_results.sort(key=lambda x: (x.get('ticker'), x.get('published_utc')))
    with open(analyzed_news_filepath, 'w') as f:
        json.dump(final_results, f, indent=4)
        
    print("Sentiment analysis complete.")