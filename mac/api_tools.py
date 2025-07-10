# In api_tools.py

# ... (all other code and imports remain the same) ...

# --- The "Super-Tool" - Now with Rate Limiting ---
async def _find_and_analyze_active_stocks(limit: int = 5) -> str:
    log.info(f"🚀 Kicking off full analysis for top {limit} stocks")
    
    active_stocks_data = await _get_most_active_stocks(limit)
    if "error" in active_stocks_data or not active_stocks_data.get("top_stocks"):
        return json.dumps({"error": "Could not retrieve active stocks."})

    active_stocks = active_stocks_data["top_stocks"]
    price_lookup = {stock['ticker']: stock.get('close_price') for stock in active_stocks}
    log.info(f"Found {len(active_stocks)} active stocks. Filtering for optionable tickers...")

    # --- ✅ RATE LIMITING LOGIC ---
    # Process optionable checks sequentially to respect the 5 calls/minute limit.
    optionable_tickers = []
    for stock in active_stocks:
        ticker = stock['ticker']
        log.info(f"Checking optionability for {ticker}...")
        
        is_optionable = await _is_ticker_optionable(ticker)
        if is_optionable:
            log.info(f"✅ {ticker} is optionable.")
            optionable_tickers.append(ticker)
        else:
            log.info(f"Skipping {ticker} (not optionable).")
            
        # Wait for 13 seconds to stay under the 5 calls/minute limit (60 / 5 = 12)
        log.info("Waiting 13 seconds before next check...")
        await asyncio.sleep(13)

    log.info(f"Found {len(optionable_tickers)} optionable stocks: {optionable_tickers}")

    if not optionable_tickers:
        return json.dumps([], indent=2)

    # Concurrently fetch analysis and news for the filtered list
    # This part remains fast because it calls your local APIs, not Polygon
    log.info("Fetching analysis and news for optionable stocks...")
    analysis_tasks = {ticker: _get_and_analyze_ticker(ticker) for ticker in optionable_tickers}
    news_tasks = {ticker: _get_news_for_ticker(ticker) for ticker in optionable_tickers}
    
    analysis_results = await asyncio.gather(*analysis_tasks.values())
    news_results = await asyncio.gather(*news_tasks.values())
    
    analysis_map = {ticker: res for ticker, res in zip(analysis_tasks.keys(), analysis_results)}
    news_map = {ticker: res for ticker, res in zip(news_tasks.keys(), news_results)}

    final_results = []
    for ticker in optionable_tickers:
        final_results.append({
            "ticker": ticker,
            "price": price_lookup.get(ticker, "N/A"),
            "technical_analysis": analysis_map.get(ticker),
            "news": news_map.get(ticker)
        })

    return json.dumps(final_results, indent=2)