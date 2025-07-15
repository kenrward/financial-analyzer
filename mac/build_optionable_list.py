import os
import json
import logging
import asyncio
import httpx
from polygon import RESTClient
from dotenv import load_dotenv
from tqdm.asyncio import tqdm_asyncio
from typing import Union # ✅ Import Union for older Python versions

# --- Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")
OUTPUT_FILE = "optionable_tickers.json"
SEMAPHORE = asyncio.Semaphore(100) 
async_client = httpx.AsyncClient(timeout=30)


# ✅ Corrected function signature for Python 3.9
async def is_ticker_optionable(ticker: str) -> Union[str, None]:
    """
    Makes a single API call to get a ticker's details and checks if it's optionable.
    Returns the ticker symbol if optionable, otherwise None.
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    params = {"apiKey": API_KEY}
    
    async with SEMAPHORE:
        try:
            response = await async_client.get(url, params=params)
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            data = response.json()
            
            if data.get('results', {}).get('options', {}).get('optionable', False):
                return ticker
        except httpx.ReadTimeout:
            logging.warning(f"Read timeout for {ticker}. Skipping.")
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            
    return None


async def build_optionable_list():
    """
    Paginates through all active US stocks, gets full details for each,
    checks if they are optionable, and saves the list to a JSON file.
    """
    if not API_KEY:
        logging.error("POLYGON_API_KEY not found in .env file.")
        return

    logging.info("Initializing Polygon client and fetching all active US stock tickers...")
    
    sync_client = RESTClient(API_KEY)
    all_tickers = [t.ticker for t in sync_client.list_tickers(market="stocks", active=True)]
    logging.info(f"Found {len(all_tickers)} total active tickers. Now checking each for options status...")

    tasks = [is_ticker_optionable(ticker) for ticker in all_tickers]

    results = await tqdm_asyncio.gather(*tasks, desc="Checking tickers")

    optionable_tickers = sorted([res for res in results if res is not None])

    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, OUTPUT_FILE)
        
        with open(file_path, "w") as f:
            json.dump(optionable_tickers, f, indent=2)
        
        logging.info(f"✅ Success! Found {len(optionable_tickers)} optionable tickers.")
        logging.info(f"Master list saved to: {file_path}")

    except Exception as e:
        logging.error(f"Failed to save the list to a file: {e}")
    finally:
        await async_client.aclose()


if __name__ == "__main__":
    asyncio.run(build_optionable_list())