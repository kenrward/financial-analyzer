import os
import json
import logging
from polygon import RESTClient
from dotenv import load_dotenv
from tqdm import tqdm

# --- Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- Configuration ---
API_KEY = os.getenv("POLYGON_API_KEY")
OUTPUT_FILE = "optionable_tickers.json"

def build_optionable_list():
    """
    Paginates through all active US stocks on Polygon.io, checks if they are
    optionable, and saves the complete list to a JSON file.
    """
    if not API_KEY:
        logging.error("POLYGON_API_KEY not found in .env file.")
        return

    logging.info("Initializing Polygon client...")
    client = RESTClient(API_KEY)

    optionable_tickers = []
    
    try:
        logging.info("Fetching all active US stock tickers from Polygon.io...")
        # Get a paginated iterator for all active US stocks
        tickers_iterator = client.list_tickers(market="stocks", active=True, limit=1000)

        # Use tqdm for a progress bar, as this will take several minutes
        for ticker in tqdm(tickers_iterator, desc="Processing tickers"):
            # The client object has an 'options' attribute if the ticker is optionable
            if hasattr(ticker, 'options') and ticker.options.get('optionable', False):
                optionable_tickers.append(ticker.ticker)

    except Exception as e:
        logging.error(f"An error occurred during the API call: {e}")
        return

    # Sort the list alphabetically for cleanliness
    optionable_tickers.sort()

    # Save the final list to the JSON file
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, OUTPUT_FILE)
        
        with open(file_path, "w") as f:
            json.dump(optionable_tickers, f, indent=2)
        
        logging.info(f"✅ Success! Found {len(optionable_tickers)} optionable tickers.")
        logging.info(f"Master list saved to: {file_path}")

    except Exception as e:
        logging.error(f"Failed to save the list to a file: {e}")


if __name__ == "__main__":
    build_optionable_list()