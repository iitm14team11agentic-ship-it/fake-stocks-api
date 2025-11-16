import httpx
import time
from datetime import datetime, timedelta
from sqlmodel import Session, select

from main import engine, StockQuote 

ALPHA_VANTAGE_API_KEY = "JYBEAOEN7IGN9CW1" 

MASTER_SYMBOL_LIST = [
    "MSFT", "NVDA", "AAPL", "AMZN", "META", "AVGO", "GOOGL", "TSLA", "BRK.B", "GOOG",
    "JPM", "V", "LLY", "NFLX", "MA", "COST", "XOM", "WMT", "PG", "JNJ",
    "HD", "ABBV", "BAC", "UNH", "KO", "PM", "CRM", "ORCL", "CSCO", "GE",
    "PLTR", "IBM", "WFC", "ABT", "MCD", "CVX", "LIN", "NOW", "DIS", "ACN",
    "T", "ISRG", "MRK", "UBER", "GS", "INTU", "VZ", "AMD", "ADBE", "RTX",
    "PEP", "BKNG", "TXN", "QCOM", "PGR", "CAT", "SPGI", "AXP", "MS", "BSX",
    "BA", "TMO", "SCHW", "TJX", "NEE", "AMGN", "HON", "BLK", "C", "UNP",
    "GILD", "CMCSA", "AMAT", "ADP", "PFE", "SYK", "DE", "LOW", "ETN", "GEV",
    "PANW", "DHR", "COF", "TMUS", "MMC", "VRTX", "COP", "ADI", "MDT", "CB",
    "CRWD", "MU", "LRCX", "APH", "KLAC", "CME", "MO", "BX", "ICE", "AMT",
    "LMT", "SO", "PLD", "ANET", "BMY", "TT", "SBUX", "ELV", "FI", "DUK",
    "WELL", "MCK", "CEG", "INTC", "CDNS", "CI", "AJG", "WM", "PH", "MDLZ",
    "EQIX", "SHW", "MMM", "AOS", "AES", "AFL", "A", "APD", "ABNB", "AKAM",
    "ALB", "ARE", "ALGN", "ALLE", "LNT", "ALL", "AMCR", "AEE", "AEP", "AIG",
    "AWK", "AMP", "AME", "AON", "APA", "APO", "APP", "ADM", "AIZ", "ATO",
    "ADSK", "AEE", "AAL", "AEP", "AFG", "AGR", "AIRC", "ATO", "ATUS", "AVY",
    "BKR", "BALL", "BAC", "BBWI", "BAX", "BDX", "BBY", "BIO", "TECH", "BIIB",
    "BLK", "BWA", "BXP", "BSX", "BMY", "AVGO", "BR", "BRO", "BF.B", "BLDR",
    "BG", "CDNS", "CZR", "CPT", "CPB", "COF", "CAH", "KMX", "CCL", "CARR",
    "CTLT", "CAT", "CBOE", "CBRE", "CDW", "CE", "CNC", "CNP", "CF", "CHRW"
]
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

CALL_DELAY_SECONDS = 70 

# How old data can be before we refresh it (e.g., 24 hours)
STALE_THRESHOLD = timedelta(hours=24)
MAX_DAILY_CALLS = 25 # API limit

def fetch_from_alpha_vantage(symbol: str) -> dict:
    """Fetches a single, real quote from the free API."""
    print(f"--- Calling REAL API for {symbol} ---")
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        response = httpx.get(ALPHA_VANTAGE_URL, params=params, timeout=10)
        response.raise_for_status() # Raise error on 4xx/5xx responses
        data = response.json()
        
        # Check for valid data
        if "Global Quote" in data and data["Global Quote"]:
            return data["Global Quote"]
        elif "Note" in data:
            print(f"API Limit Hit or Note: {data['Note']}")
            return {"error": "API limit likely hit."}
        else:
            print(f"No 'Global Quote' data for {symbol}: {data}")
            return {"error": f"No data found for {symbol}"}
            
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return {"error": f"HTTP fetch error: {e}"}

def run_populator():
    print("Starting database populator...")
    call_count = 0
    
    with Session(engine) as session:
        for symbol in MASTER_SYMBOL_LIST:
            if call_count >= MAX_DAILY_CALLS:
                print(f"Reached {MAX_DAILY_CALLS} API calls. Stopping for today.")
                break
                
            # 1. Check if symbol is in DB and if data is stale
            statement = select(StockQuote).where(StockQuote.symbol == symbol)
            db_quote = session.exec(statement).first()
            
            needs_fetch = False
            if not db_quote:
                print(f"'{symbol}' not in DB. Marking for fetch.")
                needs_fetch = True
            elif (datetime.now() - db_quote.last_updated) > STALE_THRESHOLD:
                print(f"'{symbol}' data is stale. Marking for refresh.")
                needs_fetch = True
            else:
                print(f"'{symbol}' data is fresh. Skipping.")
                
            # 2. If we need to fetch, do it
            if needs_fetch:
                print(f"Waiting {CALL_DELAY_SECONDS}s before next call to avoid rate limit...")
                time.sleep(CALL_DELAY_SECONDS) 
                
                api_data = fetch_from_alpha_vantage(symbol)
                call_count += 1
                
                if "error" in api_data:
                    print(f"Could not update {symbol}. {api_data['error']}")
                    if "limit" in api_data["error"]:
                         print("API limit hit. Stopping script.")
                         break # Stop the whole script
                    continue # Skip to next symbol

                # 3. Save the good data to the database
                try:
                    # Strip the '%' sign from 'change percent'
                    change_pct_str = api_data["10. change percent"].rstrip('%')
                    
                    quote_to_save = StockQuote(
                        symbol=api_data["01. symbol"],
                        open=api_data["02. open"],
                        high=api_data["03. high"],
                        low=api_data["04. low"],
                        price=api_data["05. price"],
                        volume=api_data["06. volume"],
                        latest_trading_day=api_data["07. latest trading day"],
                        previous_close=api_data["08. previous close"],
                        change=api_data["09. change"],
                        change_percent=change_pct_str, # Save the stripped string
                        last_updated=datetime.now()
                    )
                    
                    # session.merge() is an "upsert"
                    # It will INSERT a new record or UPDATE an existing one
                    session.merge(quote_to_save) 
                    session.commit()
                    print(f"Successfully saved data for {symbol}.")
                    
                except Exception as e:
                    print(f"Error parsing or saving API data for {symbol}: {e}")
                    print(f"Raw data from API: {api_data}")

    print("Populator run finished.")

if __name__ == "__main__":
    run_populator()