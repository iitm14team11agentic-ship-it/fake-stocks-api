import uvicorn
import random
from datetime import datetime
from fastapi import FastAPI, Query, HTTPException, Depends
from sqlmodel import Field, Session, SQLModel, create_engine, select
from typing import Optional, List, Dict, Any

class StockQuote(SQLModel, table=True):
    symbol: str = Field(primary_key=True)
    open: str
    high: str
    low: str
    price: str  # This is the 'base' close price
    volume: str
    latest_trading_day: str
    previous_close: str
    change: str
    change_percent: str
    last_updated: datetime = Field(index=True)

sqlite_file_name = "stocks.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

app = FastAPI()

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

def get_session():
    with Session(engine) as session:
        yield session

# --- 4. Helper Functions (UPDATED) ---

def transform_db_to_realtime_format(quote: StockQuote) -> Dict[str, Any]:
    """
    Transforms the DB record into the premium JSON format,
    WITH RANDOMIZED LIVE PRICES.
    """
    try:
        base_price = float(quote.price)
        prev_close = float(quote.previous_close)
        
        # Create a small random change (e.g., +/- 0.5%)
        change_percent = random.uniform(-0.05, 0.05) 
        new_price = base_price * (1 + change_percent)
        
        new_open = new_price * (1 + random.uniform(-0.01, 0.01))
        new_high = max(new_price, new_open) * (1 + random.uniform(0.001, 0.02))
        new_low = min(new_price, new_open) * (1 - random.uniform(0.001, 0.02))
        
        new_change = new_price - prev_close
        new_change_percent = (new_change / prev_close) * 100
        
        # 4. Create a live timestamp (e.g., "2025-11-16 11:07:46.123")
        live_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        return {
            "symbol": quote.symbol,
            "timestamp": live_timestamp,
            "open": f"{new_open:.4f}",
            "high": f"{new_high:.4f}",
            "low": f"{new_low:.4f}",
            "close": f"{new_price:.4f}", # This is our new randomized price
            "volume": quote.volume,      # We can just re-use the DB volume
            "previous_close": quote.previous_close,
            "change": f"{new_change:.4f}",
            "change_percent": f"{new_change_percent:.4f}",
            
            "extended_hours_quote": "N/A",
            "extended_hours_change": "N/A",
            "extended_hours_change_percent": "N/A"
        }
    except Exception as e:
        print(f"Error randomizing data for {quote.symbol}: {e}")
        # Fallback to just returning placeholder data
        return get_not_found_format(quote.symbol)

def get_not_found_format(symbol: str) -> Dict[str, Any]:
    """Returns a placeholder for a symbol we don't have in our DB."""
    return {
        "symbol": symbol, "timestamp": "N/A", "open": "N/A", "high": "N/A",
        "low": "N/A", "close": "N/A", "volume": "N/A", "previous_close": "N/A",
        "change": "N/A", "change_percent": "N/A", "extended_hours_quote": "N/A",
        "extended_hours_change": "N/A", "extended_hours_change_percent": "N/A"
    }

@app.get("/query")
async def get_batch_quotes(
    function: str = Query(...),
    symbols: str = Query(...),
    session: Session = Depends(get_session)
):
    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    data_list = []
    
    for symbol in symbol_list:
        statement = select(StockQuote).where(StockQuote.symbol == symbol)
        quote_from_db = session.exec(statement).first()
        
        if quote_from_db:
            data_list.append(transform_db_to_realtime_format(quote_from_db))
        else:
            data_list.append(get_not_found_format(symbol))

    final_response = {
        "endpoint": "Realtime Bulk Quotes",
        "message": "This is a FAKE API response. Prices are RANDOMLY generated.",
        "data": data_list
    }
    
    return final_response

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
