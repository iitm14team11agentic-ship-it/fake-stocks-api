import uvicorn
from fastapi import FastAPI, Query, HTTPException, Depends
from sqlmodel import Field, Session, SQLModel, create_engine, select
from typing import Optional, List, Dict, Any
from datetime import datetime

class StockQuote(SQLModel, table=True):
    symbol: str = Field(primary_key=True)
    open: str
    high: str
    low: str
    price: str  # This will be 'close' in the output
    volume: str
    latest_trading_day: str
    previous_close: str
    change: str
    change_percent: str  # Stored *without* the '%'
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

# This is a "dependency" that gives our API endpoint a DB session
def get_session():
    with Session(engine) as session:
        yield session

def transform_db_to_realtime_format(quote: StockQuote) -> Dict[str, Any]:
    """Transforms our DB record into the premium JSON format."""
    
    # The free API only gives a date, so we append a fake time.
    fake_timestamp = f"{quote.latest_trading_day} 00:00:00.000"
    
    return {
        "symbol": quote.symbol,
        "timestamp": fake_timestamp,
        "open": quote.open,
        "high": quote.high,
        "low": quote.low,
        "close": quote.price,  # Map 'price' (from free API) to 'close'
        "volume": quote.volume,
        "previous_close": quote.previous_close,
        "change": quote.change,
        "change_percent": quote.change_percent,
        
        # Add 'N/A' placeholders for data the free API doesn't provide
        "extended_hours_quote": "N/A",
        "extended_hours_change": "N/A",
        "extended_hours_change_percent": "N/A"
    }

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
    """
    This fake endpoint emulates 'Realtime Bulk Quotes' by reading
    *ONLY* from the local stocks.db database.
    """
    
    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    data_list = []
    
    for symbol in symbol_list:
        statement = select(StockQuote).where(StockQuote.symbol == symbol)
        quote_from_db = session.exec(statement).first()
        
        if quote_from_db:
            print(f"DB HIT for {symbol}")
            data_list.append(transform_db_to_realtime_format(quote_from_db))
        else:
            # Not in our DB
            print(f"DB MISS for {symbol}")
            data_list.append(get_not_found_format(symbol))

    final_response = {
        "endpoint": "Realtime Bulk Quotes",
        "message": "This is a FAKE API response emulating the premium endpoint. Data is from the local 'stocks.db' cache.",
        "data": data_list
    }
    
    return final_response

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)