# fake-stocks-api
fake stocks api bulk call emulation

commands that you might need:-
pip install fastapi uvicorn sqlmodel httpx
uvicorn main:app --reload
python populate.py

test:-
http://127.0.0.1:8000/query?function=BATCH_STOCK_QUOTES&symbols=AAPL,MSFT,IBM
