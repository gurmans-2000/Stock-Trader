from sqlite3.dbapi2 import Connection
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
import sqlite3, config


from datetime import date
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")



app = FastAPI()

@app.get("/")
def index(request: Request):

    stock_filter = request.query_params.get('filter', False)


    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    if stock_filter == 'new_closing_highs':
        cursor.execute("""
            select * from (
	            select symbol, name, stock_id, max(close), date
	            from stock_price join stock on stock.id = stock_price.stock_id
	            group by stock_id 
	            order by symbol
	        ) where date = ?
            """, (date.today().isoformat(),))
    elif stock_filter == 'new_closing_lows':
        cursor.execute("""
            select * from (
	            select symbol, name, stock_id, min(close), date
	            from stock_price join stock on stock.id = stock_price.stock_id
	            group by stock_id 
	            order by symbol
	        ) where date = ?
            """, (date.today().isoformat(),))
    else:
        cursor.execute("""
            SELECT id, symbol, name FROM stock ORDER BY symbol
        """)
    rows = cursor.fetchall()
    return templates.TemplateResponse("index.html", {"request": request, "stocks": rows})
    #return {"title": "Dashboard", "stocks": rows}

@app.get("/stock/{symbol}")
def stock_detail(request: Request, symbol):
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()


    cursor.execute("""
        SELECT * FROM strategy
    """)
    strategies = cursor.fetchall()

    cursor.execute("""
        SELECT id, symbol, name FROM stock WHERE symbol = ? 
    """, (symbol,))


    row = cursor.fetchone()

    cursor.execute("""
        SELECT * FROM stock_price WHERE stock_id = ? ORDER BY date DESC
    """, (row['id'],))


    prices = cursor.fetchall()
    return templates.TemplateResponse("stock_detail.html", {"request": request, "stock": row, "prices": prices, "strategies": strategies})
    

@app.post("/apply_strategy")
def apply_strategy(strategy_id: int = Form(...), stock_id: int = Form (...)):
    connection = sqlite3.connect(config.DB_FILE)
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO stock_strategy (stock_id, strategy_id) VALUES (?, ?)
    """, (stock_id, strategy_id))
    connection.commit()

    return RedirectResponse(url = f"/strategy/{strategy_id}", status_code = 303)


@app.get("/strategy/{strategy_id}")
def strategy(request: Request, strategy_id: int):
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row 

    cursor = connection.cursor()
    cursor.execute("""
        SELECT id, name 
        FROM strategy 
        WHERE id = ?
    """,(strategy_id,))
    strategy = cursor.fetchone()
    cursor.execute("""
        SELECT symbol, name 
        FROM stock JOIN stock_strategy on stock_strategy.stock_id = stock.id 
        WHERE strategy_id = ?
    """, (strategy_id,))
    stocks = cursor.fetchall()
    return templates.TemplateResponse("strategy.html", {"request": request, "stocks":stocks, "strategy":strategy})