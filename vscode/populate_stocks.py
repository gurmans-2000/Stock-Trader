import sqlite3, config
import alpaca_trade_api as tradeapi
connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""
    SELECT symbol, name FROM stock
""")
rows = cursor.fetchall()
symbols = [row['symbol'] for row in rows]
api = tradeapi.REST(config.API_KEY, config.API_SECRET, base_url=config.base_url)
assets = api.list_assets()
for asset in assets:
    try:
        if asset.status == 'active' and asset.tradable and asset.symbol not in symbols and asset.exchange == 'NASDAQ':
            print(f"Added a new stock {asset.symbol} {asset.name}")
            cursor.execute("INSERT INTO stock (symbol, name) VALUES (?, ?)", (asset.symbol, asset.name))
    except Exception as e:
        print(asset.symbol)
        print(e)
connection.commit()