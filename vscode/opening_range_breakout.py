import sqlite3
import config
import alpaca_trade_api as tradeapi
from datetime import date
from alpaca_trade_api.rest import REST
import pandas as pd
import smtplib, ssl

context = ssl.create_default_context()


connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row 
cursor = connection.cursor()

cursor.execute("""
    select id from strategy where name = 'opening_range_breakout'

""")

strategy_id = cursor.fetchone()['id']

cursor.execute("""
    select symbol, name
    from stock 
    join stock_strategy on stock_strategy.stock_id = stock.id 
    where stock_strategy.strategy_id = ?
""", (strategy_id,))



stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]
print(symbols)


api = tradeapi.REST(config.API_KEY, config.API_SECRET, base_url=config.base_url)
current_date = date.today().isoformat()

orders = api.list_orders(status='all', limit =500, after=f"{current_date}T13:30:00Z")
existing_order_symbols = [order.symbol for order in orders]




start_minute_bar = f"09:30:00-04:00"
end_minute_bar = f"09:45:00-04:00"
messages = []
for symbol in symbols:
    minute_bars = api.get_barset(symbol, '5Min', start=pd.Timestamp(current_date), end =pd.Timestamp(current_date)).df

    print("symbol")
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]
    print(opening_range_bars)
    opening_range_low = opening_range_bars[symbol]['low'].min()
    opening_range_high = opening_range_bars[symbol]['high'].max()
    opening_range = opening_range_high - opening_range_low
    print(opening_range_low)
    print(opening_range_high)
    print(opening_range)
    after_opening_mask = minute_bars.index >= end_minute_bar 
    after_opening_range_bars = minute_bars.loc[after_opening_mask]
    print(after_opening_range_bars)

    after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars[symbol]['close'] > opening_range_high]

    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:
            limit_price = after_opening_range_breakout[symbol]['close'][0]
            
            messages.append(f"Placing order for {symbol} at {limit_price}, closed above {opening_range_high}\n\n{after_opening_range_breakout.index[0]}\n\n")

            print(f"Placing order for {symbol} at {limit_price}, closed above {opening_range_high} at {after_opening_range_breakout.index[0]}")
            api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='100',
                time_in_force='day',
                order_class='bracket',
                limit_price = limit_price,
                take_profit=dict(
                    limit_price=limit_price + opening_range,
                ),
                stop_loss=dict(
                    stop_price=limit_price - opening_range,
                )
            )
        else:
            print("You already are in a position for {symbol}, skipping")

print(messages)
with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
    server.login(config.EMAIL_ADD, config.EMAIL_PASS)
    email_message = f'Subject: Trade Notifications for {current_date}\n\n'
    email_message += "\n\n".join(messages)
    server.sendmail(config.EMAIL_ADD, config.EMAIL_ADD, email_message)
    #server.sendmail(config.EMAIL_ADD, config.number, email_message)