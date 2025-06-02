import psycopg2
from datetime import datetime
import time

def predict_and_store():
    try:
        conn = psycopg2.connect(
            dbname="cryptodb", user="crypto", password="crypto123", host="localhost"
        )
        cur = conn.cursor()

        def predict_price(symbol):
            cur.execute("""
                SELECT price
                FROM crypto_prices
                WHERE symbol = %s AND timestamp >= NOW() - INTERVAL '5 minutes'
            """, (symbol,))
            prices = [r[0] for r in cur.fetchall()]
            return sum(prices) / len(prices) if prices else None

        for symbol in ['BTC', 'ETH']:
            predicted = predict_price(symbol)
            if predicted:
                cur.execute("""
                    INSERT INTO crypto_prediction (symbol, timestamp, predicted_price)
                    VALUES (%s, %s, %s)
                """, (symbol, datetime.now(), predicted))
                print(f"[✓] Predicted {symbol}: {predicted}")

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[✗] Error: {e}")

# 🔁 10초마다 반복
while True:
    predict_and_store()
    time.sleep(10)

