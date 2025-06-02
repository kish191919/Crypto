
# crypto_consumer.py
from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Consumer 설정
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# PostgreSQL 연결
conn = psycopg2.connect(
    dbname='cryptodb',      # 기존에 사용하던 데이터베이스 재사용 가능
    user='crypto',
    password='crypto123',
    host='localhost'
)
cur = conn.cursor()

for msg in consumer:
    data = msg.value
    symbol = data['symbol']
    timestamp = data['timestamp']
    price = data['price']

    # PostgreSQL에 저장
    cur.execute("""
        INSERT INTO crypto_prices (symbol, timestamp, price)
        VALUES (%s, %s, %s)
    """, (symbol, timestamp, price))

    conn.commit()
    print(f"Inserted: {symbol} @ {timestamp} = {price}")


