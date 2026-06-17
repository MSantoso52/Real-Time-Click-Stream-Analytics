# !/usr/bin/python

from datetime import datetime
import json
import sys
from decimal import Decimal
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic 
import clickhouse_driver

# Kafka configuration
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'sales_data'
KAFKA_GROUP = 'sales_consumer_group'

# Clickhouse configuration
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9002
CLICKHOUSE_DATABASE = 'sales'
CLICKHOUSE_TABLE = 'sales_data'
CLICKHOUSE_USER = 'consumer'
CLICKHOUSE_PASSWORD = 'clickhouse'

def ensure_topic_exists():
    """Ensure the Kafka topic exists, creating it if needed."""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    metadata = admin_client.list_topics(timeout=5)
    # `metadata.topics` is a dict of topic name -> TopicMetadata
    if KAFKA_TOPIC not in metadata.topics:
        print(f'Creating topic: {KAFKA_TOPIC}')
        # from confluent_kafka.admin import NewTopic
        new_topic = NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()  # Block until topic is created
                print(f'Topic {topic} created')
            except Exception as e:
                print(f'Failed to create topic {topic}: {e}')

def ensure_clickhouse_table():
    """Ensure the ClickHouse table exists with correct schema."""
    #client = clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    client = clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=10
    )

    create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE}(
            transaction_id String,
            item String,
            item_price Decimal(10, 2),
            qty_sale UInt32,
            sales_region String,
            timestamp DateTime
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, transaction_id)
    '''

    client.execute(create_table_sql)
    client.disconnect()
    print(f'Table {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} ensured')

def main():
    # Ensure infrastructure exist
    ensure_topic_exists()
    ensure_clickhouse_table()

    # Configure Consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])

    # Clickhouse client
    # ch_client = clickhouse_driver.Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    ch_client = clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=10
    )

    insert_query = f'''
    INSERT INTO {CLICKHOUSE_TABLE}
    (transaction_id, item, item_price, qty_sale, sales_region, timestamp)
    VALUES
    '''

    print('Consumer started ... Press Ctrl+C to stop')

    batch = []
    batch_size = 100

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f'Consumer error:{msg.error()}')
                    break    
            
            # Parse message
            #record = json.load(msg.value().decode('utf-8'))
            playload = msg.value().decode('utf-8')
            try:
                record = json.loads(playload)
            except json.JSONDecodeError as e:
                print(f'Failed to decode JSON from Kafka message: {e}')
                continue

            # Ensure the timestamp is a datetime object
            try:
            # Example: "2026-06-17T14:30:00"
                record["timestamp"] = datetime.fromisoformat(record["timestamp"])
            except (ValueError, AttributeError) as e:
                print(f"Invalid timestamp format: {e}")
                return
            
            batch.append((
                record['transaction_id'],
                record['item'],
                record['item_price'],
                record['qty_sale'],
                record['sales_region'],
                record['timestamp']
            ))

            # Insert batch when full
            if len(batch) >= batch_size:
                ch_client.execute(insert_query, batch)
                consumer.commit(msg)
                batch = []

    except KeyboardInterrupt:
        print('Consumer stop by user')

    finally:
        # Insert remaning record
        if batch:
            ch_client.execute(insert_query, batch)
        consumer.close()
        print('Consumer close')

if __name__ == "__main__":
    main()
