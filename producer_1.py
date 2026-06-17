#!/usr/bin/python

'''
    kafka producer produce records every 5s
    limits records until 5M
'''

import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# kafka configuration
KAFKA_BROKER = 'localhost:29092'
KAFKA_TOPIC = 'sales_data'

# data configuration
ITEMS = ['computer','laptop','scanner','webcam','modem','keyboard', 'mouse','HDD','SSD','VGA','Notebook','Pencil','Ballpoint','Binder','Paper','Ruler']
REGION = ['North Region','Central Region','East Region','West Region','South Region']
MAX_RECORDS = 5_000_000
BATCH_SIZE = 100
INTERVAL_SECONDS = 5

def generate_sales_record(record_id: int) -> dict:
    '''generate single sales records'''
    return{
        'transaction_id' : f'TXN-{record_id:08d}',
        'item': random.choice(ITEMS),
        'item_price': round(random.uniform(10.0, 500.0), 2),
        'qty_sale': random.randint(1, 50),
        'sales_region': random.choice(REGION),
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }

def deliver_report(err, msg):
    '''callback kafka for delivery report'''
    if err is not None:
        print(f'Delivery error:{err}')

def main():
    '''configure producer'''
    producer = Producer({
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': 'sales-producer'
            })
    print(f'Starting producer ... Will generate up to {MAX_RECORDS:,} records')

    try:
        record_count = 0
        while record_count < MAX_RECORDS:
            # Produce batch of the records
            for _ in range(min(BATCH_SIZE, MAX_RECORDS - record_count)):
                record = generate_sales_record(record_count)
                producer.produce(
                    KAFKA_TOPIC,
                    key=record['transaction_id'],
                    value=json.dumps(record),
                    callback=deliver_report
                )
                record_count += 1

            # Flush messages
            producer.flush()

            if record_count % 1000 == 0:
                print(f'Produced {record_count:,} records')

            if record_count >= MAX_RECORDS:
                break

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print('\nProducer stopped by user')

    finally:
        producer.flush()
        print(f'Total records produced: {record_count:,}')

if __name__ == "__main__":
    main()
