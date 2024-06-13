from purchases import create_book_purchases
import random
import time
from utils.utils import load_data_from_file
from kafka_service.kafka_service import get_kafka_producer_config, produce_data_to_purchase_topic

def run_simulator(seconds_to_run:int):

    users = load_data_from_file(file_name='users')
    end_time = time.time() + seconds_to_run
    kafka_producer = get_kafka_producer_config()

    while True:
        user = random.choice(users)
        purchase = create_book_purchases(user)
        # ingesta data into kafka topic
        produce_data_to_purchase_topic(data=purchase,  producer=kafka_producer, )
        if time.time() > end_time: 
            break

if __name__ == '__main__':
    sec = 60 # run for 1 minute
    run_simulator(seconds_to_run=sec) 
