from constants import BROKER_SERVER, BOOK_PURCHASING_KAFKA_TOPIC
import uuid
import json

def get_kafka_producer_config():
    producer_config = {
        'bootstrap.servers': BROKER_SERVER,
        'error_cb': lambda err: print(f'Kafka error >: {err}')
    }
    return producer_config


def verify_data_serialized(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(
        f'Object of type {obj.__class__.__name__} is not serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed {err}')
    else:
        print(f'Message delivered to {msg.topic()}[{msg.partition()}]')


def produce_data_to_topic(producer, topic, data):
    key = uuid.uuid4()
    producer.produce(
        topic,
        key=key,
        value=json.dumps(data, default=verify_data_serialized).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()


def produce_data_to_purchase_topic(producer, data):
    topic = BOOK_PURCHASING_KAFKA_TOPIC
    produce_data_to_topic(producer=producer, data=data, topic=topic)