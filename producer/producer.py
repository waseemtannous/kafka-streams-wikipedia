import os

from kafka import KafkaProducer
from json import dumps, loads
from sseclient import SSEClient as EventSource
from kafka.errors import NoBrokersAvailable

import time

BOOTSTRAP_SERVER = os.environ.get('BOOTSTRAP_SERVER', 'localhost:9092')


def valueSerializer(data):
    return dumps(data).encode('utf-8')


def keySerializer(data):
    return data.encode('utf-8')


def create_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,
                                 value_serializer=valueSerializer,
                                 key_serializer=keySerializer)
    except NoBrokersAvailable:
        print('No broker found at {}'.format(BOOTSTRAP_SERVER))
        exit(1)
        return None

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def constructEvent(data):
    return {
        'serverName': data['server_name'],
        'eventType': data['type'],
        'bot': data['bot'],
        'user': data['user'],
        'page': data['meta']['uri'],
    }


if __name__ == '__main__':
    time.sleep(10)
    producer = create_kafka_producer()

    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    print('Connecting to {}'.format(url))

    for event in EventSource(url):
        if event.event == 'message':
            try:
                data = loads(event.data)
            except ValueError:
                pass
            else:
                eventToSend = constructEvent(data)

                producer.send('recentChange', value=eventToSend, key=data['server_name'])
                time.sleep(0.1)
