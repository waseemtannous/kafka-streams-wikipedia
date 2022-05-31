from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from json import dumps, loads
from variables import *
from sseclient import SSEClient as EventSource

import time


def serializer(message):
    return message.encode('utf-8')


topics = []

if __name__ == '__main__':
    # topicList = [
    #     NewTopic(name='pageCreate', num_partitions=4, replication_factor=1)
    # ]
    adminClient = KafkaAdminClient(bootstrap_servers='localhost:9092')

    # adminClient.create_topics(new_topics=topicList, validate_only=False)

    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=serializer)

    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    source = EventSource(url)
    value = 0
    for event in source:
        if event.event == 'message':
            try:
                data = loads(event.data)
            except ValueError:
                pass
            else:
                topic = data['server_name']
                producer.send("topics", value=data)
                continue
                print(topic)
                if topic not in topics:
                    adminClient.create_topics(new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)],
                                              validate_only=False)
                    topics.append(topic)
                    producer.send("topics", value=topic)

                producer.send(topic, value=str(value))
                value += 1

    # for event in source:
    #     if event.event == 'message':
    #         try:
    #             data = loads(event.data)
    #         except ValueError:
    #             pass
    #         else:
    #             topic = 'topics'
    #             # print(topic)
    #             # if topic not in topics:
    #             #     adminClient.create_topics(new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)],
    #             #                               validate_only=False)
    #             #     topics.append(topic)
    #             #     producer.send("topics", value=topic)
    #
    #             producer.send(topic, value=str(value))
    #             value += 1
