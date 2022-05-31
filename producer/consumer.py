import binascii
from json import loads
from kafka import KafkaConsumer, TopicPartition
from sseclient import SSEClient as EventSource

BOOTSTRAP_SERVER = 'localhost:9092'
if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset='latest',
        # value_deserializer=lambda v: loads(v.decode('utf-8'))
    )
    partition = TopicPartition('output', 0)
    # partition = TopicPartition('output2', 0)
    consumer.assign([partition])
    # print('Consumer assigned partition:', partition)
    for message in consumer:
        # print(message.key, message.value)
        # string = str(message.key)
        # index = string.find('.org')
        # print(string[2:index + 4], int.from_bytes(message.value, "big"))
        # print(message.key.decode('utf-8'), int.from_bytes(message.value, "big"))
        obj = loads(message.value.decode('utf-8'))
        obj['hour'] = loads(obj['hour'])
        obj['day'] = loads(obj['day'])
        obj['week'] = loads(obj['week'])
        obj['month'] = loads(obj['month'])
        print(message.key.decode('utf-8'), obj)


# https://stream.wikimedia.org/v2/ui/#/?streams=page-create
# https://stream.wikimedia.org/v2/ui/#/?streams=recentchange
# https://stream.wikimedia.org/v2/ui/#/?streams=recentchange
