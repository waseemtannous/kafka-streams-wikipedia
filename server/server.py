from json import loads, dumps
import os
from time import sleep

from kafka import KafkaConsumer, TopicPartition
from flask import Flask
from flask_cors import CORS

BOOTSTRAP_SERVER = os.environ.get('BOOTSTRAP_SERVER', 'localhost:9092')

app = Flask(__name__)
CORS(app)

consumer = None


# https://www.youtube.com/watch?v=rWIQLHp_JuU
@app.route('/stream')
def stream():
    def event_stream():
        global consumer
        for message in consumer:
            dict_message = {
                'key': message.key,
                'value': message.value
            }
            res = dumps(dict_message)
            yield 'data: {}\n\n'.format(res)

    return app.response_class(event_stream(), mimetype='text/event-stream')


@app.route('/test')
def test():
    return 'server is up'


if __name__ == '__main__':
    sleep(10)
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset='latest',
        value_deserializer=lambda v: loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8')
    )
    partition = TopicPartition('output', 0)
    consumer.assign([partition])

    app.run(port=8000, host='0.0.0.0')
