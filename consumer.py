from kafka import KafkaConsumer
from time import sleep
from json import loads

consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='consumer_group_1',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# https://stackoverflow.com/questions/46001807/dump-the-kafka-kafka-python-to-a-txt-file

message_buffer = []
buffer_size = 5000
batch_counter = 0

for message in consumer:
    message_buffer.append(message.value)
    if len(message_buffer) >= buffer_size:
        for _message in message_buffer:
            f = open('batch%s.csv' % batch_counter, 'a')
            f.write("%s\n" % _message)
            message_buffer = []
        batch_counter = batch_counter + 1