#!/usr/bin/python3
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
print("Producer running---->")
for i in range(1000):
    producer.send('first_topic', key=str.encode('key_{}'.format(i)), value=b'some_message_bytes')
    print("Message sent! ---->")