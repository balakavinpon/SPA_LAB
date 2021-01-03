 #!/usr/bin/python3
from kafka import KafkaConsumer
consumer = KafkaConsumer('first_topic', bootstrap_servers='localhost:9092')
print("Consumer running---->")
for message in consumer:
    print(message)