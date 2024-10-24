from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ['localhost:9092']

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
)
print(consumer.topics())



