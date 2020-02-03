from kafka import KafkaProducer
from pipelinetypes import KEY_MESSAGE

class p_producer:
    def __init__(self, topic, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, compression_type='gzip', max_request_size=3173440261)
        self.topic = topic

    def publish(self, msg, key = KEY_MESSAGE):
        self.producer.send(self.topic, key=key.encode('utf-8'), value=msg.encode('utf-8'))


    def close(self):
        self.producer.close()
