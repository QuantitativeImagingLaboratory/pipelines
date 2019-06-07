from kafka import KafkaConsumer

class p_consumer:
   def __init__(self, topic, bootstrap_servers='localhost:9092'):
      self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')


   def get_next(self):
       for msg in self.consumer:
           key = (msg.key).decode('utf-8')
           message = (msg.value).decode('utf-8')
           yield key, message

