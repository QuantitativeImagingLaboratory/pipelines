from kafka import KafkaConsumer
import time
class p_consumer:
   def __init__(self, topic, bootstrap_servers='localhost:9092'):
       flag = 1
       while flag:
           try:
               self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
               flag = 0
           except:
               print("Exception in initlaizing kaffka producer")
               time.sleep(1)
               pass




   def get_next(self):
       for msg in self.consumer:
           key = (msg.key).decode('utf-8')
           message = (msg.value).decode('utf-8')
           yield key, message

