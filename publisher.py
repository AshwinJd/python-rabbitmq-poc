import pika
import json

class Publisher:
  config = {}
  def __init__(self,config):
    self.config = config

  def publish(self, routing_key, message):
    connection = self.create_connections()

    channel = connection.channel()
    channel.exchange_declare(exchange=self.config["exchange"], exchange_type='topic')
    channel.basic_publish(exchange=self.config["exchange"], routing_key=routing_key, body=message)
    print("[x] Sent message % r for % r" % (message, routing_key))

  def create_connections(self):
    credentials = pika.PlainCredentials('root','root123')
    param = pika.ConnectionParameters(host=self.config["host"], port=self.config["port"], credentials=credentials)
    return pika.BlockingConnection(param)


config = {
  "host": "localhost",
  "port": "5672",
  "exchange": "test_exchange"
}
publisher = Publisher(config)
data = {
  "name": "john",
  "age": 23
}

publisher.publish("test.key", json.dumps(data))