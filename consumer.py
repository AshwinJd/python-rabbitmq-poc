import pika
import json

class Consumer:
  queuename = ""
  binding_key = ""
  config = {}
  def __init__(self, queuename, binding_key, config):
    self.queuename = queuename
    self.binding_key = binding_key
    self.config = config
    self.connection = self.create_connection()

  def create_connection(self):
    credentials = pika.PlainCredentials('root', 'root123')
    param = pika.ConnectionParameters(host=self.config["host"], port=self.config["port"], credentials=credentials)
    return pika.BlockingConnection(param)

  def on_message_callback(self, channel, method, properties, body):
    binding_key = method.routing_key
    print("Recieved message " + binding_key)
    data = json.loads(body)
    print(data)

  def setup(self):
    channel = self.connection.channel()
    channel.exchange_declare(
      exchange=self.config["exchange"],
      exchange_type="topic"
    )
    channel.queue_declare(queue=self.queuename)
    channel.queue_bind(queue=self.queuename, exchange=self.config["exchange"], routing_key=self.binding_key)
    channel.basic_consume(queue=self.queuename, on_message_callback=self.on_message_callback, auto_ack=True)
    print("[*] Waiting for data for " + self.queuename + ".To exit press CTRL+C")
    try:
      channel.start_consuming()
    except:
      channel.stop_consuming()


config = {
  "host": "localhost",
  "port": "5672",
  "exchange": "test_exchange"
}

queuename = "test_queue"

consumer = Consumer(queuename, "test.key", config)
consumer.setup()