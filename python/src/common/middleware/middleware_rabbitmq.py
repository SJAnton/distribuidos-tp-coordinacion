import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.consumer_tag = None
        
        self.channel.queue_declare(queue=queue_name)

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_qos(prefetch_count=1)

            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        if not self.consumer_tag:
            return
        try:
            self.channel.basic_cancel(consumer_tag=self.consumer_tag)
            self.channel.stop_consuming(consumer_tag=self.consumer_tag)
            self.consumer_tag = None
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consumer_tag = None

        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue

        for routing_key in routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_qos(prefetch_count=1)

            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        if not self.consumer_tag:
            return
        try:
            self.channel.basic_cancel(consumer_tag=self.consumer_tag)
            self.channel.stop_consuming(consumer_tag=self.consumer_tag)
            self.consumer_tag = None
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message, routing_key):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            raise MessageMiddlewareCloseError()
