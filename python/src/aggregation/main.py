import os
import logging
import signal
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_counter_by_client = {}

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_exchange.stop_consuming()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _process_data(self, fruit, amount, client_id):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                addition = fruit_top[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                fruit_top.pop(i)
                bisect.insort(fruit_top, addition)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        eof_counter = self.eof_counter_by_client.get(client_id, 0) + 1
        self.eof_counter_by_client[client_id] = eof_counter
        if eof_counter < SUM_AMOUNT:
            return
        logging.info(f"Received EOF from all sum nodes for client {client_id}")
        fruit_top = self.fruit_top_by_client.get(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        total_fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([total_fruit_top, client_id]))
        if client_id in self.fruit_top_by_client:
            del self.fruit_top_by_client[client_id]
        if client_id in self.eof_counter_by_client:
            del self.eof_counter_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            logging.info("Processing message")
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._process_eof(*fields)
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except middleware.MessageMiddlewareDisconnectedError:
        logging.error("Connection with middleware was lost")
        return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        try:
            aggregation_filter.close()
        except Exception as e:
            logging.error(f"Error during close: {e}")
    return 0


if __name__ == "__main__":
    main()
