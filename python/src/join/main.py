import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.done_aggregations_by_client = {}
        self.tops_by_client = {}

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def process_messsage(self, message, ack, nack):
        try:
            [fruit_top, client_id] = message_protocol.internal.deserialize(message)
            top_by_client = self.tops_by_client.setdefault(client_id, [])
            top_by_client.extend(fruit_top)

            counter = self.done_aggregations_by_client.get(client_id, 0) + 1
            self.done_aggregations_by_client[client_id] = counter

            if counter == AGGREGATION_AMOUNT:
                logging.info(f"Sending top {TOP_SIZE} to client {client_id}")
                top_n = sorted(top_by_client, key=lambda x: x[1], reverse=True)[:TOP_SIZE]
                self.output_queue.send(message_protocol.internal.serialize([top_n, client_id]))
                if client_id in self.tops_by_client:
                    del self.tops_by_client[client_id]
                if client_id in self.done_aggregations_by_client:
                    del self.done_aggregations_by_client[client_id]
            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except middleware.MessageMiddlewareDisconnectedError:
        logging.error("Connection with middleware was lost")
        return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        try:
            join_filter.close()
        except Exception as e:
            logging.error(f"Error during close: {e}")
    return 0


if __name__ == "__main__":
    main()
