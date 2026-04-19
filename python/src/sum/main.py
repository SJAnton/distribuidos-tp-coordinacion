import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client = {}

        self.eof_input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{SUM_PREFIX}_{ID}"
        )
        self.eof_output_queues = []
        for j in range(SUM_AMOUNT):
            eof_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_{j}"
            )
            self.eof_output_queues.append(eof_output_queue)
        
    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()
        self.eof_input_queue.stop_consuming()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def _process_data(self, fruit, amount, client_id):
        logging.info(f"Process data")
        amount_by_fruit = self.amount_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        amount_by_fruit = self.amount_by_client.get(client_id, {})
        for final_fruit_item in amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [final_fruit_item.fruit, final_fruit_item.amount, client_id]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            logging.info("Sending EOF message to the other sum nodes")
            for eof_output_queue in self.eof_output_queues:
                eof_output_queue.send(
                    message_protocol.internal.serialize(fields)
                )
        ack()

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._process_eof(*fields)
        ack()

    def start(self):
        eof_handler_thread = threading.Thread(
            target=self.eof_input_queue.start_consuming,
            args=(self.process_eof_message,),
        )
        eof_handler_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)
        eof_handler_thread.join()

    def close(self):
        self.input_queue.close()
        self.eof_input_queue.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()
        for eof_output_queue in self.eof_output_queues:
            eof_output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
        sum_filter.close()
    except middleware.MessageMiddlewareDisconnectedError:
        logging.error("Connection with middleware was lost")
        return 1
    except Exception as e:
        logging.error(e)
        return 2
    return 0


if __name__ == "__main__":
    main()
