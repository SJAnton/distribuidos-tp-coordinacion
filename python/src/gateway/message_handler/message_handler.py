from common import message_protocol
from uuid import uuid4

class MessageHandler:

    def __init__(self):
        self.uid = str(uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self.uid])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.uid])

    def deserialize_result_message(self, message):
        [fruit_top, client_id] = message_protocol.internal.deserialize(message)
        return fruit_top if self.uid == client_id else None
