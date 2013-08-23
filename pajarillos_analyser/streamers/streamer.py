#import ujson as json
import simplejson as json
from messages.message_factory import MessageFactory


class Streamer(object):
  def __init__(self):
    self.mf = MessageFactory()

  def create_message(self, serialized_message):
    deserialized_message = json.loads(serialized_message)
    return self.mf.create_message(deserialized_message)

  def messages(self):
    pass

