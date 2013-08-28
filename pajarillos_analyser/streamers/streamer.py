#import ujson as json
import simplejson as json
from messages.message_factory import MessageFactory


class Streamer(object):
  def __init__(self):
    self.mf = MessageFactory()

  def create_message(self, message, serialized=True):
    if serialized:
      deserialized_message = json.loads(message)
    else:
      deserialized_message = message
    return self.mf.create_message(deserialized_message)

