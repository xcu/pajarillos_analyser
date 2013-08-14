from messages.tweet import Tweet
from messages.delete import Delete
from messages.scrub_geo import ScrubGeo
from messages.limit import Limit
from messages.status_withheld import StatusWithHeld
from messages.user_withheld import UserWithHeld
from messages.disconnect import Disconnect
from messages.warning import Warning


class MessageFactory(object):
  # https://dev.twitter.com/docs/streaming-apis/messages
  message_type_binds = {'text': Tweet,
                        'delete': Delete,
                        'scrub_geo': ScrubGeo,
                        'limit': Limit,
                        'status_withheld': StatusWithHeld,
                        'user_withheld': UserWithHeld,
                        'disconnect': Disconnect,
                        'warning': Warning,
                       }

  def create_message(self, value):
    for message_type in self.message_type_binds.iterkeys():
      if message_type in value:
        return self.message_type_binds[message_type](value)
    raise Exception('message type not found for message {0}'.format(value))

