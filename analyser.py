import json

class GenericAnalyser(object):

  # https://dev.twitter.com/docs/streaming-apis/messages
  message_type_handlers = {'delete': self.process_delete,
                           'scrub_geo': self.process_scrub_geo,
                           'limit': self.process_limit,
                           'status_withheld': self.process_status_withheld,
                           'user_withheld': self.process_user_withheld,
                           'disconnect': self.process_disconnect,
                           'warning': self.process_warning,
                          }
  # duplicates!

  def analyse(self, serialized_dict):
    deserialized_dict = json.loads(serialized_dict)
    message_type = get_message_type(deserialized_dict)
    
  def get_message_type(message_dict):
    pass 
