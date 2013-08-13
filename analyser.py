import json
import simplejson
from messages.message_factory import MessageFactory

class GenericAnalyser(object):
  # duplicates!
  def __init__(self):
    self.mf = MessageFactory()

  def analyse_file(self, file_name):
    with open(file_name) as f:
      for line in f:
        self.analyse_message(line)

  def analyse_message(self, serialized_message):
    #deserialized_message = json.loads(serialized_message)
    deserialized_message = simplejson.loads(serialized_message)
    message = self.mf.create_message(deserialized_message)
    message.process()

if __name__ == '__main__':
  a = GenericAnalyser()
  a.analyse_file('results')

