import ujson as json
from collections import defaultdict
from messages.message_factory import MessageFactory
from itertools import chain

class GenericAnalyser(object):
  # duplicates!
  def __init__(self):
    self.mf = MessageFactory()

  def process_file(self, file_name, process_types):
    for process_type in process_types:
      process_type(file_name)

  def process_generic(file_name):
    with open(file_name) as f:
      for line in f:
        self.create_message(line)._process()

  def create_message(self, serialized_message):
    deserialized_message = json.loads(serialized_message)
    return self.mf.create_message(deserialized_message)

  def process_by_time(self, file_name):
    tweets_date = defaultdict(set)
    with open(file_name) as f:
      for line in f:
        message = self.create_message(line)
        time_chunk = message._process_by_time(10)
        tweets_date[time_chunk].add(message)
    with open("spanish_processed", 'w') as f:
      for date in tweets_date:
        f.write("{0} SIZE: {1} MOST POPULAR: {2}".format(date, len(tweets_date[date]), self.get_terms_dict(tweets_date[date])))
        f.write('\n')
    return tweets_date

  def get_terms_dict(self, iterable):
    result = defaultdict(int)
    for tweet in iterable:
      tweet_dict = tweet.get_terms_dict()
      for word in tweet_dict:
        result[word] += tweet_dict[word]
    return sorted(result.items(), key=lambda x: x[1], reverse=True)[:5]

if __name__ == '__main__':
  a = GenericAnalyser()
  a.process_file('spanish_results', (a.process_by_time,))

