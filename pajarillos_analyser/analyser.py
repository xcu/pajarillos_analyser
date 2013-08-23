import calendar as cal
#import ujson as json
import simplejson as json
from bson import json_util
from pymongo import MongoClient
from collections import defaultdict
from messages.message_factory import MessageFactory
from datetime import datetime, timedelta
import logging
logging.basicConfig(filename='analyser.log',level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('analyser')

TIMECHUNK_SIZE = 1

def convert_date(date):
  if date.utcoffset():
    date = date - date.utcoffset()
  return int(cal.timegm(date.timetuple()))


class GenericAnalyser(object):
  # duplicates!
  def __init__(self):
    self.mf = MessageFactory()
    self.client = MongoClient('localhost', 27017)
    self.db = self.client.test_database
    self.collection = self.client.test_database.time_chunks
    self.collection.drop()
    self.collection.create_index('start_date', unique=True)

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
    return tweets_date

  def get_time_chunk(self, start):
    if not self.chunk_exists(start):
      return TimeChunk(TIMECHUNK_SIZE, start)
    chunk_dict = self._get_chunk(start).next()
    return TimeChunk(chunk_dict.pop('size'), chunk_dict.pop('start_date'), **chunk_dict)

  def insert_time_chunks(self, file_name):
    current_time_chunk = None
    with open(file_name) as f:
      for line in f:
        message = self.create_message(line)
        if not current_time_chunk:
          current_time_chunk = self.get_time_chunk(message._process_by_time(TIMECHUNK_SIZE))
        if not current_time_chunk.tweet_fits(message):
          logger.info("saving chunk in db because key {0} doesnt match tweet with date {1}".format(current_time_chunk.start_date, message.get_creation_time()))
          self.save_chunk(current_time_chunk)
          current_time_chunk = self.get_time_chunk(message._process_by_time(TIMECHUNK_SIZE))
        current_time_chunk.update(message)
    self.save_chunk(current_time_chunk)

  def all_ids_from_db(self):
    all_ids = set()
    for chunk_dict in self.collection.find():
      c = TimeChunk(chunk_dict.pop('size'), chunk_dict.pop('start_date'), **chunk_dict)
      all_ids.update(c.tweet_ids)
    return all_ids

  def save_chunk(self, chunk):
    key = convert_date(chunk.start_date)
    logger.info("updating db with key {0}. Chunk with size {1}".format(key, len(chunk.tweet_ids)))
    self.collection.update({'start_date': key}, chunk.default(), upsert=True)

  def chunk_exists(self, start_time):
    return self._get_chunk(start_time).count()

  def _get_chunk(self, start_time):
    time_chunk = self.collection.find({'start_date': convert_date(start_time)})
    return time_chunk

  def get_terms_dict(self, iterable):
    result = defaultdict(int)
    for tweet in iter(iterable):
      tweet_dict = tweet.get_terms_dict()
      for word in tweet_dict.iterkeys():
        result[word] += tweet_dict[word]
    return sorted(result.items(), key=lambda x: x[1], reverse=True)[:5]

  def dump_db_info(self):
    with open("spanish_processed", 'w') as f:
      for chunk_dict in self.collection.find():
        chunk = TimeChunk(chunk_dict.pop('size'), chunk_dict.pop('start_date'), **chunk_dict)
        f.write(chunk.pretty())
        f.write('\n')


class TimeChunk(object): #, json.JSONEncoder):
  def __init__(self, size, start_date, **kwargs):
    # size of chunk in minutesi
    assert not 60 % size, "60 must be divisible by TimeChunk size"
    self.size = size
    if isinstance(start_date, datetime):
      self.start_date = start_date
    else:
      self.start_date = datetime.utcfromtimestamp(start_date))
    # when retrieved from kwargs we're not sure they are defaultdict
    self.terms_dict = defaultdict(int, kwargs.get('terms', defaultdict(int)))
    self.user_mentions = defaultdict(int, kwargs.get('user_mentions', defaultdict(int)))
    self.hashtags = defaultdict(int, kwargs.get('hashtags', defaultdict(int)))
    self.users = set(kwargs.get('users', set()))
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))

  def default(self):
    #overrides the one in JSONEncoder
    # json.loads(aJsonString, object_hook=json_util.object_hook)
    # json.dumps(self.start_date, default=json_util.default)
    return {'size': self.size,
            'start_date': convert_date(self.start_date),
            'terms': self.terms_dict,
            'user_mentions': self.user_mentions,
            'hashtags': self.hashtags,
            'users': list(self.users),
            'tweet_ids': list(self.tweet_ids)}

  def tweet_fits(self, tweet):
    # returns True if the tweet is inside the time chunk window
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    creation_time = tweet.get_creation_time()
    delta = timedelta(minutes=creation_time.minute % self.size)
    return reset_seconds(creation_time - delta) == self.start_date

  def update(self, message):
    previous_size = len(self.tweet_ids)
    self.tweet_ids.add(message.get_id())
    if len(self.tweet_ids) == previous_size:
      logger.info('duplicated tweet: {0} not processing!'.format(message.get_id()))
      return
    self._update_dict_generic(message.get_terms_dict(), self.terms_dict)
    self._update_list_generic(message.get_user_mentions(), self.user_mentions)
    self._update_list_generic(message.get_hashtags(), self.hashtags)
    self.users.add(message.get_user())

  def _update_list_generic(self, new_list, attr):
    for item in iter(new_list):
      attr[item] += 1

  def _update_dict_generic(self, new_dict, attr):
    for key in new_dict.iterkeys():
      attr[key] += new_dict[key]

  def pretty(self):
    return 'date: {0} size: {1} tweets: {2} users: {3} hashtags: {4} terms: {5}'.format(self.start_date, self.size, len(self.tweet_ids), len(self.users), sorted(self.hashtags.items(), key=lambda x: x[1], reverse=True)[:5], sorted(self.terms_dict.items(), key=lambda x: x[1], reverse=True)[:5])
    

# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'u fagget'}})

if __name__ == '__main__':
  a = GenericAnalyser()
  #a.process_file('spanish_results', (a.process_by_time,))
  a.insert_time_chunks('spanish_results')
  a.dump_db_info()

