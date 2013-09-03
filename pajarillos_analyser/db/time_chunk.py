from collections import defaultdict
from datetime import datetime, timedelta
from utils import convert_date
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('time_chunk')

SUBCHUNK_SIZE = 100

class SubChunk(object):
  def __init__(self, parent_chunk, **kwargs):
    self.parent_chunk = parent_chunk
    # when retrieved from kwargs we're not sure they are defaultdict
    self.terms_dict = defaultdict(int, kwargs.get('terms', defaultdict(int)))
    self.user_mentions = defaultdict(int, kwargs.get('user_mentions', defaultdict(int)))
    self.hashtags = defaultdict(int, kwargs.get('hashtags', defaultdict(int)))
    self.users = set(kwargs.get('users', set()))
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))
    self.changed_since_retrieval = False

  def default(self):
    return {'terms': self.terms_dict,
            'user_mentions': self.user_mentions,
            'hashtags': self.hashtags,
            'users': list(self.users),
            'tweet_ids': list(self.tweet_ids)}

  def update(self, message):
    self.tweet_ids.add(message.get_id())
    self._update_dict_generic(message.get_terms_dict(), self.terms_dict)
    self._update_list_generic(message.get_user_mentions(), self.user_mentions)
    self._update_list_generic(message.get_hashtags(), self.hashtags)
    self.users.add(message.get_user())

  def is_duplicate(self, message):
    return message.get_id() in self.tweet_ids

  def _update_list_generic(self, new_list, attr):
    for item in iter(new_list):
      attr[item] += 1

  def _update_dict_generic(self, new_dict, attr):
    for key in new_dict.iterkeys():
      attr[key] += new_dict[key]


class TimeChunk(object):
  def __init__(self, size, start_date, **kwargs):
    # size of chunk in minutes
    assert not 60 % size, "60 must be divisible by TimeChunk size"
    self.size = size
    if isinstance(start_date, datetime):
      self.start_date = start_date
    else:
      self.start_date = datetime.utcfromtimestamp(start_date)
    self.subchunks = []
    subchunks = kwargs.get('subchunks', [])
    for subchunk in subchunks:
      self.subchunks.append(SubChunk(convert_date(self.start_date), **subchunk))
    self.changed_since_retrieval = False

  def default(self):
    # json.loads(aJsonString, object_hook=json_util.object_hook)
    # json.dumps(self.start_date, default=json_util.default)
    return {'size': self.size,
            'start_date': convert_date(self.start_date),
            'subchunk_size': SUBCHUNK_SIZE,
            'subchunks': [sb.default() for sb in self.subchunks]}

  def tweet_fits(self, tweet):
    # returns True if the tweet is inside the time chunk window
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    creation_time = tweet.get_creation_time()
    delta = timedelta(minutes=creation_time.minute % self.size)
    return reset_seconds(creation_time - delta) == self.start_date

  def update(self, message):
    if self.is_duplicate(message):
      logger.info('duplicated tweet: {0} not processing!'.format(message.get_id()))
      return
    subchunk = self.get_first_subchunk(message)
    subchunk.update(message)
    self.changed_since_retrieval = True

  def is_duplicate(self, message):
    # membership test is O(1) on average in sets, this should be cheap
    return any(subchunk.is_duplicate(message) for subchunk in self.subchunks)

  def get_first_subchunk(self, message):
    for subchunk in self.subchunks:
      if len(subchunk.tweet_ids) < SUBCHUNK_SIZE:
        return subchunk
    new_chunk = SubChunk(convert_date(self.start_date))
    self.subchunks.append(new_chunk)
    return new_chunk

  def pretty(self):
    return 'date: {0} size: {1} tweets: {2} users: {3} hashtags: {4} terms: {5}'.format(self.start_date, self.size, len(self.tweet_ids), len(self.users), sorted(self.hashtags.items(), key=lambda x: x[1], reverse=True)[:5], sorted(self.terms_dict.items(), key=lambda x: x[1], reverse=True)[:5])
# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'u fagget'}})

