from collections import defaultdict
from datetime import datetime, timedelta
from utils import convert_date
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('time_chunk')

class TimeChunk(object): #, json.JSONEncoder):
  def __init__(self, size, start_date, **kwargs):
    # size of chunk in minutesi
    assert not 60 % size, "60 must be divisible by TimeChunk size"
    self.size = size
    if isinstance(start_date, datetime):
      self.start_date = start_date
    else:
      self.start_date = datetime.utcfromtimestamp(start_date)
    # when retrieved from kwargs we're not sure they are defaultdict
    self.terms_dict = defaultdict(int, kwargs.get('terms', defaultdict(int)))
    self.user_mentions = defaultdict(int, kwargs.get('user_mentions', defaultdict(int)))
    self.hashtags = defaultdict(int, kwargs.get('hashtags', defaultdict(int)))
    self.users = set(kwargs.get('users', set()))
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))
    self.changed_since_retrieval = False

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
    self.changed_since_retrieval = True

  def _update_list_generic(self, new_list, attr):
    for item in iter(new_list):
      attr[item] += 1

  def _update_dict_generic(self, new_dict, attr):
    for key in new_dict.iterkeys():
      attr[key] += new_dict[key]

  def pretty(self):
    return 'date: {0} size: {1} tweets: {2} users: {3} hashtags: {4} terms: {5}'.format(self.start_date, self.size, len(self.tweet_ids), len(self.users), sorted(self.hashtags.items(), key=lambda x: x[1], reverse=True)[:5], sorted(self.terms_dict.items(), key=lambda x: x[1], reverse=True)[:5])
# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'u fagget'}})

