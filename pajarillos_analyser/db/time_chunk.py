from collections import defaultdict
from datetime import datetime, timedelta
from utils import convert_date, update_dict, update_set, CHUNK_DATA
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('time_chunk')

SUBCHUNK_SIZE = 100


class TimeChunkMgr(object):
  def load_chunk(self, chunk_dict):
    return TimeChunk(chunk_dict.pop('size'), chunk_dict.pop('start_date'), **chunk_dict)

  def reduce_chunks(self, chunk_iterable, postprocess=False):
    terms = defaultdict(int)
    user_mentions = defaultdict(int)
    hashtags = defaultdict(int)
    users = set()
    tweet_ids = set()
    for chunk_dict in chunk_iterable:
      update_dict(terms, chunk_dict['terms'])
      update_dict(hashtags, chunk_dict['hashtags'])
      update_dict(user_mentions, chunk_dict['user_mentions'])
      update_set(users, chunk_dict['users'])
      update_set(tweet_ids, chunk_dict['tweet_ids'])
    if postprocess:
      terms, user_mentions, hashtags, users, tweet_ids = \
                         self.postprocess_chunks(terms, user_mentions, hashtags, users, tweet_ids)
    keys = CHUNK_DATA
    values = (terms, user_mentions, hashtags, users, tweet_ids)
    return dict(zip(keys, values))

  def postprocess_chunks(self, terms, user_mentions, hashtags, users, tweet_ids):
    def get_sorted(iterable, trim=0):
      sorted_list = sorted(iterable.iteritems(), key=lambda x: x[1], reverse=True)
      if trim:
        return sorted_list[:trim]
      return sorted_list
    terms = dict(i for i in terms.iteritems() if not self.filter_term(i[0]))
    return (get_sorted(terms, 20),
            get_sorted(user_mentions, 10),
            get_sorted(hashtags, 10),
            len(users),
            len(tweet_ids))    

  def filter_term(self, term):
    if len(term) <= 2:
      return True
    filter_list = set(['ante', 'con', 'como', 'del', 'desde', 'entre', 'este', 'estas', 'estos', 'hacia',
                       'hasta', 'las', 'los', 'mas', 'nos', 'para', 'pero', 'por', 'que', 'segun', 'ser',
                       'sin', 'una', 'unas', 'uno', 'unos'])
    return term.lower() in filter_list


class SubChunk(object):
  def __init__(self, parent_chunk, **kwargs):
    self.parent_chunk = parent_chunk
    # when retrieved from kwargs we're not sure they are defaultdict
    self.terms = defaultdict(int, kwargs.get('terms', defaultdict(int)))
    self.user_mentions = defaultdict(int, kwargs.get('user_mentions', defaultdict(int)))
    self.hashtags = defaultdict(int, kwargs.get('hashtags', defaultdict(int)))
    self.users = set(kwargs.get('users', set()))
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))
    self.changed_since_retrieval = False

  def default(self):
    keys = CHUNK_DATA
    values = (self.terms, self.user_mentions, self.hashtags, list(self.users), list(self.tweet_ids))
    return dict(zip(keys, values))

  def update(self, message):
    self.tweet_ids.add(message.get_id())
    self._update_dict_generic(message.get_terms(), self.terms)
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

  def reduce_subchunks(self):
    return TimeChunkMgr().reduce_chunks([sc.default() for sc in self.subchunks])

  def pretty(self):
    results = self.reduce_subchunks()
    results[3] = len(results[3])
    return 'Most used terms: {0} \n Most popular hashtags: {1} \n Users with more mentions: {2} \n Number of users writing tweets: {3} \n Tweets written: {4}'.format(*results)
# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'u fagget'}})

