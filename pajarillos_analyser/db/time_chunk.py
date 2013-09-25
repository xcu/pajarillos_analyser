from collections import defaultdict
from datetime import datetime, timedelta
from utils import get_top_occurrences, sorted_list_from_dict, \
                  convert_date, update_dict, update_set, CHUNK_DATA
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('time_chunk')

SUBCHUNK_SIZE = 100


class TimeChunkMgr(object):
  def load_chunk(self, chunk_dict):
    return TimeChunk(chunk_dict.pop('size'),
                     self.get_date_from_db_key(chunk_dict.pop('start_date')),
                     **chunk_dict)

  def get_top_occurrences(self, subchunk_list, number_of_occurrences):
    ''' returns the top number_of_occurrences out of subchunk_list for each
        dictionary in the subchunk -namely, the terms, the hashtags and the user mentions
    '''
    occurrence_keys = ('terms', 'user_mentions', 'hashtags')
    results = {}
    for occurrence_key in occurrence_keys:
      dicts = [sc[occurrence_key] for sc in subchunk_list]
      results[occurrence_key] = get_top_occurences(number_of_occurrences,
                                                   dicts,
                                                   [sorted_list_from_dict(d) for d in dicts])
    return results

  def reduce_chunks(self, chunk_iterable, postprocess=False):
    ''' reduces an iterable of chunks to a single chunk, merging their contents.
        Each chunk included in the iterable is expected to be a dictionary
    '''
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

  def get_date_db_key(self, date):
    return convert_date(date)

  def get_date_from_db_key(self, key):
    return datetime.utcfromtimestamp(start_date)


class FullSubChunk(SubChunk):
  ''' when a subchunk is full fast lookup to perform updates quickly stop being important
      Instead, we need to keep records sorted to be able to do the mergesort algorithm
      as fast as possible.
  '''
  def __init__(self, parent_chunk, **kwargs):
    self.parent_chunk = parent_chunk
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))
    assert self.is_full(), "subchunk is not full {0}".format(len(self.tweet_ids))
    self.users = set(kwargs.get('users', set()))
    self.changed_since_retrieval = False
    self.terms = kwargs.get('terms', [])
    self.user_mentions = kwargs.get('user_mentions', [])
    self.hashtags = kwargs.get('hashtags', [])

  def update(self, message):
    raise Exception("Full subchunk cannot be updated")

  def default(self):
    keys = CHUNK_DATA
    values = ((t.default() for t in self.terms),
             (t.default() for t in self.user_mentions),
             (t.default() for t in self.hashtags),
             list(self.users),
             list(self.tweet_ids))
    return dict(zip(keys, values))

class SubChunk(object):
  def __init__(self, parent_chunk, **kwargs):
    self.parent_chunk = parent_chunk
    self.obj_id = kwargs.get('obj_id', None)
    # when retrieved from kwargs we're not sure they are defaultdict
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))
    self.users = set(kwargs.get('users', set()))
    self.changed_since_retrieval = False
    self.terms = defaultdict(int, kwargs.get('terms', defaultdict(int)))
    self.user_mentions = defaultdict(int, kwargs.get('user_mentions', defaultdict(int)))
    self.hashtags = defaultdict(int, kwargs.get('hashtags', defaultdict(int)))

  def is_full(self):
    return len(self.tweet_ids) >= SUBCHUNK_SIZE

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
    self.start_date = start_date
    self.complete_subchunks = []
    subchunks = kwargs.get('complete_subchunks', [])
    for subchunk in subchunks:
      self.complete_subchunks.append(SubChunk(self.start_date, **subchunk))
    self.changed_since_retrieval = False

  def get_db_key(self):
    return TimeChunkMgr().get_date_db_key(self.start_date)

  def default(self):
    # json.loads(aJsonString, object_hook=json_util.object_hook)
    # json.dumps(self.start_date, default=json_util.default)
    return {'size': self.size,
            'start_date': self.get_db_key(),
            'subchunk_size': SUBCHUNK_SIZE,
            'complete_subchunks': [sb.default() for sb in self.complete_subchunks]}

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
    #subchunk = self.get_first_subchunk(message)
    self.current_subchunk.update(message)
    self.changed_since_retrieval = True

  def current_subchunk_isfull(self):
    return self.current_chunk.is_full()

  def update_current_subchunk(self, id_in_db):
    self.complete_subchunks.append(id_in_db)
    self.current_subchunk = SubChunk(self.start_date)

  def is_duplicate(self, message):
    # membership test is O(1) on average in sets, this should be cheap
    return any(subchunk.is_duplicate(message) for subchunk in self.complete_subchunks)

  def get_first_subchunk(self, message):
    for subchunk in self.complete_subchunks:
      if not subchunk.is_full():
        return subchunk
    new_chunk = SubChunk(self.start_date)
    self.complete_subchunks.append(new_chunk)
    return new_chunk

  def reduce_subchunks(self):
    return TimeChunkMgr().reduce_chunks([sc.default() for sc in self.complete_subchunks])

  def pretty(self):
    results = self.reduce_subchunks()
    results[3] = len(results[3])
    return 'Most used terms: {0} \n Most popular hashtags: {1} \n Users with more mentions: {2} \n Number of users writing tweets: {3} \n Tweets written: {4}'.format(*results)
# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'u fagget'}})

