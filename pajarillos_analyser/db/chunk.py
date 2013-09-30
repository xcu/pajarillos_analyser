from collections import defaultdict
from datetime import datetime, timedelta
from utils import get_top_occurrences, sorted_list_from_dict,\
                  convert_date, update_dict, update_set, CHUNK_DATA
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('chunk_container')

CHUNK_SIZE = 100


class ChunkMgr(object):
  def load_empty_chunk_container(self, size, sdate):
    return self.load_chunk_container({'size': size, 'start_date': sdate})

  def load_chunk_container(self, container_dict):
    # here start_date is expected to be a datetime object
    return ChunkContainer(container_dict.pop('size'),
                          container_dict.pop('start_date'),
                          **container_dict)

  def load_chunk(self, parent_container, chunk_dict):
    return Chunk(parent_container, **chunk_dict)

  def get_top_occurrences(self, chunk_list, number_of_occurrences):
    ''' returns the top number_of_occurrences out of chunk_list for each
        dictionary in the chunk -namely, the terms, the hashtags and the user mentions
    '''
    occurrence_keys = (('terms', 'sorted_terms'),
                       ('user_mentions', 'sorted_user_mentions'),
                       ('hashtags', 'sorted_hashtags'))
    results = {}
    for dict_key, list_key in occurrence_keys:
      dicts = [sc[dict_key] for sc in chunk_list]
      lists = [sc[list_key] for sc in chunk_list]
      results[dict_key] = get_top_occurrences(number_of_occurrences, dicts, lists)
    return results

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
    return datetime.utcfromtimestamp(key)

#  def reduce_chunks(self, chunk_iterable, postprocess=False):
#    ''' reduces an iterable of chunks to a single chunk, merging their contents.
#        Each chunk included in the iterable is expected to be a dictionary
#    '''
#    terms = defaultdict(int)
#    user_mentions = defaultdict(int)
#    hashtags = defaultdict(int)
#    users = set()
#    tweet_ids = set()
#    for chunk_dict in chunk_iterable:
#      update_dict(terms, chunk_dict['terms'])
#      update_dict(hashtags, chunk_dict['hashtags'])
#      update_dict(user_mentions, chunk_dict['user_mentions'])
#      update_set(users, chunk_dict['users'])
#      update_set(tweet_ids, chunk_dict['tweet_ids'])
#    if postprocess:
#      terms, user_mentions, hashtags, users, tweet_ids = \
#                         self.postprocess_chunks(terms, user_mentions, hashtags, users, tweet_ids)
#    keys = CHUNK_DATA
#    values = (terms, user_mentions, hashtags, users, tweet_ids)
#    return dict(zip(keys, values))


class Chunk(object):
  def __init__(self, parent_container, **kwargs):
    self.parent_container = parent_container
    self.obj_id = kwargs.get('obj_id', None)
    # when retrieved from kwargs we're not sure they are defaultdict
    self.tweet_ids = set(kwargs.get('tweet_ids', set()))
    self.users = set(kwargs.get('users', set()))
    self.changed_since_retrieval = False
    self.terms = defaultdict(int, kwargs.get('terms', defaultdict(int)))
    self.sorted_terms = kwargs.get('sorted_terms', [])
    self.user_mentions = defaultdict(int, kwargs.get('user_mentions', defaultdict(int)))
    self.sorted_user_mentions = kwargs.get('sorted_user_mentions', [])
    self.hashtags = defaultdict(int, kwargs.get('hashtags', defaultdict(int)))
    self.sorted_hashtags = kwargs.get('sorted_hashtags', [])

  def is_full(self):
    return len(self.tweet_ids) >= CHUNK_SIZE

  def sorted_dicts(self):
    # if needed, creates a sorted list out of the dictionaries
    def need_to_recalculate():
      return self.changed_since_retrieval or any(not d for d in (self.sorted_terms,
                                                                 self.sorted_user_mentions,
                                                                 self.sorted_hashtags))
    if need_to_recalculate():
      self.sorted_terms = sorted_list_from_dict(self.terms)
      self.sorted_user_mentions = sorted_list_from_dict(self.user_mentions)
      self.sorted_hashtags = sorted_list_from_dict(self.hashtags)
    return self.sorted_terms, self.sorted_user_mentions, self.sorted_hashtags

  def make_sorted_lists_serializable(self):
    #the output of sorted_dicts has some sets that need to be transformed into something different
    for attr in ('sorted_terms', 'sorted_user_mentions', 'sorted_hashtags'):
        setattr(self, attr, [(num, list(occ)) for num, occ in getattr(self, attr)])

  def default(self):
    # this class needs a change_since_retrieval instead of force
    self.sorted_dicts()
    self.make_sorted_lists_serializable()
    keys = CHUNK_DATA
    values = (self.parent_container, self.terms, self.sorted_terms,
              self.user_mentions, self.sorted_user_mentions,
              self.hashtags, self.sorted_hashtags, list(self.users), list(self.tweet_ids))
    return dict(zip(keys, values))

  def update(self, message):
    self.tweet_ids.add(message.get_id())
    self._update_dict_generic(message.get_terms(), self.terms)
    self._update_list_generic(message.get_user_mentions(), self.user_mentions)
    self._update_list_generic(message.get_hashtags(), self.hashtags)
    self.users.add(message.get_user())
    self.changed_since_retrieval = True

  def is_duplicate(self, message):
    return message.get_id() in self.tweet_ids

  def _update_list_generic(self, new_list, attr):
    for item in iter(new_list):
      attr[item] += 1

  def _update_dict_generic(self, new_dict, attr):
    for key in new_dict.iterkeys():
      attr[key] += new_dict[key]


class ChunkContainer(object):
  def __init__(self, size, start_date, **kwargs):
    # size of container in minutes
    assert not 60 % size, "60 must be divisible by ChunkContainer size"
    self.size = size
    self.start_date = start_date
    # key: ObjectId, val: Chunk obj
    self.chunks = kwargs.get('chunks', {})
    self.current_chunk = kwargs.get('current_chunk', (None, self.get_new_current_chunk()))
    self.changed_since_retrieval = False

  def get_db_key(self):
    return ChunkMgr().get_date_db_key(self.start_date)

  def default(self):
    # DB only cares about the chunk index
    return {'size': self.size,
            'start_date': self.get_db_key(),
            'chunk_size': CHUNK_SIZE,
            'chunks': self.chunks.keys(),
            'current_chunk': self.current_chunk[0] if self.current_chunk else None}

  def tweet_fits(self, tweet):
    # returns True if the tweet is inside the time chunk window
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    creation_time = tweet.get_creation_time()
    delta = timedelta(minutes=creation_time.minute % self.size)
    return reset_seconds(creation_time - delta) == self.start_date

  def update_current_chunk(self, message):
    # updates current chunk values with the new message
    current_chunk_obj = self.current_chunk[1]
    current_chunk_obj.update(message)

  def update(self, message):
    if self.is_duplicate(message):
      logger.info('duplicated tweet: {0} not processing!'.format(message.get_id()))
      return
    self.update_current_chunk(message)
    self.changed_since_retrieval = True

  def current_chunk_isfull(self):
    return self.current_chunk[1].is_full()

  def store_current_chunk(self, id_in_db):
    # puts current_chunk in the chunks list and creates a new one
    self.chunks[id_in_db] = self.current_chunk[1]
    self.current_chunk = (None, self.get_new_current_chunk())

  def get_new_current_chunk(self):
    return ChunkMgr().load_chunk(self.start_date, {})

  def is_duplicate(self, message):
    # membership test is O(1) on average in sets, this should be cheap
    return any(chunk.is_duplicate(message) for _, chunk in self.chunks.iteritems())

  def reduce_chunks(self):
    return ChunkMgr().reduce_chunks([sc.default() for sc in self.chunks])

  def pretty(self):
    results = self.reduce_chunks()
    results[3] = len(results[3])
    return 'Most used terms: {0} \n Most popular hashtags: {1} \n Users with more mentions: {2} \n Number of users writing tweets: {3} \n Tweets written: {4}'.format(*results)
# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'u fagget'}})

