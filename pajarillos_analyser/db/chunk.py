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
  '''
  this class should be completely decoupled from the DB layer. All it provides for
  that purpose are methods to get the db key for a container
  '''
  def load_empty_chunk_container(self, size, sdate):
    # sdate is a datetime object
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
      dicts = [getattr(chunk, dict_key) for chunk in chunk_list]
      lists = [getattr(chunk, list_key) for chunk in chunk_list]
      results[dict_key] = get_top_occurrences(number_of_occurrences, dicts, lists)
    return results

  def filter_term(self, term):
    if len(term) <= 2:
      return True
    filter_list = set(['ante', 'con', 'como', 'del', 'desde', 'entre', 'este', 'estas', 'estos', 'hacia',
                       'hasta', 'las', 'los', 'mas', 'nos', 'para', 'pero', 'por', 'que', 'segun', 'ser',
                       'sin', 'una', 'unas', 'uno', 'unos'])
    return term.lower() in filter_list

  def get_chunk_id_in_db(self, date):
    ''' expects a datetime object '''
    return convert_date(date)

  def get_chunk_att_from_db_id(self, key):
    ''' expects db id, returns datetime obj '''
    return datetime.utcfromtimestamp(key)

  def get_container_db_index_key(self):
    return 'start_date'

  def get_chunk_db_index_key(self):
    return '_id'


class ChunkContainer(object):
  def __init__(self, size, start_date, **kwargs):
    # size of container in minutes
    assert not 60 % size, "60 must be divisible by ChunkContainer size"
    self.size = size
    self.start_date = start_date
    # key: ObjectId, val: Chunk obj
    self.chunks = kwargs.get('chunks', {})
    # kwargs['current_chunk'] is the id in the db
    self.current_chunk = kwargs.get('current_chunk')
    if self.current_chunk:
      # (id in db, chunk object)
      self.current_chunk = (self.current_chunk, None)
    else:
      # (nothing in db yet. fresh obj)
      self.current_chunk = (None, self.get_new_current_chunk())
    self.changed_since_retrieval = False

  def num_tweets(self):
    return sum((c.num_tweets() for c in self.chunks))

  def num_users(self):
    return sum((c.num_users() for c in self.chunks))

  def get_db_key(self):
    return ChunkMgr().get_chunk_id_in_db(self.start_date)

  def default(self):
    ''' json dictionary with the object representation to be stored in the db.
    Current chunk only cares about the db id, so '''
    return {'size': self.size,
            'start_date': self.get_db_key(),
            'chunk_size': CHUNK_SIZE,
            'chunks': self.chunks.keys(),
            'current_chunk': self.current_chunk[0]}

  def tweet_fits(self, tweet):
    # returns True if the tweet is inside the container window
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    creation_time = reset_seconds(tweet.get_creation_time())
    lower_bound = self.start_date
    upper_bound = self.start_date + timedelta(minutes=self.size)
    return lower_bound <= creation_time and creation_time < upper_bound

  def update(self, message):
    # updates the container with the new message
    if message in self:
      logger.info('duplicated tweet: {0} not processing!'.format(message.get_id()))
      return
    self._update_current_chunk(message)
    self.changed_since_retrieval = True

  def _update_current_chunk(self, message):
    # updates current chunk values with the new message
    current_chunk_obj = self.current_chunk[1]
    current_chunk_obj.update(message)

  def current_chunk_isfull(self):
    return self.current_chunk[1].is_full()

  def store_current_chunk(self, id_in_db):
    # puts current_chunk in the chunks list and creates a new one
    self.chunks[id_in_db] = self.current_chunk[1]
    self.current_chunk = (None, self.get_new_current_chunk())

  def get_new_current_chunk(self):
    return ChunkMgr().load_chunk(self.start_date, {})

  def __contains__(self, message):
    # membership test is O(1) on average in sets, this should be cheap
    return any(message in chunk for chunk in self.chunks.itervalues())


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
    self.deserialize_sorted_lists()

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

  def serialize_sorted_lists(self):
    #the output of sorted_dicts has some sets that need to be transformed into something different
    for attr in ('sorted_terms', 'sorted_user_mentions', 'sorted_hashtags'):
        setattr(self, attr, [(num, list(occ)) for num, occ in getattr(self, attr)])

  def deserialize_sorted_lists(self):
    #convert back lists into sets
    for attr in ('sorted_terms', 'sorted_user_mentions', 'sorted_hashtags'):
        setattr(self, attr, [(num, set(occ)) for num, occ in getattr(self, attr)])

  def default(self):
    # json reprentation with the desired format to store the chunk in the DB
    self.sorted_dicts()
    self.serialize_sorted_lists()
    keys = CHUNK_DATA
    values = (self.parent_container, self.terms, self.sorted_terms,
              self.user_mentions, self.sorted_user_mentions,
              self.hashtags, self.sorted_hashtags, list(self.users), list(self.tweet_ids))
    return dict(zip(keys, values))

  def update(self, message):
    # updates current chunk with the message passed
    self.tweet_ids.add(message.get_id())
    self._update_dict_generic(message.get_terms(), self.terms)
    self._update_list_generic(message.get_user_mentions(), self.user_mentions)
    self._update_list_generic(message.get_hashtags(), self.hashtags)
    self.users.add(message.get_user())
    self.changed_since_retrieval = True

  def __contains__(self, message):
    return message.get_id() in self.tweet_ids

  def _update_list_generic(self, new_list, attr):
    for item in iter(new_list):
      attr[item] += 1

  def _update_dict_generic(self, new_dict, attr):
    for key in new_dict.iterkeys():
      attr[key] += new_dict[key]

  def num_tweets(self):
    return len(self.tweet_ids)

  def num_users(self):
    return len(self.users)
