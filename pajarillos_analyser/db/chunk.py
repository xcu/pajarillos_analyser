from collections import defaultdict
from datetime import datetime, timedelta
from db_manager import ContainerDB, ChunkDB
from utils import get_top_occurrences, sorted_list_from_dict, CHUNK_DATA
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('chunk_container')


class ObjMgr(object):
  @property
  def size(self):
    raise NotImplementedError

  def save_in_db(self, object_to_save):
    raise NotImplementedError

  def get_obj(self, json_obj):
    raise NotImplementedError

  def get_empty_obj(self, *mandatory_args):
    raise NotImplementedError

  def load_obj_from_id(self, id_in_db):
    raise NotImplementedError

  def load_obj_from_json(self, json_obj):
    raise NotImplementedError

  def get_db_index_key(self):
    raise NotImplementedError


class ContainerMgr(ObjMgr):
  def __init__(self, conn, db_name):
    self.dbmgr = ContainerDB(conn, db_name, self.get_db_index_key())
    self.chunkmgr = ChunkMgr(conn, db_name)

  @property
  def size(self):
    return 1

  def save_in_db(self, container):
    # chunks will be object ids already, no worries about them
    # current chunk needs to get its sorted lists recalculated
    # and update its entry in the db (fetching by its object id)
    if container.current_chunk:
      json_chunk = container.current_chunk.default()
      if container.current_chunk.obj_id:
        self.chunkmgr.dbmgr.update_obj(json_chunk)
      else:
        container.current_chunk.obj_id = self.chunkmgr.dbmgr.save_chunk(json_chunk)
    self.dbmgr.update_obj(container.default())

  def get_obj(self, json_container):
    return ChunkContainer(self,
                          json_container.get('size'),
                          json_container.get('start_date'),
                          json_container.get('chunks', []),
                          json_container.get('current_chunk'))

  def get_empty_obj(self, size, sdate):
    # sdate is a datetime object
    return self.get_obj({'size': size, 'start_date': sdate})

  def load_obj_from_id(self, start):
    '''
    loads the time chunk from the db if it exists or creates a new one
    @param start datetime object to match a key in the db
    '''
    logger.info("trying to get chunk container from date {0}".format(start))
    container = self.dbmgr.load_json_from_id(start)
    if container:
      logger.info("Container found, retrieving from database")
      return self.load_obj_from_json(container)
    else:
      logger.info("Container not found, creating an empty one")
      return self.get_empty_obj(self.size, start)

  def load_obj_from_json(self, container_dict):
    # returns a ChunkContainer object out from the provided dictionary
    # here start_date is expected to be the key in the db
    container = self.get_obj(container_dict)
    self._set_container_fields_from_json(container)
    return container

  def _set_container_fields_from_json(self, container):
    ''' after fetching a container from the db and initializing the container
    object some of their fields need to be translated from DB representation
    (like ids in the DB) to actual information to be used by the object '''
    container.start_date = self.dbmgr.id_in_db_to_fieldval(container.start_date)
    # container.current_chunk is only an ObjectId at this point
    current_chunk_id = container.current_chunk
    if current_chunk_id:
      container.current_chunk = self.chunkmgr.load_obj_from_id(current_chunk_id)
    container.chunks = [self.load_obj_from_id(chunk_id) for
                                              chunk_id in container.chunks]

  def get_db_index_key(self):
    return 'start_date'


class ChunkContainer(object):
  def __init__(self, mgr, size, start_date, chunks, current_chunk):
    # size of container in minutes
    assert not 60 % size, "60 must be divisible by ChunkContainer size"
    self.mgr = mgr
    self.size = size
    self.start_date = start_date
    # list
    self.chunks = chunks
    self.current_chunk = current_chunk
    if not self.current_chunk:
      self.current_chunk = self.get_new_current_chunk()
    self.changed_since_retrieval = False

  def num_tweets(self):
    return sum((c.num_tweets() for c in self.chunks))

  def num_users(self):
    return sum((c.num_users() for c in self.chunks))

  def default(self):
    ''' json dictionary with the object representation to be stored in the db.
    Current chunk only cares about the db id, so '''
    return {'size': self.size,
            'start_date': self.start_date,
            'chunk_size': self.mgr.chunkmgr.size,
            'chunks': [chunk.obj_id for chunk in self.chunks],
            'current_chunk': self.current_chunk.obj_id}

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
    self.current_chunk.update(message)
    self.changed_since_retrieval = True

  def current_chunk_isfull(self):
    return self.current_chunk.is_full()

  def set_current_chunk(self, id_in_db):
    # puts current_chunk in the chunks list and creates a new one
    self.chunks.append(self.current_chunk)
    self.current_chunk = self.get_new_current_chunk()

  def get_new_current_chunk(self):
    return self.mgr.chunkmgr.get_empty_obj()

  def __contains__(self, message):
    # membership test is O(1) on average in sets, this should be cheap
    return any(message in chunk for chunk in iter(self.chunks))


class ChunkMgr(ObjMgr):
  def __init__(self, conn, db_name):
    self.dbmgr = ChunkDB(conn, db_name, self.get_db_index_key())

  @property
  def size(self):
    return 100

  def save_in_db(self, container, chunk):
    # chunk is the object, not the json representation
    id_ref = self.dbmgr.save_chunk(chunk.default())
    # link the DB id as the id of the current chunk within the container
    container.set_current_chunk(id_ref)

  def get_obj(self, json_chunk):
    return Chunk(json_chunk.get('size'),
                 json_chunk.get('_id'),
                 json_chunk.get('tweet_ids', []),
                 json_chunk.get('users', []),
                 json_chunk.get('terms', []),
                 json_chunk.get('user_mentions', []),
                 json_chunk.get('hashtags', []),
                 json_chunk.get('sorted_terms', []),
                 json_chunk.get('sorted_user_mentions', []),
                 json_chunk.get('sorted_hashtags', []))

  def get_empty_obj(self):
    # assume that the empty obj won't be in the DB, so no need for obj_id
    return self.get_obj({'size': self.size})

  def load_obj_from_id(self, chunk_id):
    chunk = self.dbmgr.load_json_from_id(chunk_id)
    if not chunk:
      raise Exception("No chunk found with id {0}".format(chunk_id))
    return self.load_obj_from_json(chunk)

  def load_obj_from_json(self, chunk_dict):
    return self.get_obj(chunk_dict)

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

  def get_db_index_key(self):
    return '_id'


class Chunk(object):
  def __init__(self, size, obj_id,
               tweet_ids, users, terms, user_mentions, hashtags,
               sorted_terms, sorted_user_mentions, sorted_hashtags):
    self.obj_id = obj_id
    self.size = size
    self.tweet_ids = set(tweet_ids)
    self.users = set(users)
    self.changed_since_retrieval = False
    self.terms = defaultdict(int, terms)
    self.user_mentions = defaultdict(int, user_mentions)
    self.hashtags = defaultdict(int, hashtags)
    self._deserialize_sorted_lists(sorted_terms,
                                   sorted_user_mentions,
                                   sorted_hashtags)

  def is_full(self):
    return len(self.tweet_ids) >= self.size

  def sorted_dicts(self):
    # if needed, creates a sorted list out of the dictionaries
    def need_to_recalculate():
      return self.changed_since_retrieval or \
             any(not d for d in (self.sorted_terms,
                                 self.sorted_user_mentions,
                                 self.sorted_hashtags))
    if need_to_recalculate():
      self.sorted_terms = sorted_list_from_dict(self.terms)
      self.sorted_user_mentions = sorted_list_from_dict(self.user_mentions)
      self.sorted_hashtags = sorted_list_from_dict(self.hashtags)
    return self.sorted_terms, self.sorted_user_mentions, self.sorted_hashtags

  def _serialize_sorted_lists(self):
    #the output of sorted_dicts has some sets that need to be transformed into something different
    self.sorted_terms = [(num, list(occ)) for num, occ in self.sorted_terms]
    self.sorted_user_mentions = [(num, list(occ)) for num, occ in self.sorted_user_mentions]
    self.sorted_hashtags = [(num, list(occ)) for num, occ in self.sorted_hashtags]

  def _deserialize_sorted_lists(self, terms, user_mentions, hashtags):
    # attrs_list is a list of tuples: (class attribute to set, value used to set it)
    #convert back lists into sets
    self.sorted_terms = [(num, set(occ)) for num, occ in terms]
    self.sorted_user_mentions = [(num, set(occ)) for num, occ in user_mentions]
    self.sorted_hashtags = [(num, set(occ)) for num, occ in hashtags]

  def default(self):
    # json reprentation with the desired format to store the chunk in the DB
    self.sorted_dicts()
    self._serialize_sorted_lists()
    keys = CHUNK_DATA
    values = (self.terms, self.sorted_terms,
              self.user_mentions, self.sorted_user_mentions,
              self.hashtags, self.sorted_hashtags, list(self.users), list(self.tweet_ids))
    d = dict(zip(keys, values))
    if self.obj_id:
        d['_id'] = self.obj_id
    return d

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
