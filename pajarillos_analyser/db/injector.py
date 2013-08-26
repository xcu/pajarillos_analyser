from db.time_chunk import TimeChunk
from utils import convert_date
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_injector')


TIMECHUNK_SIZE = 1


class DBInjector(object):
  def __init__(self, connection, db_name, collection_name, flush=True, index=''):
    self.collection = connection[db_name][collection_name]
    if flush:
      self.collection.drop()
    if index:
      self.collection.create_index(index, unique=True)

  def get_time_chunk_fromkey(self, start):
    '''
    loads the time chunk from the db if it exists or creates a new one
    @param start datetime object to match a key in the db
    '''
    if not self.chunk_exists(start):
      return TimeChunk(TIMECHUNK_SIZE, start)
    chunk_dict = self._get_chunk(start).next()
    return self.load_chunk(chunk_dict)

  def insert_time_chunks(self, streamer):
    ''' puts the messages given by the streamer in the database '''
    current_time_chunk = None
    for message in streamer.messages():
      if not current_time_chunk:
        current_time_chunk = self.get_time_chunk_fromkey(message._process_by_time(TIMECHUNK_SIZE))
      if not current_time_chunk.tweet_fits(message):
        if current_time_chunk.changed_since_retrieval:
          logger.info("saving chunk in db because key {0} doesnt match tweet with date {1}".\
                       format(current_time_chunk.start_date, message.get_creation_time()))
          self.save_chunk(current_time_chunk)
        current_time_chunk = self.get_time_chunk_fromkey(message._process_by_time(TIMECHUNK_SIZE))
      current_time_chunk.update(message)
    self.save_chunk(current_time_chunk)

  def all_ids_from_db(self):
    all_ids = set()
    for chunk_dict in self.collection.find():
      all_ids.update(self.load_chunk(chunk_dict).tweet_ids)
    return all_ids

  def save_chunk(self, chunk):
    if chunk:
      key = convert_date(chunk.start_date)
      logger.info("updating db with key {0}. Chunk with size {1}".format(key,
                                                                         len(chunk.tweet_ids)))
      self.collection.update({'start_date': key}, chunk.default(), upsert=True)

  def load_chunk(self, chunk_dict):
    return TimeChunk(chunk_dict.pop('size'), chunk_dict.pop('start_date'), **chunk_dict)

  def chunk_exists(self, start_time):
    return self._get_chunk(start_time).count()

  def _get_chunk(self, start_time):
    time_chunk = self.collection.find({'start_date': convert_date(start_time)})
    return time_chunk

  def dump_db_info(self):
    with open("processed", 'w') as f:
      for chunk_dict in self.collection.find():
        f.write(self.load_chunk(chunk_dict).pretty())
        f.write('\n')

