from db.time_chunk import TimeChunk, TimeChunkMgr
from utils import convert_date
from collections import defaultdict
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_injector')


TIMECHUNK_SIZE = 1


class Injector(object):
  def __init__(self, dbmgr):
    self.dbmgr = dbmgr

  def to_db(self, message, last_returned_val):
    pass

  def last_to_db(self, last_val):
    pass


class TweetInjector(Injector):
  def to_db(self, message, last_returned_val):
    self.dbmgr.update_doc({'id': int(message.get_id())}, message.message)


class TimeChunkInjector(Injector):
  def to_db(self, message, current_time_chunk):
    ''' updates current_time_chunk with message and stores the chunk in the db if necessary '''
    if not current_time_chunk:
      current_time_chunk = self.get_chunk_from_date(message.get_associated_chunk(TIMECHUNK_SIZE))
    if not current_time_chunk.tweet_fits(message):
      if current_time_chunk.changed_since_retrieval:
        logger.info("saving chunk in db because key {0} doesnt match tweet {1} with date {2}".\
                     format(current_time_chunk.start_date, message.get_id(), message.get_creation_time()))
        self.save_chunk(current_time_chunk)
      current_time_chunk = self.get_chunk_from_date(message.get_associated_chunk(TIMECHUNK_SIZE))
    current_time_chunk.update(message)
    return current_time_chunk

  def last_to_db(self, current_time_chunk):
    self.save_chunk(current_time_chunk)

  def get_chunk_from_date(self, start):
    '''
    loads the time chunk from the db if it exists or creates a new one
    @param start datetime object to match a key in the db
    '''
    logger.info("trying to get chunk from date {0}".format(start))
    if not self.chunk_exists(start):
      return TimeChunk(TIMECHUNK_SIZE, start)
    chunk_dict = self._get_chunk(start)
    return TimeChunkMgr().load_chunk(chunk_dict)

  def chunk_exists(self, start_time):
    return self._get_chunk(start_time)

  def _get_chunk(self, start_time):
    return self.dbmgr.get_chunk(start_time)

  def save_chunk(self, chunk):
    for sc in chunk.subchunks:
    if chunk:
      self.dbmgr.upsert_chunk(chunk.default())

  def save_subchunk(self, subchunk):
