from db.chunk_container import ChunkContainer, ChunkMgr
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


class ChunkInjector(Injector):
  def to_db(self, message, current_chunk_container):
    ''' updates current_chunk_container with message and stores the chunk in the db if necessary '''
    if not current_chunk_container:
      current_chunk_container = self.get_chunk_from_date(message.get_associated_container(TIMECHUNK_SIZE))
    if not current_chunk_container.tweet_fits(message):
      if current_chunk_container.changed_since_retrieval:
        logger.info("saving chunk in db because key {0} doesnt match tweet {1} with date {2}".\
                     format(current_chunk_container.start_date, message.get_id(), message.get_creation_time()))
        self.save_chunk(current_chunk_container)
      current_chunk_container = self.get_chunk_from_date(message.get_associated_container(TIMECHUNK_SIZE))
    if current_chunk_container.current_chunk_isfull():
      id_ref = self.dbmgr.save_chunk(current_chunk_container.current_chunk)
      current_chunk_container.update_current_chunk(id_ref)
    current_chunk_container.update(message)
    return current_chunk_container

  def last_to_db(self, current_chunk_container):
    self.save_chunk(current_chunk_container)

  def get_chunk_from_date(self, start):
    '''
    loads the time chunk from the db if it exists or creates a new one
    @param start datetime object to match a key in the db
    '''
    logger.info("trying to get chunk from date {0}".format(start))
    if not self.chunk_exists(start):
      return ChunkContainer(TIMECHUNK_SIZE, start)
    chunk_dict = self._get_chunk(start)
    return ChunkMgr().load_chunk(chunk_dict)

  def chunk_exists(self, start_time):
    return self._get_chunk(start_time)

  def _get_chunk(self, start_time):
    return self.dbmgr.get_chunk(start_time)

  def save_chunk(self, chunk):
    for sc in chunk.complete_chunks:
    if chunk:
      self.dbmgr.upsert_chunk(chunk.default())

  def save_chunk(self, chunk):
