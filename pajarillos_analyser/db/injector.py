from db.chunk import ChunkContainer, ChunkMgr
from utils import convert_date
from collections import defaultdict
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_injector')


CONTAINER_SIZE = 1


class Injector(object):
  def __init__(self, dbmgr):
    self.dbmgr = dbmgr
    self.chunkmgr = ChunkMgr()

  def to_db(self, message, last_returned_val):
    pass

  def last_to_db(self, last_val):
    pass


class TweetInjector(Injector):
  def to_db(self, message, last_returned_val):
    self.dbmgr.update_doc({'id': int(message.get_id())}, message.message)


class ChunkInjector(Injector):
  def to_db(self, message, current_chunk_container):
    ''' updates current_chunk_container with message and stores the chunk in the db if necessary'''
    def pick_container_from_msg_date():
      message_date = message.get_associated_container(CONTAINER_SIZE)
      return self.get_chunk_container_from_date(message_date)

    if not current_chunk_container:
      current_chunk_container = pick_container_from_msg_date()
    if not current_chunk_container.tweet_fits(message):
      if current_chunk_container.changed_since_retrieval:
        msg = "saving chunk in db because key {0} doesnt match tweet {1} with date {2}"
        logger.info(msg.format(current_chunk_container.start_date,
                               message.get_id(),
                               message.get_creation_time()))
        self.save_container(current_chunk_container)
      current_chunk_container = pick_container_from_msg_date()
    if current_chunk_container.current_chunk_isfull():
      self.store_current_chunk_in_db(current_chunk_container)
    current_chunk_container.update(message)
    return current_chunk_container

  def store_current_chunk_in_db(self, container):
    # put it in db, get its id and with it update the container object
    id_ref = self.dbmgr.save_chunk(container.current_chunk[1])
    container.store_current_chunk(id_ref)

  def last_to_db(self, current_chunk_container):
    self.save_container(current_chunk_container)

  def get_chunk_container_from_date(self, start):
    '''
    loads the time chunk from the db if it exists or creates a new one
    @param start datetime object to match a key in the db
    '''
    logger.info("trying to get chunk from date {0}".format(start))
    if not self.chunk_exists(start):
      return self.chunkmgr.load_empty_chunk_container(CONTAINER_SIZE, start)
    return self.dbmgr.load_container_from_date(start)

  def chunk_exists(self, start_time):
    return self._get_chunk_container(start_time)

  def _get_chunk_container(self, start_time):
    # returns the container in the db with that start_time
    return self.dbmgr.get_chunk_container(start_time)

  def save_container(self, container):
    # chunks will be object ids already, no worries about them
    # current chunk needs to get its sorted lists recalculated
    # and update its entry in the db (fetching by its object id)
    if container:
      if container.current_chunk:
        chunk_id, chunk_obj = container.current_chunk
        if chunk_id:
          self.dbmgr.update_chunk(chunk_id, chunk_obj.default())
        else:
          container.current_chunk = (self.dbmgr.save_chunk(chunk_obj), container.current_chunk)
      self.dbmgr.upsert_container(container.default())

