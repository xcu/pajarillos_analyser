from db.chunk import ContainerMgr
from datetime import datetime, timedelta
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_injector')


class Injector(object):
  def to_db(self, message, last_returned_val):
    pass

  def last_to_db(self, last_val):
    pass


class TweetInjector(Injector):
  # TODO: class shouldn't have a reference to the DB layer directly
  def __init__(self, dbmgr):
    self.dbmgr = dbmgr

  def to_db(self, message):
    self.dbmgr.update_doc({'id': int(message.get_id())}, message.message)


class ChunkContainerInjector(Injector):
  def __init__(self, conn, db_name):
    self.container_mgr = ContainerMgr(conn, db_name)
    # last modified object
    self.current_chunk_container = None

  def to_db(self, message):
    ''' updates current_chunk_container with message and stores the chunk in the db if necessary'''
    # doesnt make sense to have a non-time based msg in a time-based container
    if not message.is_time_based():
      return
    if not self.current_chunk_container:
      self.current_chunk_container = self.pick_container_from_msg_date(message)
    if not self.current_chunk_container.tweet_fits(message):
      # store the current container and get a new one
      self._refresh_current_container(message)
    if self.current_chunk_container.current_chunk_isfull():
      self.container_mgr.save_in_db(self.current_chunk_container)
    self.current_chunk_container.update(message)

  def pick_container_from_msg_date(self, message):
    # given a message object, returns the associated container for that time
    container_key = self._get_associated_container_key(message)
    return self.container_mgr.load_obj_from_id(container_key)

  def _refresh_current_container(self, message):
    if self.current_chunk_container.changed_since_retrieval:
      msg = "saving chunk in db because key {0} doesnt match tweet {1} with date {2}"
      logger.info(msg.format(self.current_chunk_container.start_date,
                             message.get_id(),
                             message.get_creation_time()))
      self.container_mgr.save_in_db(self.current_chunk_container)
    self.current_chunk_container = self.pick_container_from_msg_date(message)

  def _get_associated_container_key(self, message):
    ''' returns the chunk container the message belongs to
    @params chunk_container_size, integer with the number of minutes delimiting a
     chunk. 60 must be divisible by it'''
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    if self.container_mgr.size > 60 or 60 % self.container_mgr.size:
      raise Exception("chunk_container_size of size {0} is not valid".format(self.container_mgr.size))
    creation_time = message.get_creation_time()
    delta = timedelta(minutes=creation_time.minute % self.container_mgr.size)
    return reset_seconds(creation_time - delta)

  def last_to_db(self):
    if self.current_chunk_container:
      self.container_mgr.save_in_db(self.current_chunk_container)

