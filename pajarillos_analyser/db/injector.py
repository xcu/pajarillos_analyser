from db.chunk import ContainerMgr
from datetime import datetime, timedelta
from utils import container_size_is_valid
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
    self.dbmgr.db.update_doc({'id': int(message.get_id())}, message.message)

  def last_to_db(self):
      pass


class ChunkContainerInjector(Injector):
  def __init__(self, conn, db_name):
    self.container_mgr = ContainerMgr(conn, db_name)
    # last modified object
    self.chunk_containers = {}

  def container_sizes(self):
    return [1, 5, 30, 240]

  def non_existing_containers(self):
    for key in iter(self.container_sizes()):
        if not self.chunk_containers.get(key):
            yield key

  def to_db(self, message):
    ''' updates current_chunk_container with message and stores the chunk in the db if necessary'''
    # doesnt make sense to have a non-time based msg in a time-based container
    if not message.is_time_based():
      return
    for container_size in self.non_existing_containers():
      newcontainer = self.pick_container_from_msg_date(message, container_size)
      self.chunk_containers[container_size] = newcontainer
    for size, container in self.chunk_containers.iteritems():
      if not container.tweet_fits(message):
          # store the current container and get a new one
          self._refresh_container(container, message)
      if container.current_chunk_isfull():
        self.container_mgr.refresh_current_chunk(container)
      self.chunk_containers[size].update(message)

  def pick_container_from_msg_date(self, message, container_size):
    # given a message object, returns the associated container for that time
    container_key = self._get_associated_container_key(message, container_size)
    return self.container_mgr.load_obj_from_id(container_key, container_size)

  def _refresh_container(self, container, message):
    # TODO: shouldn't the container be in charge of deciding whether to be
    # stored in the db or not?
    if container.changed_since_retrieval:
      msg = "saving chunk in db because key {0} doesnt match tweet {1} with date {2}"
      logger.info(msg.format(container.start_date,
                             message.get_id(),
                             message.get_creation_time()))
      self.container_mgr.save_in_db(container)
    self.chunk_containers[container.size] = \
                    self.pick_container_from_msg_date(message, container.size)

  def _get_associated_container_key(self, message, container_size):
    ''' returns the chunk container the message belongs to
    @params chunk_container_size, integer with the number of minutes delimiting a
     chunk. 60 must be divisible by it'''
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    if not container_size_is_valid(container_size):
      raise Exception("chunk_container_size of size {0} is not valid".format(container_size))
    creation_time = message.get_creation_time()
    delta = timedelta(minutes=creation_time.minute % container_size)
    return reset_seconds(creation_time - delta)

  def last_to_db(self):
    for container in self.chunk_containers.itervalues():
      self.container_mgr.save_in_db(container)

