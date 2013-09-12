from db.time_chunk import TimeChunk
from utils import convert_date, update_dict, update_set
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

  def from_db(self):
    pass


class TweetInjector(Injector):
  def to_db(self, message, last_returned_val):
    self.dbmgr.update_doc({'id': int(message.get_id())}, message.message)


class TimeChunkInjector(Injector):
  def to_db(self, message, current_time_chunk):
    ''' puts the message in the database '''
    if not current_time_chunk:
      current_time_chunk = self.get_time_chunk_fromkey(message._process_by_time(TIMECHUNK_SIZE))
    if not current_time_chunk.tweet_fits(message):
      if current_time_chunk.changed_since_retrieval:
        logger.info("saving chunk in db because key {0} doesnt match tweet {1} with date {2}".\
                     format(current_time_chunk.start_date, message.get_id(), message.get_creation_time()))
        self.save_chunk(current_time_chunk)
      current_time_chunk = self.get_time_chunk_fromkey(message._process_by_time(TIMECHUNK_SIZE))
    current_time_chunk.update(message)
    return current_time_chunk

  def last_to_db(self, current_time_chunk):
    self.save_chunk(current_time_chunk)

  def from_db(self, chunk_id):
    res = self.dbmgr.get({'start_date': int(chunk_id)})
    if not res.count():
      return 'no chunk found'
    return self.load_chunk(res.next()).reduce_subchunks()
#    with open("processed", 'w') as f:
#      for chunk_dict in self.collection.find():
#        f.write(self.load_chunk(chunk_dict).pretty())
#        f.write('\n')

  def reduce_range(self, lower, upper):
    terms = defaultdict(int)
    user_mentions = defaultdict(int)
    hashtags = defaultdict(int)
    users = set()
    tweet_ids = 0
    logger.info("reducing in range {0} {1}".format(lower, upper))
    for chunk_dict in self.dbmgr.get_chunk_range(lower, upper):
      reduced_chunk = self.load_chunk(chunk_dict).reduce_subchunks()
      update_dict(terms, reduced_chunk[0])
      update_dict(hashtags, reduced_chunk[1])
      update_dict(user_mentions, reduced_chunk[2])
      update_set(users, reduced_chunk[3])
      tweet_ids += reduced_chunk[4]

    logger.info("hashtags is {0}".format(hashtags))
    terms = dict(i for i in terms.iteritems() if len(i[0]) > 2)
    terms = sorted(terms.items(), key=lambda x: x[1], reverse=True)[:20]
    user_mentions = sorted(user_mentions.items(), key=lambda x: x[1], reverse=True)[:5]
    hashtags = sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:5]
    return (terms, user_mentions, hashtags, users, tweet_ids)

  def get_time_chunk_fromkey(self, start):
    '''
    loads the time chunk from the db if it exists or creates a new one
    @param start datetime object to match a key in the db
    '''
    if not self.chunk_exists(start):
      return TimeChunk(TIMECHUNK_SIZE, start)
    chunk_dict = self._get_chunk(start).next()
    return self.load_chunk(chunk_dict)

  def chunk_exists(self, start_time):
    return self._get_chunk(start_time).count()

  def _get_chunk(self, start_time):
    time_chunk = self.dbmgr.get({'start_date': convert_date(start_time)})
    return time_chunk

  def all_ids_from_db(self):
    all_ids = set()
    for chunk_dict in self.dbmgr.get_all():
      all_ids.update(self.load_chunk(chunk_dict).tweet_ids)
    return all_ids

  def save_chunk(self, chunk):
    if chunk:
      key = convert_date(chunk.start_date)
      logger.info("updating db with key {0}. Chunk with {1} subchunks of size ".format(key,
                                                                  ','.join([str(len(sc.tweet_ids)) for sc in chunk.subchunks])))
      self.dbmgr.update_doc({'start_date': key}, chunk.default())

  def load_chunk(self, chunk_dict):
    return TimeChunk(chunk_dict.pop('size'), chunk_dict.pop('start_date'), **chunk_dict)

