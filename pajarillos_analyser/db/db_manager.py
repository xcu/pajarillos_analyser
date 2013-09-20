from db.time_chunk import TimeChunkMgr
from utils import convert_date
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_mgr')



class DBManager(object):
  def __init__(self, conn, db_name, collection_name, flush=False, index=''):
    self.db = DBHandler(conn[db_name][collection_name])
    if flush:
      self.db.collection.drop()
    if index:
      self.db.collection.create_index(index, unique=True)

  def get_chunk(self, sdate):
    logger.info("db manager get_chunk: sdate is {0}".format(sdate))
    chunk_id = TimeChunkMgr().get_date_db_key(sdate)
    res = self.db.get({'start_date': chunk_id})
    if not res.count():
      return ''
    return res.next()

  def get_reduced_chunk_range(self, sdate, edate):
    def chunk_obj_generator(chunks):
      mgr = TimeChunkMgr()
      for chunk_dict in chunks:
        yield mgr.load_chunk(chunk_dict).reduce_subchunks()
    chunk_dicts = self.db.get_reduced_chunk_range(mgr.get_date_db_key(sdate),
                                                  mgr.get_date_db_key(edate))
    return TimeChunkMgr().reduce_chunks(chunk_obj_generator(chunk_dicts), postprocess=True)

  def upsert_chunk(self, chunk_dict):
    # probably chunk.default should return the datetime and not the posix seconds
    logger.info("updating db with key {0}. Chunk containing subchunks with size: {1}".format(key,
                          ','.join([str(len(sc['tweet_ids'])) for sc in chunk_dict['subchunks']])))
    self.update_doc({'start_date': chunk_dict['start_date']}, chunk_dict)

  def update_doc(self, doc_id, doc):
    return self.db.update_doc(doc_id, doc)

  def __iter__(self):
    return self.db.get_all()



class DBHandler(object):
  ''' a purely stupid class that only updates/retrieves/inserts without asking
      questions, it has no logic inside. '''
  def __init__(self, collection):
    self.collection = collection

  def update_doc(self, doc_id, doc):
    self.collection.update(doc_id, doc, upsert=True)

  def get(self, query):
    return self.collection.find(query)

  def get_all(self):
    return self.collection.find()

  def get_reduced_chunk_range(self, lower, upper):
    return self.collection.find({'start_date': {'$gte': lower, '$lte': upper}})

