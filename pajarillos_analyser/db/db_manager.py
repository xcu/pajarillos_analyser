from db.chunk import ChunkMgr
import itertools
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_mgr')

CONTAINER_COLLECTION = 'chunk_containers'
CHUNK_COLLECTION = 'chunks'


class DBManager(object):
  def __init__(self, conn, db_name, collection_name, flush=False, index=''):
    self.db = DBHandler(conn[db_name][collection_name])
    if flush:
      self.db.collection.drop()
    if index:
      self.db.collection.ensure_index(index, unique=True)

  def update_doc(self, doc_id, doc):
    return self.db.update_doc(doc_id, doc)

  def __iter__(self):
    return self.db.get_all()


class DBChunkManager(object):
  def __init__(self, conn, db_name):
    self.chunk_mgr = ChunkMgr(xxxxxxxxxxxxx)
    self.container_db = DBHandler(conn[db_name][CONTAINER_COLLECTION])
    self.chunk_db = DBHandler(conn[db_name][CHUNK_COLLECTION])
    index_key = self.chunk_mgr.get_db_index_key()
    self.container_db.collection.ensure_index(index_key, unique=True)
    index_key = self.chunk_mgr.get_chunk_db_index_key()
    self.chunk_db.collection.ensure_index(index_key, unique=True)

  def _container_key_dict(self, container_id):
    ''' {container_index_str: container_id}'''
    index_key = self.chunk_mgr.get_db_index_key()
    return {index_key: container_id}

  def _chunk_key_dict(self, chunk_id):
    index_key = self.chunk_mgr.get_chunk_db_index_key()
    return {index_key: chunk_id}

  def update_container(self, container_id, container):
    ''' it actually upserts '''
    return self.container_db.update_doc(self._container_key_dict(container_id), container)

  def update_chunk(self, chunk_id, chunk):
    ''' it actually upserts '''
    return self.chunk_db.update_doc(self._chunk_key_dict(chunk_id), chunk)

  def save_chunk(self, json_chunk):
    ''' inserts, but doesn't update '''
    return self.chunk_db.insert_doc(json_chunk)

  def load_container_json_from_id(self, sdate):
    # returns a dictionary with the container stored with the provided date
    logger.info("db manager load_container_json_from_id: sdate is {0}".format(sdate))
    container_id = self.chunk_mgr.date_to_id_in_db(sdate)
    res = self.container_db.get(self._container_key_dict(container_id))
    if not res.count():
      return ''
    return res.next()

  def load_chunk_json_from_id(self, chunk_id):
    logger.info("db manager get_chunk id is {0}".format(chunk_id))
    res = self.chunk_db.get(self._chunk_key_dict(chunk_id))
    if not res.count():
      return ''
    return res.next()

  def get_chunk_range(self, sdate, edate):
    ''' returns all chunk ids between sdate, edate '''
    containers = self.container_db.get_chunk_range(self.chunk_mgr.date_to_id_in_db(sdate),
                                         self.chunk_mgr.date_to_id_in_db(edate))
    chunk_ids = (container.get('chunks', []) for container in containers)
    return itertools.chain(*chunk_ids)

  def upsert_container(self, container_dict):
    # probably chunk.default should return the datetime and not the posix seconds
    msg = "updating db with key {0}. Chunk containing {1} chunks"
    key = container_dict['start_date']
    chunks_size = len(container_dict['chunks'])
    logger.info(msg.format(key, chunks_size))
    self.update_container(container_dict['start_date'], container_dict)


class DBHandler(object):
  ''' a purely stupid class that only updates/retrieves/inserts without asking
      questions, it has no logic inside. '''
  def __init__(self, collection):
    self.collection = collection

  def update_doc(self, doc_id, doc):
    self.collection.update(doc_id, doc, upsert=True)

  def insert_doc(self, doc):
    return self.collection.insert(doc)

  def get(self, query):
    return self.collection.find(query)

  def get_all(self):
    return self.collection.find()

  def get_chunk_range(self, lower, upper):
    return self.collection.find({'start_date': {'$gte': lower, '$lte': upper}})
# tweets.update({'id_str': "368308360074240000"}, { '$set': {'text': 'blah'}})
