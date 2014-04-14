import itertools
from utils import convert_date
from datetime import datetime
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_mgr')

CONTAINER_COLLECTION = 'chunk_containers'
CHUNK_COLLECTION = 'chunks'


class DBHandler(object):
  ''' a purely basic class that only updates/retrieves/inserts without asking
      questions, with no logic inside. '''
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


class ObjDB(object):
  def __init__(self, conn, db_name, index_key, collection_name):
    self.db = DBHandler(conn[db_name][collection_name])
    self.index_key = index_key
    self.db.collection.ensure_index(index_key, unique=True)
#    if flush:
#      self.db.collection.drop()

  def update_obj(self, obj_id, obj):
    raise NotImplementedError

  def load_json_from_id(self, db_id):
    res = self.db.get(self._get_index_and_id(db_id))
    if not res.count():
      return ''
    return res.next()

  def _get_index_and_id(self, obj_id):
    return {self.index_key: obj_id}

  def fieldval_to_id_in_db(self, objfield):
    return objfield

  def id_in_db_to_fieldval(self, key):
    return key

  def __iter__(self):
    return self.db.get_all()


class ChunkDB(ObjDB):
  def __init__(self, conn, db_name, index_key):
    super(ContainerDB, self).__init__(conn,
                                             db_name,
                                             index_key,
                                             CHUNK_COLLECTION)

  def update_obj(self, chunk_id, chunk):
    ''' it actually upserts '''
    return self.db.update_doc(self._get_index_and_id(chunk_id), chunk)

  def save_chunk(self, json_chunk):
    ''' inserts, but doesn't update '''
    return self.db.insert_doc(json_chunk)


class ContainerDB(ObjDB):
  def __init__(self, conn, db_name, index_key):
    super(ContainerDB, self).__init__(conn,
                                             db_name,
                                             index_key,
                                             CONTAINER_COLLECTION)

  def get_chunk_range(self, sdate, edate):
    ''' returns all chunk ids between sdate, edate '''
    containers = self.db.get_chunk_range(self.fieldval_to_id_in_db(sdate),
                                         self.fieldval_to_id_in_db(edate))
    chunk_ids = (container.get('chunks', []) for container in containers)
    return itertools.chain(*chunk_ids)

  def update_obj(self, container_id, container):
    ''' it actually upserts '''
    key = self.fieldval_to_id_in_db(container['start_date'])
    logger.info("updating db with key {0}. Chunk containing {1} chunks".\
                format(key, len(container['chunks'])))
    return self.db.update_doc(self._get_index_and_id(key), container)

  def load_json_from_id(self, sdate):
    # returns a dictionary with the container stored with the provided date
    logger.info("db manager load_json_from_id: sdate is {0}".format(sdate))
    container_id = self.fieldval_to_id_in_db(sdate)
    return super(ContainerDB, self).load_json_from_id(container_id)

  def fieldval_to_id_in_db(self, date):
    ''' expects a datetime object, returns db id '''
    return convert_date(date)

  def id_in_db_to_fieldval(self, key):
    ''' expects db id, returns datetime obj '''
    return datetime.utcfromtimestamp(key)
