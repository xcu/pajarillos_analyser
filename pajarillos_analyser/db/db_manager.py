import pymongo
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
    self.db.collection.ensure_index(self._build_indexes_direction(index_key),
                                    unique=True)
#    if flush:
#      self.db.collection.drop()

  def _build_indexes_direction(self, indexes):
    # let's make it simple until we care about directions
    return [(index, pymongo.ASCENDING) for index in indexes]

  def _set_id_field_in_db(self, obj):
    # TODO: is there a better way?
    vals = [obj[key] for key in self.index_key]
    for key, val in zip(self.index_key, self.fieldval_to_id_in_db(vals)):
      obj[key] = val

  def _set_id_field_in_obj(self, obj):
    vals = [obj[key] for key in self.index_key]
    for key, val in zip(self.index_key, self.id_in_db_to_fieldval(vals)):
      obj[key] = val

  def update_obj(self, obj):
    self._set_id_field_in_db(obj)

  def load_json_from_id(self, id_values):
    res = self.db.get(self._get_index_and_id(id_values))
    if not res.count():
      return ''
    json_obj = res.next()
    self._set_id_field_in_obj(json_obj)
    return json_obj

  def _get_index_values(self, obj):
    return [obj[index] for index in self.index_key]

  def _get_index_and_id(self, id_values):
    return dict(zip(self.index_key, id_values))

  def fieldval_to_id_in_db(self, objfield):
    return objfield

  def id_in_db_to_fieldval(self, key):
    return key

  def __iter__(self):
    return self.db.get_all()


class ChunkDB(ObjDB):
  def __init__(self, conn, db_name, index_key):
    super(ChunkDB, self).__init__(conn,
                                  db_name,
                                  index_key,
                                  CHUNK_COLLECTION)

  def update_obj(self, chunk):
    ''' it actually upserts '''
    super(ChunkDB, self).update_obj(chunk)
    index_vals = self._get_index_values(chunk)
    return self.db.update_doc(self._get_index_and_id(index_vals), chunk)

  def save_chunk(self, json_chunk):
    ''' inserts, but doesn't update '''
    if json_chunk.get('_id'):
      raise Exception("Chunk to be inserted, but has id {0}".format(json_chunk['_id']))
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

  def update_obj(self, container):
    ''' it actually upserts '''
    super(ContainerDB, self).update_obj(container)
    logger.info("updating db with date {0}. Chunk containing {1} chunks".\
           format(self._get_index_values(container), len(container['chunks'])))
    index_vals = self._get_index_values(container)
    return self.db.update_doc(self._get_index_and_id(index_vals), container)

  def load_json_from_id(self, sdate, size):
    # returns a dictionary with the container stored with the provided date
    logger.info("db manager load_json_from_id: sdate {0}, size {1}".\
                                                           format(sdate, size))
    container_id = self.fieldval_to_id_in_db((sdate, size))
    return super(ContainerDB, self).load_json_from_id(container_id)

  def fieldval_to_id_in_db(self, fields):
    ''' expects a datetime object, returns db id '''
    date, size = fields
    return convert_date(date), size

  def id_in_db_to_fieldval(self, fields):
    ''' expects db id, returns datetime obj '''
    date, size = fields
    return datetime.utcfromtimestamp(date), size
