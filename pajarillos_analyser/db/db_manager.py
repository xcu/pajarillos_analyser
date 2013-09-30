from db.chunk import ChunkMgr
import itertools
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('db_mgr')



class DBManager(object):
  def __init__(self, conn, db_name, collection_name, sc='chunks', flush=False, index=''):
    self.db = DBHandler(conn[db_name][collection_name])
    self.chunk_mgr = ChunkMgr()
    # If I see this piece of code after one week I'll kill you
    # YOU KNOW I WILL BECAUSE I AM YOU
    self.sc = DBHandler(conn[db_name][sc])
    if flush:
      self.db.collection.drop()
    if index:
      self.db.collection.create_index(index, unique=True)

  def load_container_from_date(self, sdate):
    container = self.get_chunk_container(sdate)
    if not container:
      raise Exception("No container found with date {0}".format(sdate))
    return self.load_chunk_container(container)

  def get_chunk_container(self, sdate):
    # returns a dictionary with the container stored with the provided date
    logger.info("db manager get_chunk_container: sdate is {0}".format(sdate))
    container_id = self.chunk_mgr.get_date_db_key(sdate)
    res = self.db.get({'start_date': container_id})
    if not res.count():
      return ''
    return res.next()

  def load_chunk_container(self, container_dict):
    # returns a ChunkContainer object out from the provided dictionary
    container = self.chunk_mgr.load_chunk_container(container_dict)
    current_chunk = container_dict.get('current_chunk', {})
    container.current_chunk = {current_chunk: self.load_chunk(current_chunk)}
    chunks = container_dict.get('chunks', [])
    container.chunks = dict((chunk_id, self.load_chunk(chunk_id)) for chunk_id in chunks)
    return container

  def load_chunk_from_id(self, chunk_id):
    chunk = self.get_chunk(chunk_id)
    if not chunk:
      raise Exception("No chunk found with id {0}".format(chunk_id))
    return self.load_chunk(chunk)

  def get_chunk(self, chunk_id):
    # Refactor this and get chunk container. Create a method for each class
    # that returns the db id out of the candidate and accepts another param
    # with the name of the id in the db
    logger.info("db manager get_chunk id is {0}".format(chunk_id))
    res = self.db.get({'_id': chunk_id})
    if not res.count():
      return ''
    return res.next()

  def load_chunk(self, chunk_dict):
    parent_container = chunk_dict.get('parent_container')
    if not parent_container:
        raise Exception("no parent container found for chunk {0}".format(chunk_dict))
    return self.chunk_mgr.load_chunk(parent_container, chunk_dict)

  def get_chunk_range(self, sdate, edate):
    # pick containers between sdate, edate
    # for each container retrieve all its chunks
    # finally do sorting algorithm on those
    containers = self.db.get_chunk_range(self.chunk_mgr.get_date_db_key(sdate),
                                         self.chunk_mgr.get_date_db_key(edate))
    chunk_ids = (container.subchunks for container in containers)
    return itertools.chain(*chunk_ids)

  def upsert_container(self, container_dict):
    # probably chunk.default should return the datetime and not the posix seconds
    msg = "updating db with key {0}. Chunk containing chunks with size: {1}"
    key = self.chunk_mgr.get_date_db_key(container_dict['start_date'])
    chunks_size = ','.join([str(len(sc['tweet_ids'])) for sc in container_dict['chunks']])
    logger.info(msg.format(key, chunks_size))
    self.update_doc({'start_date': container_dict['start_date']}, container_dict)

  def save_chunk(self, chunk):
    return self.sc.insert_doc(chunk.default())

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

  def insert_doc(self, doc):
    return self.collection.insert(doc)

  def get(self, query):
    return self.collection.find(query)

  def get_all(self):
    return self.collection.find()

  def get_chunk_range(self, lower, upper):
    return self.collection.find({'start_date': {'$gte': lower, '$lte': upper}})

