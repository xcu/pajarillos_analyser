from utils import convert_date


class DBManager(object):
  def __init__(self, conn, db_name, collection_name, flush=False, index=''):
    self.db = DBHandler(conn[db_name][collection_name])
    if flush:
      self.db.collection.drop()
    if index:
      self.db.collection.create_index(index, unique=True)

  def get_chunk(self, sdate):
    chunk_id = int(convert_date(sdate))
    res = self.db.get({'start_date': chunk_id})
    if not res.count():
      return ''
    return res.next()

  def get_chunk_range(self,injector, sdate, edate):
    return injector.reduce_range(convert_date(sdate), convert_date(edate))

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
    return self.collection.find(query))

  def get_all(self):
    return self.collection.find()

  def get_chunk_range(self, lower, upper):
    return self.collection.find({'start_date': {'$gte': lower, '$lte': upper}})

