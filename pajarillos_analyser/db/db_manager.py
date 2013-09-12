
class DBManager(object):
  def __init__(self, conn, db_name, collection_name, flush=False, index=''):
    self.collection = conn[db_name][collection_name]
    if flush:
      self.collection.drop()
    if index:
      self.collection.create_index(index, unique=True)

  def update_doc(self, doc_id, doc):
    self.collection.update(doc_id, doc, upsert=True)

  def get(self, query):
    return self.collection.find(query)

  def get_all(self):
    return self.collection.find()

  def get_chunk_range(self, lower, upper):
    return self.collection.find({'start_date': {'$gte': lower, '$lte': upper}})

