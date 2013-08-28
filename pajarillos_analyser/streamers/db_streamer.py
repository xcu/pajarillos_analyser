from streamers.streamer import Streamer

class DBStreamer(Streamer):
  def __init__(self, connection, db_name, collection_name):
    super(DBStreamer, self).__init__()
    self.collection = connection[db_name][collection_name]

  def __iter__(self):
    for line in self.collection.find():
      yield self.create_message(line, serialized=False)

