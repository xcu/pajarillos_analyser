from streamers.streamer import Streamer


class DBStreamer(Streamer):
  def __init__(self, dbmgr):
    super(DBStreamer, self).__init__()
    self.dbmgr = dbmgr

  def __iter__(self):
    for line in self.dbmgr:
      yield self.create_message(line, serialized=False)

