from streamers.streamer import Streamer


class FileStreamer(Streamer):
  def __init__(self, file_name):
    super(FileStreamer, self).__init__()
    self.file_name = file_name

  def __iter__(self):
    with open(self.file_name) as f:
      for line in f:
        yield self.create_message(line)

