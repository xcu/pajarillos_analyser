
class Message(object):
  def __init__(self, value):
    self.message = value

  def is_time_based(self):
    return False

  def _process(self):
    pass

