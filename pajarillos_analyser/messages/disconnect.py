from messages.message import Message


class Disconnect(Message):
  def _process(self):
    with open("disconnect", "a") as f:
      f.write(self.message.disconnect)
      f.write('\n')

