from messages.message import Message


class Warning(Message):
  def process(self):
    with open("warning", "a") as f:
      f.write(self.message.warning)
      f.write('\n')
