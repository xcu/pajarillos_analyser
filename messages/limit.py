from messages.message import Message


class Limit(Message):
  def process(self):
    with open("limit", "a") as f:
      f.write(self.message.limit)
      f.write('\n')

