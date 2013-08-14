from messages.message import Message

class Delete(Message):
  def process(self):
    with open("delete", "a") as f:
      f.write(str(self.message.delete).encode('utf-8'))
      f.write('\n')

