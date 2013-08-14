from messages.message import Message


class StatusWithHeld(Message):
  def process(self):
    with open("status_withheld", "a") as f:
      f.write(self.message.status_withheld)
      f.write('\n')

