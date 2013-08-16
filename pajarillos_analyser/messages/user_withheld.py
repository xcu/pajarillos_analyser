from messages.message import Message


class UserWithHeld(Message):
  def _process(self):
    with open("user_withheld", "a") as f:
      f.write(self.message.user_withheld)
      f.write('\n')

