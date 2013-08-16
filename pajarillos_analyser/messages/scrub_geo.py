from messages.message import Message


class ScrubGeo(Message):
  def _process(self):
    with open("scrub_geo", "a") as f:
      f.write(self.message.scrub_geo)
      f.write('\n')

