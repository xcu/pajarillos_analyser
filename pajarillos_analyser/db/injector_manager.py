

class InjectorManager(object):
  def __init__(self, registered_injectors=[]):
    self.registered_injectors = registered_injectors

  def register_injector(self, injector):
    self.registered_injectors.append(injector)

  def to_db(self, streamer):
    for message in streamer:
      # each injector can inject the same message wherever it needs to
      for injector in iter(self.registered_injectors):
        injector.to_db(message)
    # do whatever is left
    for injector in iter(self.registered_injectors):
      injector.last_to_db()

