

class InjectorManager(object):
  def __init__(self, registered_injectors=[]):
    self.registered_injectors = dict((i, None) for i in registered_injectors)

  def register_injector(self, injector):
    self.registered_injectors.append(injector)

  def to_db(self, streamer):
    for message in streamer:
      for injector in self.registered_injectors.iterkeys():
        self.registered_injectors[injector] = injector.to_db(message,
                                                             self.registered_injectors[injector])
    for injector, last_val in self.registered_injectors.iteritems():
      injector.last_to_db(last_val)

