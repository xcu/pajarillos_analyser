
class Message(object):
  def __init__(self, value):
    self.message = DotDict(value)

  def process(self):
    pass


class DotDict(dict):
    ''' thanks, stackoverflow: http://stackoverflow.com/questions/3031219/
    python-recursively-access-dict-via-attributes-as-well-as-index-access'''
    marker = object()
    def __init__(self, value=None):
        if value is None:
            pass
        elif isinstance(value, dict):
            for key in value:
                self.__setitem__(key, value[key])
        else:
            raise TypeError, 'expected dict'

    def __setitem__(self, key, value):
        def valid_instance(val):
          return isinstance(val, dict) and not isinstance(val, DotDict)
        if valid_instance(value):
            value = DotDict(value)
        elif isinstance(value, list) and all(valid_instance(item) for item in value):
            value = [DotDict(item) for item in value]
        dict.__setitem__(self, key, value)

    def __getitem__(self, key):
        found = self.get(key, DotDict.marker)
        if found is DotDict.marker:
            found = DotDict()
            dict.__setitem__(self, key, found)
        return found

    __setattr__ = __setitem__
    __getattr__ = __getitem__

