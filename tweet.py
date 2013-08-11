from datetime import datetime
import calendar

class Tweet(object):

  def __init__(self, deserialized_tweet):
    self.message = DotDict(deserialized_tweet)

  def get_text(self):
    return self.message.text

  def get_user_mentions(self, prop='screen_name'):
    ''' 
    https://dev.twitter.com/docs/tweet-entities
    @param property str with the property to pick within the user mention:
    id, id_str, screen_name, name, indices
    '''
    if not prop:
      return self.message.entities.user_mentions
    return ' '.join([getattr(mention, prop) for mention in self.message.entities.user_mentions])

  def get_hashtags(self):
    return ' '.join([ht.text for ht in self.message.entities.hashtags])

  def get_creation_time(self):
    'self.created_at will return something like Wed Aug 27 13:08:45 +0000 2008'
    def get_month_number(month):
      months = {v: k for k,v in enumerate(calendar.month_abbr)}
      return months.get(month)
    splitted_date = self.created_at.split()
    splitted_time = dict(zip(['hour', 'minute', 'second'],
                             [int(n) for n in splitted_date[3].split(':')]
                            ))
    splitted_date = (int(splitted_date[5]),
                     get_month_number(splitted_date[1]),
                     int(splitted_date[2]))
    return datetime(*splitted_date, **splitted_time)

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


