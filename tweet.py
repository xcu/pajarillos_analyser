
class Tweet(object):

  def __init__(self, deserialized_tweet):
    self._tweet = DotDict(deserialized_tweet)

  def get_text(self):
    return self._tweet_text

  def get_user_mentions(self, property='screen_name'):
    ''' 
    https://dev.twitter.com/docs/tweet-entities
    @param property str with the property to pick within the user mention:
    id, id_str, screen_name, name, indices
    '''
    return self._tweet.entities.user_mentions.property

  def get_hashtags(self):
    return [ht.text for ht in self._tweet.entities.hashtags]

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
        if isinstance(value, dict) and not isinstance(value, DotDict):
            value = DotDict(value)
        dict.__setitem__(self, key, value)

    def __getitem__(self, key):
        found = self.get(key, DotDict.marker)
        if found is DotDict.marker:
            found = DotDict()
            dict.__setitem__(self, key, found)
        return found

    __setattr__ = __setitem__
    __getattr__ = __getitem__


