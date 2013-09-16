from messages.message import Message

from datetime import datetime, timedelta
from collections import defaultdict
import calendar
import re


class Tweet(Message):
  def get_text(self):
    return self.message.get('text', '')

  def get_user_mentions(self, prop='screen_name'):
    ''' 
    https://dev.twitter.com/docs/tweet-entities
    @param property str with the property to pick within the user mention:
    id, id_str, screen_name, name, indices
    '''
    if not prop:
      return self.message.get('entities', {}).get('user_mentions')
    return [mention.get(prop, '') for mention in self.message.get('entities', {}).get('user_mentions', '')]

  def get_hashtags(self):
    return [ht.get('text', '') for ht in self.message.get('entities', {}).get('hashtags', '')]

  def get_creation_time(self, process=True):
    'self.created_at will return something like Wed Aug 27 13:08:45 +0000 2008'
    def get_month_number(month):
      months = dict((v,k) for k,v in enumerate(calendar.month_abbr))
      # supported after python 2.7
      # months = {v: k for k,v in enumerate(calendar.month_abbr)}
      return months.get(month)
    if not process:
      return self.message.get('created_at')
    splitted_date = self.message.get('created_at', '').split()
    if not splitted_date:
      return None
    splitted_time = dict(zip(['hour', 'minute', 'second'],
                             [int(n) for n in splitted_date[3].split(':')]
                            ))
    splitted_date = (int(splitted_date[5]),
                     get_month_number(splitted_date[1]),
                     int(splitted_date[2]))
    return datetime(*splitted_date, **splitted_time)

  def get_id(self):
    return self.message.get('id_str', '')

  def get_retweet_count(self):
    return self.message.get('retweet_count', '')

  def get_favorite_count(self):
    return self.message.get('favorite_count', '')

  def get_user(self, field='screen_name'):
    if not field:
      return self.message.get('user', {})
    return self.message.get('user', {}).get(field, '')

  def _process(self):
    with open("tweets", "a") as f:
      f.write('{0}\t'.format(self.get_text().encode('utf-8')))
      f.write('{0}\t'.format(self.get_user_mentions().encode('utf-8')))
      f.write('{0}\t'.format(self.get_hashtags().encode('utf-8')))
      f.write('{0}\t'.format(self.get_creation_time(process=False).encode('utf-8')))
      f.write('\n')

  def get_associated_chunk(self, time_chunk_size):
    ''' returns the time chunk the tweet belongs to
    @params time_chunk_size, integer with the number of minutes delimiting a
     chunk. 60 must be divisible by it'''
    def reset_seconds(date):
      return datetime(date.year, date.month, date.day, date.hour, date.minute)
    if time_chunk_size > 60 or 60 % time_chunk_size:
      raise Exception("time_chunk_size of size {0} is not valid".format(time_chunk_size))
    creation_time = self.get_creation_time()
    delta = timedelta(minutes=creation_time.minute % time_chunk_size)
    return reset_seconds(creation_time - delta)

  def get_terms(self):
    terms = defaultdict(int)
    # we need more than 1 separator
    # http://stackoverflow.com/questions/1059559/python-strings-split-with-multiple-separators
    for word in re.findall(r"[\w']+", self.get_text()):
      terms[word] += 1
    return terms

