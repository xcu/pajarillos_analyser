from messages.message import Message

from datetime import datetime
import calendar

class Tweet(Message):
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
    return [getattr(mention, prop) for mention in self.message.entities.user_mentions]

  def get_hashtags(self):
    return [ht.text for ht in self.message.entities.hashtags]

  def get_creation_time(self, process=True):
    'self.created_at will return something like Wed Aug 27 13:08:45 +0000 2008'
    def get_month_number(month):
      months = dict((v,k) for k,v in enumerate(calendar.month_abbr))
      # supported after python 2.7
      # months = {v: k for k,v in enumerate(calendar.month_abbr)}
      return months.get(month)
    if not process:
      return self.message.created_at
    splitted_date = self.message.created_at.split()
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
    return self.message.id_str

  def get_retweet_count(self):
    return self.message.retweet_count

  def get_favorite_count(self):
    return self.message.favorite_count

  def process(self):
    with open("tweets", "a") as f:
      f.write('{0}\t'.format(self.get_text().encode('utf-8')))
      f.write('{0}\t'.format(self.get_user_mentions().encode('utf-8')))
      f.write('{0}\t'.format(self.get_hashtags().encode('utf-8')))
      f.write('{0}\t'.format(self.get_creation_time(process=False).encode('utf-8')))
      f.write('\n')



