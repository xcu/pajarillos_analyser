import calendar
from collections import defaultdict


CHUNK_DATA = ('terms', 'user_mentions', 'hashtags', 'users', 'tweet_ids')

def convert_date(date):
  '''
  converts datetime to unix timestamp
  @param date datetime object
  '''
  if date.utcoffset():
    date = date - date.utcoffset()
  return int(calendar.timegm(date.timetuple()))

def update_dict(append_to, append_from):
  for key in append_from.iterkeys():
    append_to[key] += append_from[key]

def update_set(new_set, old_set):
  for item in old_set:
    new_set.add(item)

def reduce_chunks(chunk_iterable):
    terms = defaultdict(int)
    user_mentions = defaultdict(int)
    hashtags = defaultdict(int)
    users = set()
    tweet_ids = 0
    for chunk_dict in iterable:
      update_dict(terms, chunk_dict['terms'])
      update_dict(hashtags, chunk_dict['hashtags'])
      update_dict(user_mentions, chunk_dict['user_mentions'])
      update_set(users, chunk_dict['users'])
      tweet_ids += chunk_dict['tweet_ids']
    terms = dict(i for i in terms.iteritems() if not self.filter_term(i[0]))
    terms = sorted(terms.items(), key=lambda x: x[1], reverse=True)[:20]
    user_mentions = sorted(user_mentions.items(), key=lambda x: x[1], reverse=True)[:5]
    hashtags = sorted(hashtags.items(), key=lambda x: x[1], reverse=True)[:5]
    keys = CHUNK_DATA
    values = (terms, user_mentions, hashtags, users, tweet_ids)
    return dict(zip(keys, values))

  def filter_term(self, term):
    if len(term) <= 2:
      return True
    filter_list = set(['ante', 'con', 'como', 'del', 'desde', 'entre', 'este', 'estas', 'estos', 'hacia',
                       'hasta', 'las', 'los', 'mas', 'nos', 'para', 'pero', 'por', 'que', 'segun', 'ser',
                       'sin', 'una', 'unas', 'uno', 'unos'])
    return term.lower() in filter_list

def chunks_are_equal(col1, col2):
  cols_are_the_same(col1, col2)
  cols_are_the_same(col2, col1)

def cols_are_the_same(c1, c2):
  for tc1 in c1.find():
    if not c2.find({'start_date': tc1['start_date']}).count():
      raise Exception("not found!! {0}".format(tc1['start_date']))
    tc2 = c2.find_one({'start_date': tc1['start_date']})
    assert tc1['terms'] == tc2['terms']
    assert tc1['user_mentions'] == tc2['user_mentions']
    assert tc1['hashtags'] == tc2['hashtags']
    assert set(tc1['users']) == set(tc2['users'])
    assert set(tc1['tweet_ids']) == set(tc2['tweet_ids'])
    print 'compared {0}, no changes'.format(tc1['start_date'])
  print 'they are all the same!'
