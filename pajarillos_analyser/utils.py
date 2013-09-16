import calendar
from collections import defaultdict
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('utils')



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

#def all_ids_from_db():
#  all_ids = set()
#  for chunk_dict in self.dbmgr:
#    all_ids.update(TimeChunkMgr().load_chunk(chunk_dict).tweet_ids)
#  return all_ids


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
