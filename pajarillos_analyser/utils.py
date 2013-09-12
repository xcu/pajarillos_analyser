import calendar


def convert_date(date):
  '''
  converts datetime to unix timestamp
  @param date datetime object
  '''
  if date.utcoffset():
    date = date - date.utcoffset()
  return int(calendar.timegm(date.timetuple()))


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