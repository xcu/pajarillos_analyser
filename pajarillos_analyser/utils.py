import calendar

def convert_date(date):
  '''
  converts datetime to unix timestamp
  @param date datetime object
  '''
  if date.utcoffset():
    date = date - date.utcoffset()
  return int(calendar.timegm(date.timetuple()))

