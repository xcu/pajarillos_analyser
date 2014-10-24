import calendar
import heapq
import bisect
from collections import defaultdict
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('utils')



CHUNK_DATA = ('terms', 'sorted_terms',
              'user_mentions', 'sorted_user_mentions', 'hashtags',
              'sorted_hashtags', 'users', 'tweet_ids')

def container_size_is_valid(size):
  if size < 60:
    return not 60 % size
  else:
    return not size % 60

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

def sorted_list_from_dict(d):
  '''
  out of a dictionary (term, occurences) creates a sorted list like
  (occurences, set(terms with that number of occurrences)
  '''
  reverted_dict = defaultdict(set)
  l = []
  for key, val in d.iteritems():
    # heapq does not handle heaps of maximums, so like it or not this is the approach
    reverted_dict[-val].add(key)
  return sorted(reverted_dict.items())

def create_maxs_heap(sorted_lists):
  '''
  will return a heap containing the maximum element for each list, and also the sum
  of these maximums
  '''
  heap = []
  for l in sorted_lists:
    list_iterator = iter(l)
    try:
      # next has something like (occurrences, set(terms matching that number)
      heap.append((list_iterator.next(), list_iterator))
    except StopIteration:
      pass
  heaps_sum = reduce(lambda r, t1: t1[0][0] + r, heap, 0)
  heapq.heapify(heap)
  return heap, heaps_sum

def reduce_occurrence_set(processed_terms, non_processed_terms, dicts):
  def reduce_occurrences():
    # returns the number of occurrences in all dicts for the requested term
    return (-reduce(lambda r, d: d.get(term, 0) + r, dicts, 0), term)

  total_occurrences = []
  heapq.heapify(total_occurrences)
  for term in non_processed_terms:
    processed_terms.add(term)
    heapq.heappush(total_occurrences, reduce_occurrences())
  return total_occurrences

def find_le(a, x):
    'Find rightmost value less than or equal to x'
    i = bisect.bisect_right(a, x)
    if i:
        return i
    raise ValueError

def get_top_occurrences(num_occurrences, dicts, sorted_lists):
  maxs, maxs_sum = create_maxs_heap(sorted_lists)
  total_occurrences = []
  processed_terms = set()
  while maxs:
    current_max, list_iter = heapq.heappop(maxs)
    current_max_number_occurrences, current_max_terms_set = current_max
    try:
      next_max_number_occurrences, next_max_terms_set = list_iter.next()
      maxs_sum += next_max_number_occurrences - current_max_number_occurrences
      heapq.heappush(maxs, ((next_max_number_occurrences, next_max_terms_set), list_iter))
    except StopIteration:
      # nothing to do really, just keep going
      pass
    yet_non_processed_terms = current_max_terms_set.difference(processed_terms)
    if yet_non_processed_terms:
      new_occurrences = reduce_occurrence_set(processed_terms, yet_non_processed_terms, dicts)
      for occurrence in new_occurrences:
        bisect.insort(total_occurrences, occurrence)
      if abs(total_occurrences[0][0]) >= abs(maxs_sum):
        index = find_le([i[0] for i in total_occurrences], maxs_sum)
        if index >= num_occurrences:
          return [(abs(t[0]), t[1]) for t in total_occurrences[:num_occurrences]]
  return [(abs(t[0]), t[1]) for t in total_occurrences[:num_occurrences]]

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
