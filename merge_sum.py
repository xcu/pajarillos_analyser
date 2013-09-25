import bisect
from collections import defaultdict
import heapq


def sort_dict(d):
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
          return ((abs(t[0]), t[1]) for t in total_occurrences[:num_occurrences])
  return ((abs(t[0]), t[1]) for t in total_occurrences[:num_occurrences])


if __name__ == '__main__':
  d1 = {'pepe': 19, 'javi': 17, 'perico': 15, 'manguan': 13, 'cholo': 10}
  d2 = {'paco': 15, 'manolo': 11, 'yoni': 4}
  d3 = {'manolo': 10, 'perico': 8, 'pelanas': 2, 'pepe': 1}
  l1, l2, l3 = (sort_dict(d) for d in (d1, d2, d3))
  print [oc for oc in get_top_occurrences(26, (d1, d2, d3), (l1, l2, l3))]

  '''
  1 - get max from tops-> pepe: 19
  2 - reduce it in the other dictionaries-> pepe: 20
  3 - put it in total_occurrences list
  4 - pop from tops_sum and put next in
  5 - if pepe >= tops_sum FINISHED!
  6 - not finished though, 20 < 17 + 15 + 10
  7 - go back to 1

  Space in memory: 2x (dictionary + heap)
  tops is size m, where m is the number of chunks
  total_occurrences list size is variable, at least it'll be the size of the number
  of elements you want to retrieve. At most it'll be the size of the combined
  dictionaries for each chunk. Fortunately this is not very likely
  '''
