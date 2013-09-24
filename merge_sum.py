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

def create_relative_maxs_heap(sorted_lists):
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

def reduce_occurrence_set(processed_terms, s, dicts):
  def reduce_occurrences():
    # returns the number of occurrences in all dicts for the requested term
    return (-reduce(lambda r, d: d.get(term, 0) + r, dicts, 0), term)

  total_occurrences = []
  heapq.heapify(total_occurrences)
  for term in s:
    processed_terms.add(term)
    heapq.heappush(total_occurrences, reduce_occurrences())
  return total_occurrences

def find_le(a, x):
    'Find rightmost value less than or equal to x'
    i = bisect.bisect_right(a, x)
    if i:
        return i
    raise ValueError

if __name__ == '__main__':
  d1 = {'pepe': 19, 'javi': 17, 'perico': 15, 'manguan': 13, 'cholo': 10}
  d2 = {'paco': 15, 'manolo': 11, 'yoni': 4}
  d3 = {'manolo': 10, 'perico': 8, 'pelanas': 2, 'pepe': 1}
  l1, l2, l3 = (sort_dict(d) for d in (d1, d2, d3))
  relative_maxs, relative_maxs_sum = create_relative_maxs_heap((l1, l2, l3))
  total_occurrences = []
  processed_terms = set()
  while True:
    # next biggest number of occurrences
    absolute_max, list_iter = heapq.heappop(relative_maxs)
    absolute_max_number_occurrences, absolute_max_terms_set = absolute_max
    try:
      next_max_number_occurrences, next_max_terms_set = list_iter.next()
    except StopIteration:
      continue
    relative_maxs_sum += next_max_number_occurrences - absolute_max_number_occurrences
    heapq.heappush(relative_maxs, ((next_max_number_occurrences, next_max_terms_set), list_iter))
    if absolute_max_terms_set.difference(processed_terms):
      new_occurrences = reduce_occurrence_set(processed_terms, absolute_max_terms_set, (d1, d2, d3))
      for occurrence in new_occurrences:
        bisect.insort(total_occurrences, occurrence)
      if abs(total_occurrences[0][0]) >= abs(relative_maxs_sum):
        index = find_le([i[0] for i in total_occurrences], relative_maxs_sum)
        print total_occurrences
        print total_occurrences[:index]
        print relative_maxs_sum
        exit(0)

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
