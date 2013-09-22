import bisect
import heapq

class HeapTuple(object):
  ''' just a wrapper for tuples in which we define a descending order for the number
      of occurrences. This will be useful to sort elements in heaps '''
  def __init__(self, tup):
    self.t = tup

  def __cmp__(self, other):
#    return self.t[1] - other.t[1]
     return other.t[1] - self.t[1]

  def default(self):
    return self.t

  def __str__(self):
    return str(self.t)

  def __add__(self, other):
    return self.t[1] + other.t[1]

def heap_dict(d):
  l = [HeapTuple(t) for t in d.iteritems()]
  heapq.heapify(l)
  return l

def top(l):
  return heapq.nsmallest(1, l)[0]

def create_tops(heaps):
  heap = [top(h) for h in heaps]
  heaps_sum = sum(heap)
  heapq.heapify(heap)
  return heap, heaps_sum
  

if __name__ == '__main__':
  d1 = {'pepe': 19, 'javi': 17, 'perico': 15, 'manguan': 13, 'cholo': 10}
  d2 = {'paco': 15, 'manolo': 11, 'yoni': 4}
  d3 = {'manolo': 10, 'perico': 8, 'pelanas': 2, 'pepe': 1}
  l1, l2, l3 = (heap_dict(d) for d in (d1, d2, d3))
  tops, tops_sum = create_tops((l1, l2, l3))
  '''
  1 - get max from tops-> pepe: 19
  2 - reduce it in the other dictionaries-> pepe: 20
  3 - put it in result list
  4 - pop from tops_sum and put next in
  5 - if pepe >= tops_sum FINISHED!
  6 - not finished though, 20 < 17 + 15 + 10
  7 - go back to 1

  Space in memory: 2x (dictionary + heap)
  tops is size m, where m is the number of chunks
  result list size is variable, at least it'll be the size of the number
  of elements you want to retrieve. At most it'll be the size of the combined
  dictionaries for each chunk. Fortunately this is not very likely
  '''
