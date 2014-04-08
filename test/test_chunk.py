from streamers.file_streamer import FileStreamer
from messages.tweet import Tweet
from db_test_base import MongoTest
from db.injector import ChunkInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import DBChunkManager
from db.chunk import ChunkContainer, ChunkMgr, Chunk
from datetime import datetime
import tweet_samples
import chunk_samples
import ujson as json
import unittest
from itertools import izip


class TestChunkInjector(MongoTest):
  def setUp(self):
    self.tci = ChunkInjector(DBChunkManager(self.conn, 'stats'))
    super(TestChunkInjector, self).setUp()

  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    im = InjectorManager(registered_injectors=(self.tci,))
    im.to_db(streamer)



class TestChunkMgr(unittest.TestCase):
  def setUp(self):
    self.mgr = ChunkMgr(xxxxxxxxxxxxx)
    self.chunk = chunk_samples.chunk_sample.copy()
    chunk_samples.chunk_sample['parent_container'] = datetime(2013, 8, 16, 9, 48)
    chunk_samples.chunk_sample_small1['parent_container'] = datetime(2013, 8, 16, 9, 49)
    chunk_samples.chunk_sample_small2['parent_container'] = datetime(2013, 8, 16, 9, 50)
    chunk_samples.chunk_container_sample["size"] = 10

  def test_get_empty_chunk_container(self):
    c = self.mgr.get_empty_chunk_container(3, datetime(2013, 10, 4, 9, 8))
    self.assertEquals(c.size, 3)
    self.assertEquals(c.start_date, datetime(2013, 10, 4, 9, 8))
    self.assertEquals(c.changed_since_retrieval, False)
    self.assertEquals(c.chunks, {})
    self.assertEquals(type(c.current_chunk), tuple)
    self.assertEquals(c.current_chunk[0], None)
    self.assertEquals(type(c.current_chunk[1]), Chunk)

  def test_get_chunk_container(self):
    c = self.mgr.get_chunk_container(chunk_samples.chunk_container_sample)
    self.assertEquals(type(c), ChunkContainer)
    self.assertEquals(c.size, 10)
    self.assertEquals(c.start_date, datetime(2013, 8, 16, 9, 48))

  def test_get_chunk(self):
    parent = chunk_samples.chunk_sample['parent_container']
    c = self.mgr.get_chunk(self.chunk.pop('parent_container'), self.chunk)
    self.assertEquals(c.parent_container, parent)
    self.assertEqual(c.terms, {"de" : 25, "y" : 14, "http" : 14, "co" : 14, "es" : 14, "por" : 6, "s" : 6, "o" : 6, "n" : 6})

  def test_get_top_occurrences(self):
    chunks = (chunk_samples.chunk_sample.copy(),
              chunk_samples.chunk_sample_small1.copy(),
              chunk_samples.chunk_sample_small2.copy())
    chunks = [Chunk(chunk.pop('parent_container'), **chunk) for chunk in chunks]
    r = self.mgr.get_top_occurrences(chunks, 4)
    self.assertEquals(r, {'user_mentions': [(4, 'Fulendstambulen'), (4, 'el_fary'), (2, 'Los40_Spain'), (2, 'williamlevybra')],
                          'hashtags': [(4, '10CosasQueOdio'), (3, 'nature'), (1, 'PutaVidaTete')],
                          'terms': [(25, 'de'), (20, 'pollo'), (15, 'froyo'), (14, 'co')]})

class TestChunkContainer(unittest.TestCase):
  def setUp(self):
    self.container = chunk_samples.chunk_container_sample.copy()

  def test_constructor_from_db(self):
    cc = ChunkContainer(self.container.pop('size'), self.container.pop('start_date'), **self.container)
    self.assertEquals(type(cc.current_chunk[0]), str)
    self.assertEquals(cc.current_chunk[1], None)

  def test_constructor_fresh_obj(self):
    self.container.pop("current_chunk")
    cc = ChunkContainer(self.container.pop('size'), self.container.pop('start_date'), **self.container)
    self.assertEquals(type(cc.current_chunk[1]), Chunk)
    self.assertEquals(cc.current_chunk[0], None)
    
  def test_constructor_bad_size(self):
    self.container = chunk_samples.chunk_container_sample_bad_size
    self.assertRaises(AssertionError, ChunkContainer, self.container.pop('size'),
                                                      self.container.pop('start_date'),
                                                      **self.container)

  def test_default(self):
    self.container = chunk_samples.chunk_container_with_chunks.copy()
    cc = ChunkContainer(self.container.pop('size'), self.container.pop('start_date'), **self.container)
    self.assertEquals(cc.default(), {'chunks': ['1', '2'],
                                     'chunk_size': 100,
                                     'current_chunk': '52499970e138235994c416a3',
                                     'start_date': 1376646480,
                                     'size': 10})

  def test_tweet_fits(self):
    class MockTweet(object):
      def __init__(self, creation):
        self.creation_time = creation
      def get_creation_time(self):
        return self.creation_time
    cc = ChunkContainer(self.container.pop('size'), self.container.pop('start_date'), **self.container)
    tweet = MockTweet(datetime(2013, 8, 16, 9, 47))
    self.assertFalse(cc.tweet_fits(tweet))
    tweet = MockTweet(datetime(2013, 8, 16, 9, 48))
    self.assertTrue(cc.tweet_fits(tweet))
    # container size is 10
    tweet = MockTweet(datetime(2013, 8, 16, 9, 57))
    self.assertTrue(cc.tweet_fits(tweet))
    tweet = MockTweet(datetime(2013, 8, 16, 9, 57, 59))
    self.assertTrue(cc.tweet_fits(tweet))
    tweet = MockTweet(datetime(2013, 8, 16, 9, 58))
    self.assertFalse(cc.tweet_fits(tweet))

  def test_update(self):
    class MockTweet(object):
      def __init__(self, tweetid):
          self.id = tweetid
      def get_id(self):
          return self.id
    self.container = chunk_samples.chunk_container_with_chunks.copy()
    cc = ChunkContainer(self.container.pop('size'), self.container.pop('start_date'), **self.container)
    for chunk in cc.chunks.iterkeys():
        cc.chunks[chunk] = Chunk(cc.chunks[chunk].pop("parent_container"), **cc.chunks[chunk])
    cc.current_chunk[1].update = lambda x: x
    cc.update(MockTweet("584700299958824960"))
    self.assertFalse(cc.changed_since_retrieval)
    cc.update(MockTweet("i'm changing you"))
    self.assertTrue(cc.changed_since_retrieval)