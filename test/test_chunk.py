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
    self.mgr = ChunkMgr()
    chunk_samples.chunk_sample['parent_container'] = datetime(2013, 8, 16, 9, 48)
    chunk_samples.chunk_sample_small1['parent_container'] = datetime(2013, 8, 16, 9, 49)
    chunk_samples.chunk_sample_small2['parent_container'] = datetime(2013, 8, 16, 9, 50)
    chunk_samples.chunk_container_sample["size"] = 10

  def test_load_empty_chunk_container(self):
    c = self.mgr.load_empty_chunk_container(3, datetime(2013, 10, 4, 9, 8))
    self.assertEquals(c.size, 3)
    self.assertEquals(c.start_date, datetime(2013, 10, 4, 9, 8))
    self.assertEquals(c.changed_since_retrieval, False)
    self.assertEquals(c.chunks, {})
    self.assertEquals(type(c.current_chunk), tuple)
    self.assertEquals(c.current_chunk[0], None)
    self.assertEquals(type(c.current_chunk[1]), Chunk)

  def test_load_chunk_container(self):
    c = self.mgr.load_chunk_container(chunk_samples.chunk_container_sample)
    self.assertEquals(type(c), ChunkContainer)
    self.assertEquals(c.size, 10)
    self.assertEquals(c.start_date, 1376646480)

  def test_load_chunk(self):
    parent = chunk_samples.chunk_sample['parent_container']
    c = self.mgr.load_chunk(parent, chunk_samples.chunk_sample)
    self.assertEquals(c.parent_container, parent)
    self.assertEqual(c.terms, {"de" : 25, "y" : 14, "http" : 14, "co" : 14, "es" : 14, "por" : 6, "s" : 6, "o" : 6, "n" : 6})

  def test_get_top_occurrences(self):
    chunks = (chunk_samples.chunk_sample,
              chunk_samples.chunk_sample_small1,
              chunk_samples.chunk_sample_small2)
    parents = (c['parent_container'] for c in chunks)
    chunks = [Chunk(parent, **chunk) for chunk, parent in izip(chunks, parents)]
    r = self.mgr.get_top_occurrences(chunks, 4)
    self.assertEquals(r, {'user_mentions': [(4, 'Fulendstambulen'), (4, 'el_fary'), (2, 'Los40_Spain'), (2, 'williamlevybra')],
                          'hashtags': [(4, '10CosasQueOdio'), (3, 'nature'), (1, 'PutaVidaTete')],
                          'terms': [(25, 'de'), (20, 'pollo'), (15, 'froyo'), (14, 'co')]})

class TestChunkContainer(unittest.TestCase):
  def setUp(self):
    self.container = chunk_samples.chunk_container_sample
    chunk_samples.chunk_container_sample["size"] = 10

  def test_constructor(self):
    cc = ChunkContainer(self.container.pop('size'), 1376646480, self.container)
    self.assertTrue(type(cc.current_chunk[1]) == Chunk)
    self.assertEquals(cc.current_chunk[0], None)
    
  def test_constructor_bad_size(self):
    self.assertRaises(AssertionError, ChunkContainer, 100, 1376646480, **chunk_samples.chunk_container_sample_bad_size)
