from streamers.file_streamer import FileStreamer
from messages.tweet import Tweet
from db_test_base import MongoTest
from db.injector import ChunkInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import DBManager
from db.chunk_container import ChunkContainer, ChunkMgr, Chunk
from datetime import datetime
import tweet_samples
import ujson as json
import unittest


class TestChunkInjector(MongoTest):
  def setUp(self):
    self.tci = ChunkInjector(DBManager(self.conn, 'stats', 'chunk_containers', index='start_date'))
    super(TestChunkInjector, self).setUp()

  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    im = InjectorManager(registered_injectors=(self.tci,))
    im.to_db(streamer)


chunk_dict1 = {'size': 1,
               'start_date': 1376646480,
               'complete_chunks': [{
                 'terms': {'term1': 1, 'term2': 2},
                 'hashtags': {'one': 3, 'two': 5},
                 'users': ['user1'],
                 'tweet_ids': [1, 2, 3, 4],
                 'user_mentions': {}
              }]}

chunk_dict2 = {'size': 1,
               'start_date': 1376646540,
               'complete_chunks': [{
                 'terms': {'term1': 10},
                 'hashtags': {'one': 2, 'three': 3},
                 'users': ['user3', 'user5', 'user9001'],
                 'tweet_ids': [5, 6, 7],
                 'user_mentions': {'user5': 2}
              }]}

chunk_dict3 = {'size': 1,
               'start_date': 1376646600,
               'complete_chunks': [{
                 'terms': {'term1': 50, 'term2': 2, 'term10': 1},
                 'hashtags': {},
                 'users': ['user7'],
                 'tweet_ids': [8, 9],
                 'user_mentions': {'user7': 3}
              }]}


class TestChunkMgr(unittest.TestCase):
  def setUp(self):
    self.mgr = ChunkMgr()

  def test_load_chunk(self):
    c = self.mgr.load_chunk(chunk_dict1)
    self.assertEquals(type(c), ChunkContainer)
    self.assertEquals(c.size, 1)
    self.assertEquals(c.start_date, datetime(2013, 8, 16, 9, 48))
    self.assertEquals(c.complete_chunks[0].terms, {'term1': 1, 'term2': 2})
    self.assertEquals(c.complete_chunks[0].hashtags, {'one': 3, 'two': 5})
    self.assertEquals(c.complete_chunks[0].users, set(['user1']))
    self.assertEquals(c.complete_chunks[0].tweet_ids, set([1, 2, 3, 4]))
    self.assertEquals(c.complete_chunks[0].user_mentions, {})

  def test_reduce_chunks(self):
    l = [chunk_dict1, chunk_dict2, chunk_dict3]
    self.assertEquals(self.mgr.reduce_chunks(l), {})

  def test_get_top_occurrences(self):
    pass

