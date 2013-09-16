from streamers.file_streamer import FileStreamer
from messages.tweet import Tweet
from db_test_base import MongoTest
from db.injector import TimeChunkInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import DBManager
from db.time_chunk import TimeChunk, TimeChunkMgr
from datetime import datetime
import tweet_samples
import ujson as json
import unittest


class TestChunkInjector(MongoTest):
  def setUp(self):
    self.tci = TimeChunkInjector(DBManager(self.conn, 'stats', 'time_chunks', index='start_date'))
    super(TestChunkInjector, self).setUp()

  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    im = InjectorManager(registered_injectors=(self.tci,))
    im.to_db(streamer)



class TestTimeChunkMgr(unittest.TestCase):
  def setUp(self):
    self.mgr = TimeChunkMgr()

  def test_load_chunk(self):
    dict = {'size': 1,
            'start_date': 1376646480,
            'subchunks': [{
              'terms': {'term1': 1, 'term2': 2},
              'hashtags': {'one': 3, 'two': 5},
              'users': ['user1'],
              'tweet_ids': [1, 2, 3, 4],
              'user_mentions': {}
           }]}
    c = self.mgr.load_chunk(dict)
    self.assertEquals(type(c), TimeChunk)
    self.assertEquals(c.size, 1)
    self.assertEquals(c.start_date, datetime(2013, 8, 16, 9, 48))
    self.assertEquals(c.subchunks[0].terms, {'term1': 1, 'term2': 2})
    self.assertEquals(c.subchunks[0].hashtags, {'one': 3, 'two': 5})
    self.assertEquals(c.subchunks[0].users, set(['user1']))
    self.assertEquals(c.subchunks[0].tweet_ids, set([1, 2, 3, 4]))
    self.assertEquals(c.subchunks[0].user_mentions, {})
