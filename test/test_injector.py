from test_db import MongoTest
from streamers.file_streamer import FileStreamer
from db.injector import TimeChunkInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import DBManager

class TestChunkInjector(MongoTest):
  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    tci = TimeChunkInjector(DBManager(self.conn, 'stats', 'time_chunks', index='start_date'))
    im = InjectorManager(registered_injectors=(tci,))
    im.to_db(streamer)

  def test_injector_worked(self):
    # 3 chunks
    self.assertEquals(self.conn.database_names(), ['stats'])
    self.assertEquals(self.conn['stats'].collection_names(), [u'system.indexes', u'time_chunks'])
    self.assertEquals(self.conn['stats']['time_chunks'].find().count(), 3)

class TestTweetInjector(MongoTest):
  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    ti = TweetInjector(DBManager(self.conn, 'raw', 'tweets', index='id'))
    im = InjectorManager(registered_injectors=(ti,))
    im.to_db(streamer)

  def test_injector_worked(self):
    # 20 tweets
    self.assertEquals(self.conn.database_names(), ['raw'])
    self.assertEquals(self.conn['raw'].collection_names(), [u'system.indexes', u'tweets'])
    self.assertEquals(self.conn['raw']['tweets'].find().count(), 20)

