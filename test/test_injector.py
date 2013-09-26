from streamers.file_streamer import FileStreamer
from messages.tweet import Tweet
from db_test_base import MongoTest
from db.injector import ChunkInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import DBManager
from db.chunk_container import ChunkContainer
from datetime import datetime
import tweet_samples
import ujson as json


class TestChunkInjector(MongoTest):
  def setUp(self):
    self.tci = ChunkInjector(DBManager(self.conn, 'stats', 'chunk_containers', index='start_date'))
    super(TestChunkInjector, self).setUp()

  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    im = InjectorManager(registered_injectors=(self.tci,))
    im.to_db(streamer)

  def test_injector_worked(self):
    # 3 chunks
    self.assertEquals(self.conn.database_names(), ['stats'])
    self.assertEquals(self.conn['stats'].collection_names(), [u'system.indexes', u'chunk_containers'])
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)

  def test_to_db(self):
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
    chunk = self.tci.to_db(tweet, None)
    # no db update
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
    tweet = Tweet(json.loads(tweet_samples.non_ascii_tweet))
    # trick it to avoid dp update
    chunk.changed_since_retrieval = False
    self.tci.to_db(tweet, chunk)
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
    # real val
    chunk.changed_since_retrieval = True
    self.tci.to_db(tweet, chunk)
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 4)

  def test_get_chunk_from_date(self):
    # first one that exists:
    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1376646540))
    self.assertEquals(c.complete_chunks[0].tweet_ids, set([u'368308364012691456']))
    # now a random one
    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1076646540))
    self.assertEquals(c.complete_chunks, [])

  def test_chunk_exists(self):
    self.assertTrue(self.tci.chunk_exists(datetime.utcfromtimestamp(1376646540)))
    self.assertFalse(self.tci.chunk_exists(datetime.utcfromtimestamp(1076646540)))

  def test_save_chunk(self):
    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1376646540))
    self.assertEquals(len(c.complete_chunks), 1)
    c.complete_chunks = []
    self.tci.save_chunk(c)
    # pick it again and check it changed
    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1376646540))
    self.assertEquals(len(c.complete_chunks), 0)


# BELONGS TO DB MGR!!
#  def test_reduce_range(self):
#    ids = [c['start_date'] for c in self.conn['stats']['chunk_containers'].find()]
#    lower = datetime.utcfromtimestamp(min(ids))
#    upper = datetime.utcfromtimestamp(max(ids))
#    self.assertEquals(self.tci.reduce_range(lower, upper), {'user_mentions': [(u'aenaaeropuertos', 2), (u'Sammiraa_18', 1), (u'marchante905', 1), (u'MeliaVillaitana', 1), (u'JaviRubira', 1), (u'JSudowoodo', 1)], 'tweet_ids': 20, 'hashtags': [(u'pueblo', 1), (u'matarranya', 1), (u'sienteteruel', 1), (u'TeQuiero', 1), (u'igers_spain', 1), (u'conalma', 1), (u'carmen', 1)], 'terms': [(u'http', 3), (u'han', 2), (u'jajajajaja', 2), (u'aenaaeropuertos', 2), (u'dias', 2), (u'son', 2), (u'her', 2), (u'jaja', 1), (u'marchante905', 1), (u'Buenos', 1), (u'primer', 1), (u'Lleida', 1), (u'Sammiraa_18', 1), (u'ponerse', 1), (u'piscina', 1), (u'jaj', 1), (u'PABLO', 1), (u'fatal', 1), (u'fin', 1), (u'prueba', 1)], 'users': 20})


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


class Test2Injectors(MongoTest):
  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    ti = TweetInjector(DBManager(self.conn, 'raw', 'tweets', index='id'))
    tci = ChunkInjector(DBManager(self.conn, 'stats', 'chunk_containers', index='start_date'))
    im = InjectorManager(registered_injectors=(ti, tci))
    im.to_db(streamer)

  def test_injector_worked(self):
    self.assertEquals(set(self.conn.database_names()), set(['raw', 'stats']))
    self.assertEquals(self.conn['stats'].collection_names(), [u'system.indexes', u'chunk_containers'])
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
    self.assertEquals(self.conn['raw'].collection_names(), [u'system.indexes', u'tweets'])
    self.assertEquals(self.conn['raw']['tweets'].find().count(), 20)

