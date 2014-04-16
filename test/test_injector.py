from streamers.file_streamer import FileStreamer
from messages.tweet import Tweet
from db_test_base import MongoTest
from db.injector import ChunkContainerInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import ObjDB
from datetime import datetime, time
import tweet_samples
import ujson as json
from mock import MagicMock, patch


class TestChunkContainerInjector(MongoTest):
  def setUp(self):
    self.tci = ChunkContainerInjector(self.conn, 'stats')
    super(TestChunkContainerInjector, self).setUp()

  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    im = InjectorManager(registered_injectors=(self.tci,))
    im.to_db(streamer)

  def test_injector_worked(self):
    # 3 chunks
    self.assertEquals(self.conn.database_names(), ['stats'])
    self.assertEquals([u'system.indexes', u'chunks', u'chunk_containers'],
                      self.conn['stats'].collection_names())
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)

  def test_non_time_based_unprocessed(self):
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    tweet.is_time_based = MagicMock(return_value=False)
    self.assertTrue(self.tci.current_chunk_container is not None)
    self.tci.current_chunk_container.tweet_fits = MagicMock()
    self.tci.to_db(tweet)
    # nothing called
    self.tci.current_chunk_container.tweet_fits.assert_has_calls([])

  def test_non_current_chunk(self):
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    self.tci.current_chunk_container = None
    self.tci.to_db(tweet)
    self.assertEquals(datetime(2013, 8, 10, 18, 44),
                      self.tci.current_chunk_container.start_date)
    chunk = self.tci.current_chunk_container.current_chunk
    self.assertTrue(chunk.changed_since_retrieval)
    self.assertTrue([] != chunk.tweet_ids)

  def test_to_db_generic(self):
    # 3 containers: 9:48, 9:49 and 9:50
    # created at 18:44, current chunk pointing at 9:48
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
    self.tci.to_db(tweet)
    # no db update, current_chunk points now at 18:44 and has changed_since_retrieval
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
    # created at 18:40
    tweet = Tweet(json.loads(tweet_samples.non_ascii_tweet))
    self.tci.to_db(tweet)
    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 4)

  def test_pick_container_from_msg_date(self):
    # newly created container
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    c = self.tci.pick_container_from_msg_date(tweet)
    self.assertEquals(datetime(2013, 8, 10, 18, 44), c.start_date)
    # container from db, there is a container in the db with date 9:48
    tweet.message['created_at'] = "Fri Aug 16 09:48:55 +0000 2013"
    c = self.tci.pick_container_from_msg_date(tweet)
    self.assertEquals(datetime(2013, 8, 16, 9, 48), c.start_date)

  def test_refresh_current_container(self):
    self.assertTrue(self.tci.current_chunk_container.changed_since_retrieval)
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    previous_container = self.tci.current_chunk_container
    self.tci.container_mgr.save_in_db = MagicMock()
    self.tci._refresh_current_container(tweet)
    self.assertNotEquals(self.tci.current_chunk_container, previous_container)
    # the previous container was stored in the db
    self.tci.container_mgr.save_in_db.assert_called_once_with(previous_container)

  def test_get_associated_container_key(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.tci.container_mgr = MagicMock()
    self.tci.container_mgr.size = 10
    self.assertEquals(self.tci._get_associated_container_key(tweet).time(), time(8, 30))
    tweet.message['created_at'] = u'Wed Aug 07 08:44:39 +0000 2013'
    self.assertEquals(self.tci._get_associated_container_key(tweet).time(), time(8, 40))
    self.tci.container_mgr.size = 50
    self.assertRaises(Exception, self.tci._get_associated_container_key, tweet)
    self.tci.container_mgr.size = 120
    self.assertRaises(Exception, self.tci._get_associated_container_key, tweet)
    tweet.message['created_at'] = u'Wed Aug 07 00:59:39 +0000 2013'
    self.tci.container_mgr.size = 30
    self.assertEquals(self.tci._get_associated_container_key(tweet).time(), time(0, 30))


  # TODO: belongs to db manager
#  def test_get_chunk_from_date(self):
#    # first one that exists:
#    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1376646540))
#    self.assertEquals(c.chunks[0].tweet_ids, set([u'368308364012691456']))
#    # now a random one
#    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1076646540))
#    self.assertEquals(c.chunks, [])

  # TODO: refactor, it now calls _get_chunk_container_from_db
#  def test_chunk_exists(self):
#    self.assertTrue(self.tci.chunk_exists(datetime.utcfromtimestamp(1376646540)))
#    self.assertFalse(self.tci.chunk_exists(datetime.utcfromtimestamp(1076646540)))

#  def test_save_chunk(self):
#    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1376646540))
#    self.assertEquals(len(c.chunks), 1)
#    c.chunks = []
#    self.tci.save_chunk(c)
#    # pick it again and check it changed
#    c = self.tci.get_chunk_from_date(datetime.utcfromtimestamp(1376646540))
#    self.assertEquals(len(c.chunks), 0)


# BELONGS TO DB MGR!!
#  def test_reduce_range(self):
#    ids = [c['start_date'] for c in self.conn['stats']['chunk_containers'].find()]
#    lower = datetime.utcfromtimestamp(min(ids))
#    upper = datetime.utcfromtimestamp(max(ids))
#    self.assertEquals(self.tci.reduce_range(lower, upper), {'user_mentions': [(u'aenaaeropuertos', 2), (u'Sammiraa_18', 1), (u'marchante905', 1), (u'MeliaVillaitana', 1), (u'JaviRubira', 1), (u'JSudowoodo', 1)], 'tweet_ids': 20, 'hashtags': [(u'pueblo', 1), (u'matarranya', 1), (u'sienteteruel', 1), (u'TeQuiero', 1), (u'igers_spain', 1), (u'conalma', 1), (u'carmen', 1)], 'terms': [(u'http', 3), (u'han', 2), (u'jajajajaja', 2), (u'aenaaeropuertos', 2), (u'dias', 2), (u'son', 2), (u'her', 2), (u'jaja', 1), (u'marchante905', 1), (u'Buenos', 1), (u'primer', 1), (u'Lleida', 1), (u'Sammiraa_18', 1), (u'ponerse', 1), (u'piscina', 1), (u'jaj', 1), (u'PABLO', 1), (u'fatal', 1), (u'fin', 1), (u'prueba', 1)], 'users': 20})


class TestTweetInjector(MongoTest):
  def load_fixture(self):
    streamer = FileStreamer('mock_db_data')
    ti = TweetInjector(ObjDB(self.conn, 'raw', 'id', 'tweets'))
    im = InjectorManager(registered_injectors=(ti,))
    im.to_db(streamer)

  def test_injector_worked(self):
    # 20 tweets
    self.assertEquals(self.conn.database_names(), ['raw'])
    self.assertEquals(self.conn['raw'].collection_names(), [u'system.indexes', u'tweets'])
    self.assertEquals(self.conn['raw']['tweets'].find().count(), 20)
#
#
#class Test2Injectors(MongoTest):
#  def load_fixture(self):
#    streamer = FileStreamer('mock_db_data')
#    ti = TweetInjector(ObjDB(self.conn, 'raw', 'tweets', index='id'))
#    tci = ChunkContainerInjector(ChunkDB(self.conn, 'stats'))
#    im = InjectorManager(registered_injectors=(ti, tci))
#    im.to_db(streamer)
#
#  def test_injector_worked(self):
#    self.assertEquals(set(self.conn.database_names()), set(['raw', 'stats']))
#    self.assertEquals(self.conn['stats'].collection_names(), [u'system.indexes', u'chunk_containers'])
#    self.assertEquals(self.conn['stats']['chunk_containers'].find().count(), 3)
#    self.assertEquals(self.conn['raw'].collection_names(), [u'system.indexes', u'tweets'])
#    self.assertEquals(self.conn['raw']['tweets'].find().count(), 20)

