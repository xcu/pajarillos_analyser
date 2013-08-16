import unittest
from messages.tweet import Tweet
import ujson as json
from datetime import time
import tweet_samples

class TestTweet(unittest.TestCase):

  def test_get_text_nonascii(self):
    tweet = Tweet(json.loads(tweet_samples.non_ascii_tweet))
    self.assertEquals(tweet.get_text(), tweet_samples.non_ascii_text)

  def test_get_text_standard_ascii(self):
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    self.assertEquals(tweet.get_text(), tweet_samples.standard_ascii_text)

  def test_get_text_extended_ascii(self):
    tweet = Tweet(json.loads(tweet_samples.extended_ascii_tweet))
    self.assertEquals(tweet.get_text(), tweet_samples.extended_ascii_text)

  def test_get_user_mentions(self):
    tweet = Tweet(json.loads(tweet_samples.user_mentions_tweet))
    self.assertEquals(tweet.get_user_mentions(), [u'CedricDickies', u'AumS_TTD'])
    self.assertEquals(tweet.get_user_mentions(prop='indices'), [[3, 17], [19, 28]])
    self.assertEquals(tweet.get_user_mentions(prop=''), [{u'indices': [3, 17],
                                                          u'screen_name': u'CedricDickies',
                                                          u'id': 488837080,
                                                          u'name': u'Cedric Dickies',
                                                          u'id_str': u'488837080'},
                                                         {u'indices': [19, 28],
                                                          u'screen_name': u'AumS_TTD',
                                                          u'id': 533771089,
                                                          u'name': u'Purple Drank \u274c\u274c\u2757',
                                                          u'id_str': u'533771089'}])

  def test_get_user_mentions_empty(self):
    tweet = Tweet(json.loads(tweet_samples.no_mentions_tweet))
    self.assertEquals(tweet.get_user_mentions(), [])

  def test_get_hashtags(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.assertEquals(tweet.get_hashtags(), [u'SougoFollow', u'THF', u'TeamFollowBack', u'HITFOLLOWSTEAM', u'TFBJP', u'OpenFollow', u'MustFollow'])

  def test_get_hashtags_empty(self):
    tweet = Tweet(json.loads(tweet_samples.no_hashtags_tweet))
    self.assertEquals(tweet.get_hashtags(), [])

  def test_get_creation_time(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    d = tweet.get_creation_time()
    self.assertEquals(d.year, 2013)
    self.assertEquals(d.month, 8)
    self.assertEquals(d.day, 7)
    self.assertEquals(d.hour, 8)
    self.assertEquals(d.minute, 30)
    self.assertEquals(d.second, 39)
    d = tweet.get_creation_time(process=False)
    self.assertEquals(d, u'Wed Aug 07 08:30:39 +0000 2013')

  def test_get_creation_time_empty(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    tweet.message.created_at = ''
    self.assertEquals(tweet.get_creation_time(), None)

  def test_get_id(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.assertEquals(tweet.get_id(), u'365027194331865088')

  def test_get_retweet_count(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.assertEquals(tweet.get_retweet_count(), 4)

  def test_get_favorite_count(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.assertEquals(tweet.get_favorite_count(), 6)

  def test_process_by_time(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.assertEquals(tweet._process_by_time(10).time(), time(8, 30))
    tweet.message.created_at = u'Wed Aug 07 08:44:39 +0000 2013'
    self.assertEquals(tweet._process_by_time(10).time(), time(8, 40))
    self.assertRaises(Exception, tweet._process_by_time, 50)
    self.assertRaises(Exception, tweet._process_by_time, 120)
    tweet.message.created_at = u'Wed Aug 07 00:59:39 +0000 2013'
    self.assertEquals(tweet._process_by_time(30).time(), time(0, 30))

