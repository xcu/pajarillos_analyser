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
    tweet.message['created_at'] = ''
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

  def test_get_user(self):
    tweet = Tweet(json.loads(tweet_samples.user_mentions_tweet))
    self.assertEquals(tweet.get_user(), 'AumS_TTD')
    self.assertEquals(tweet.get_user(field='id'), 533771089)

  def test_get_user_wrong_field(self):
    tweet = Tweet(json.loads(tweet_samples.user_mentions_tweet))
    self.assertEquals(tweet.get_user(field='i dont exist'), '')

  def test_get_terms(self):
    tweet = Tweet(json.loads(tweet_samples.hashtags_tweet))
    self.assertEquals(tweet.get_terms(), {u'RT': 1, u'HITFOLLOWSTEAM': 1, u'HitFollowsJp': 1, u'TeamFollowBack': 1, u'OpenFollow': 1, u'SougoFollow': 1, u'ONLY': 1, u'FOLLOWERS': 1, u'WANT': 1, u'THF': 1, u'NEW': 1, u'YOU': 1, u'TFBJP': 1, u'RETWEET': 1, u'MustFollow': 1, u'IF': 1})
    tweet = Tweet(json.loads(tweet_samples.extended_ascii_tweet))
    self.assertEquals(tweet.get_terms(), {u'el': 2, u'gracias': 1, u'la': 1, u'tienen': 1, u'pero': 1, u'al': 1, u'da': 1, u'los': 1, u'miedo': 2, u'que': 1, u'ra': 1, u'misma': 1, u'problemas': 1, u'amor': 2, u'z': 1, u'desaparece': 1, u'Todos': 1, u'nos': 1})

  def test_get_location(self):
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    self.assertEquals({u'type': u'Point', u'coordinates': [-6.827979, 37.132955000000003]},
                      tweet.get_location())

  def test_get_lang(self):
    tweet = Tweet(json.loads(tweet_samples.standard_ascii_tweet))
    self.assertEquals('es', tweet.get_lang())
