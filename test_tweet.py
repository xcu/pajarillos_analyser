import unittest
from tweet import Tweet
import json
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

  def test_user_mentions(self):
    tweet = Tweet(json.loads(tweet_samples.user_mentions_tweet))
    self.assertEquals(tweet.get_user_mentions(), [])

