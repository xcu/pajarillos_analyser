from messages.message_factory import MessageFactory
from messages.tweet import Tweet
from messages.delete import Delete
from messages.scrub_geo import ScrubGeo
from messages.limit import Limit
from messages.status_withheld import StatusWithHeld
from messages.user_withheld import UserWithHeld
from messages.disconnect import Disconnect
from messages.warning import Warning
import unittest


class TestMessageFactory(unittest.TestCase):

  sample_delete = {"delete": {"status": {"id": 1234,
                                         "id_str": "1234",
                                         "user_id": 3,
                                         "user_id_str": "3"}}}
  sample_scrub_geo = {"scrub_geo": {
    "user_id": 14090452,
    "user_id_str": "14090452",
    "up_to_status_id": 23260136625,
    "up_to_status_id_str": "23260136625"}}

  sample_limit = {"limit": {"track":1234}}

  sample_status_withheld = {"status_withheld": {
      "id":1234567890,
      "user_id":123456,
      "withheld_in_countries":["DE", "AR"]}}

  sample_user_withheld = {"user_withheld": {
    "id":123456,
    "withheld_in_countries":["DE","AR"]}}

  sample_disconnect = {"disconnect": {
    "code": 4,
    "stream_name":"< A stream identifier >",
    "reason":"< Human readable status message >"}}

  sample_warning = {"warning": {
    "code":"FALLING_BEHIND",
    "message":"Your connection is falling behind and messages are being queued for delivery to you. Your queue is now over 60% full. You will be disconnected when the queue is full.",
    "percent_full": 60}}
  sample_tweet = {"created_at":"Wed Aug 07 08:30:39 +0000 2013","id":365027194319282176,"id_str":"365027194319282176","text":"@PhilipSyren hej och v\u00e4lkommen till v\u00e5rt fl\u00f6de! =)","source":"web","truncated":False,"in_reply_to_status_id":None,"in_reply_to_status_id_str":None,"in_reply_to_user_id":44958329,"in_reply_to_user_id_str":"44958329","in_reply_to_screen_name":"PhilipSyren","user":{"id":569034753,"id_str":"569034753","name":"UF Halland","screen_name":"uf_halland","location":"Halmstad","url":"http:\/\/ungforetagsamhet.se\/halland","description":"Vi utbildar i entrepren\u00f6rskap och erbjuder verktyg f\u00f6r entrepren\u00f6riellt l\u00e4rande i Halland. Vi tror p\u00e5 unga m\u00e4nniskors f\u00f6retagsamhet!","protected":False,"followers_count":150,"friends_count":239,"listed_count":2,"created_at":"Wed May 02 09:26:34 +0000 2012","favourites_count":21,"utc_offset":3600,"time_zone":"London","geo_enabled":True,"verified":False,"statuses_count":250,"lang":"sv","contributors_enabled":False,"is_translator":False,"profile_background_color":"ACDED6","profile_background_image_url":"http:\/\/a0.twimg.com\/images\/themes\/theme18\/bg.gif","profile_background_image_url_https":"https:\/\/si0.twimg.com\/images\/themes\/theme18\/bg.gif","profile_background_tile":False,"profile_image_url":"http:\/\/a0.twimg.com\/profile_images\/2184240960\/logoBrun_normal.jpg","profile_image_url_https":"https:\/\/si0.twimg.com\/profile_images\/2184240960\/logoBrun_normal.jpg","profile_link_color":"038543","profile_sidebar_border_color":"EEEEEE","profile_sidebar_fill_color":"F6F6F6","profile_text_color":"333333","profile_use_background_image":True,"default_profile":False,"default_profile_image":False,"following":None,"follow_request_sent":None,"notifications":None},"geo":None,"coordinates":None,"place":{"id":"729cfb1ea4791470","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/729cfb1ea4791470.json","place_type":"city","name":"Halmstad","full_name":"Halmstad, Hallands L\u00e4n","country_code":"SE","country":"Sverige","bounding_box":{"type":"Polygon","coordinates":[[[12.2417066,56.5421347],[12.2417066,56.9442902],[13.3250779,56.9442902],[13.3250779,56.5421347]]]},"attributes":{}},"contributors":None,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"symbols":[],"urls":[],"user_mentions":[{"screen_name":"PhilipSyren","name":"Philip Syr\u00e9n","id":44958329,"id_str":"44958329","indices":[0,12]}]},"favorited":False,"retweeted":False,"filter_level":"medium","lang":"sv"}

  def setUp(self):
    self.mf = MessageFactory()

  def test_create_message(self):
    self.assertEquals(type(self.mf.create_message(self.sample_delete)), Delete)
    self.assertEquals(type(self.mf.create_message(self.sample_tweet)), Tweet)
    self.assertEquals(type(self.mf.create_message(self.sample_scrub_geo)), ScrubGeo)
    self.assertEquals(type(self.mf.create_message(self.sample_limit)), Limit)
    self.assertEquals(type(self.mf.create_message(self.sample_status_withheld)), StatusWithHeld)
    self.assertEquals(type(self.mf.create_message(self.sample_user_withheld)), UserWithHeld)
    self.assertEquals(type(self.mf.create_message(self.sample_disconnect)), Disconnect)
    self.assertEquals(type(self.mf.create_message(self.sample_warning)), Warning)

  def test_non_supported(self):
    non_supported = {'haha': 'i dun goof'}
    self.assertRaises(Exception, self.mf.create_message, non_supported)

