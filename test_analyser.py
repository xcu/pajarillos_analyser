from messages import *
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

  def setUp(self):
    self.mf = MessageFactory()

  def test_create_message(self):
    
