import oauth2 as oauth
import urllib2 as urllib
from streamers.streamer import Streamer


class HTTPStreamer(Streamer):
  def __init__(self, **kwargs):
    super(HTTPStreamer, self).__init__()
    self.token_key = kwargs.get('token_key', '')
    self.token_secret = kwargs.get('token_secret', '')
    self.consumer_key = kwargs.get('consumer_key', '')
    self.token_secret = kwargs.get('token_secret', '')
    self.consumer_secret = kwargs.get('consumer_secret', '')
    self._debug = 0

  def twitter_request(self, url, http_method, parameters):
    ' Construct, sign, and open a twitter request '
    oauth_token    = oauth.Token(key=self.token_key, secret=self.token_secret)
    oauth_consumer = oauth.Consumer(key=self.consumer_key, secret=self.consumer_secret)
    req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url,
                                             parameters=parameters)
    signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)
    headers = req.to_header()
    if http_method == "POST":
      encoded_post_data = req.to_postdata()
    else:
      encoded_post_data = None
      url = req.to_url()
    opener = urllib.OpenerDirector()
    opener.add_handler(urllib.HTTPHandler(debuglevel=self._debug))
    opener.add_handler(urllib.HTTPSHandler(debuglevel=self._debug))
    response = opener.open(url, encoded_post_data)
    return response

  def __iter__(self):
    #url = "https://stream.twitter.com/1.1/statuses/sample.json"
    #parameters = {}
    #response = twitterreq(url, "GET", parameters)
    #url = "https://stream.twitter.com/1.1/statuses/filter.json?language=en"
    #response = twitterreq(url, "POST", parameters)
    url = "https://stream.twitter.com/1.1/statuses/filter.json?"
    #parameters = {'language': 'en'}
    #parameters = {'track': 'mourinho,ronaldo', 'language': 'es'}
    parameters = {'locations': '-9.472656,36.160774,4.809570,43.153753'}
    response = self.twitter_request(url, "POST", parameters)
    for line in response:
      yield self.create_message(line.strip())

