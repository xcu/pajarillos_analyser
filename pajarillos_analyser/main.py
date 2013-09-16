from pymongo import MongoClient
from db.injector import TimeChunkInjector, TweetInjector
from db.injector_manager import InjectorManager
from db.db_manager import DBManager
from streamers.file_streamer import FileStreamer
from streamers.http_streamer import HTTPStreamer
from streamers.db_streamer import DBStreamer
from utils import chunks_are_equal
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('analyser')

client = MongoClient('localhost', 27017)

# TODO: time chunk queue
# sub chunks
# unit tests!

if __name__ == '__main__':
  # streamer = FileStreamer('spanish_results')
  #streamer = DBStreamer(DBManager(client, 'raw', 'tweets'))
  #chunks_are_equal(client['stats']['time_chunks'], client['stats']['time_chunks_fromdb'])
  streamer = HTTPStreamer(**dict(line.split() for line in open("token_data")))
  tci = TimeChunkInjector(DBManager(client, 'stats', 'time_chunks', index='start_date'))
  im = InjectorManager(registered_injectors=(tci,))
  im.to_db(streamer)

