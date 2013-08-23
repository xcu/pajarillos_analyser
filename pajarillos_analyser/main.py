from pymongo import MongoClient
from db.injector import DBInjector
from streamers.file_streamer import FileStreamer
from streamers.http_streamer import HTTPStreamer
import logging
logging.basicConfig(filename='tweets.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('analyser')

if __name__ == '__main__':
  '''  streamer = HTTPStreamer(**dict(line.split() for line in open("token_data")))
  for m in streamer.messages():
    print m
  '''
  streamer = FileStreamer('spanish_results')
  for m in streamer.messages():
    print m
  '''
  injector = DBInjector(MongoClient('localhost', 27017), 'stats', 'time_chunks',
                        index='start_date')
  injector.insert_time_chunks(streamer)
  injector.dump_db_info()
  '''

