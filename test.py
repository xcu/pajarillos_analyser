from tweet import Tweet
import json

if __name__ == '__main__':
  with open("results") as f, open("dest", "w") as f2:
    for line in f:
      deserialized = json.loads(line)
      if 'text' in deserialized:
        tweet = Tweet(deserialized)
        methods = ['get_text', 'get_user_mentions', 'get_hashtags']
        f2.write('|||'.join([getattr(tweet, m)().encode('utf-8') for m in methods]) + '\n')
        #f2.write(tweet.get_text().encode('utf-8') + '\n')
