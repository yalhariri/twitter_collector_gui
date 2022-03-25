#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 00:00:00 2020

"""

import pysolr
import time
import twython
import copy
from urllib.parse import urlparse
from util import *

solr = ""
MAX_RETRY_CNT = 3
WAIT_TIME = 40
topic_tokens = []

class TwitterCrawler(twython.Twython):

    def __init__(self, *args, **kwargs):
        
        api_keys = copy.copy(kwargs.pop('api_keys', None))

        if not api_keys:
            raise Exception('api keys is missing')

        self.api_keys= copy.copy(api_keys)

        oauth2 = kwargs.pop('oauth2', True)

        if oauth2:
            api_keys.pop('oauth_token')
            api_keys.pop('oauth_token_secret')
            twitter = twython.Twython(api_keys['app_key'], api_keys['app_secret'], oauth_version=2)
            access_token = twitter.obtain_access_token()
            kwargs['access_token'] = access_token
            api_keys.pop('app_secret')

        kwargs.update(api_keys)
        global solr
        solr_url = kwargs.pop('solr_url', None)
        solr = pysolr.Solr(solr_url, timeout=10)
        super(TwitterCrawler, self).__init__(*args, **kwargs)

    def rate_limit_error_occured(self, resource, api):
        rate_limits = self.get_application_rate_limit_status(resources=[resource])
        
        wait_for = int(rate_limits['resources'][resource][api]['reset']) - time.time() + WAIT_TIME
        print('rate limit reached, sleep for %d'%wait_for)
        if wait_for < 0:
            wait_for = 60

        time.sleep(wait_for)

    def search_by_query(self, query, since_id = 0, geocode=None, lang=None):

        if not query or len(query) < 1:
            raise Exception("search: query terms is empty or not entred")
        
        prev_max_id = -1
        current_max_id = 0
        cnt = 0
        current_since_id = since_id

        retry_cnt = MAX_RETRY_CNT
        
        while current_max_id != prev_max_id and retry_cnt > 0:
            try:
                if current_max_id > 0:
                    tweets = self.search(q=query, geocode=geocode, since_id=since_id, lang=lang, tweet_mode='extended', max_id=current_max_id-1, result_type='recent', count=100)
                else:
                    tweets = self.search(q=query, geocode=geocode, since_id=since_id, lang=lang, tweet_mode='extended', result_type='recent', count=100)
                
                prev_max_id = current_max_id
                
                for tweet in tweets['statuses']:
                    write_to_solr(tweet, solr)

                    if current_max_id == 0 or current_max_id > int(tweet['id']):
                        current_max_id = int(tweet['id'])
                    if current_since_id == 0 or current_since_id < int(tweet['id']):
                        current_since_id = int(tweet['id'])
                
                    
                cnt += len(tweets['statuses'])
                
                time.sleep(1)

            except twython.exceptions.TwythonRateLimitError:
                self.rate_limit_error_occured('search', '/search/tweets')
            except Exception as exc:
                time.sleep(10)
                update_log('./.log/crawler', str("exception@TwitterCrawler.search_by_query(): %s"%exc))
                retry_cnt -= 1
                if (retry_cnt == 0):
                    print("exceed max retry... return")
                    return since_id

        update_log('./.log/crawler', str("[%s]; since_id: [%d]; total tweets: %d "%(query, since_id, cnt)))
        return current_since_id

class TwitterStreamer(twython.TwythonStreamer):

    def __init__(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, solr_url):
        self.counter = 0
        self.error = 0
        global solr
        solr = pysolr.Solr(solr_url, timeout=10)
        super(TwitterStreamer, self).__init__(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    
    def on_success(self, tweet):
        self.counter += 1
        if ('text' in tweet and 'id' in tweet and 'created_at' in tweet and 'user' in tweet):
            write_to_solr(tweet, solr)
        elif not "delete" in tweet:
            self.error += 1
            update_log('./.log/streamer', str('%s\n'%json.dumps(tweet,ensure_ascii=False)))
            
    def on_error(self, status_code, data):
        update_log('./.log/streamer', str('ERROR CODE: [%s]-[%s]'%(status_code, data)))
        
    def close(self):
        self.disconnect()