#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Sat Jan 11 00:00:00 2020

"""


import sys
import os
import time
import itertools
from twitter_classes import TwitterCrawler, TwitterStreamer
from util import *

WAIT_TIME = 30

        
def collect_tweets_by_search_terms(config, terms_file, solr_url):
    
    api_keys = list(config['api_keys'].values()).pop()
    
    search_configs = {}
    search_configs = get_search_dict(terms_file)
    
    for search_config_id in itertools.cycle(search_configs):
       
        search_config = search_configs[search_config_id]
        
        search_terms = [term.lower() for term in search_config['terms']]
        
        querystring = '%s'%(' OR '.join('(' + term.strip() + ')' for term in search_terms))
        
        since_id = search_config['since_id'] if 'since_id' in search_config else 0
        geocode = tuple(search_config['geocode']) if ('geocode' in search_config and search_config['geocode']) else None

        try:
            twitterCralwer = TwitterCrawler(api_keys=api_keys, solr_url = solr_url)
            since_id = twitterCralwer.search_by_query(querystring, geocode = geocode, since_id = since_id)
        except Exception as exc:
            update_log('./.log/crawler', exc)
            pass

        search_config['since_id'] = since_id
        search_config['querystring'] = querystring
        search_config['geocode'] = geocode

        search_configs[search_config_id] = search_config
        time.sleep(WAIT_TIME)

def collect_public_tweets(config, solr_url):

    api_keys = list(config['api_keys'].values()).pop()
    
    if len(api_keys) > 0:
        app_key = api_keys['app_key']
        app_secret = api_keys['app_secret']
        oauth_token = api_keys['oauth_token']
        oauth_token_secret = api_keys['oauth_token_secret']
        
        streamer = TwitterStreamer(app_key, app_secret, oauth_token, oauth_token_secret, solr_url)
        print("start collecting.....")
        streamer.statuses.sample(tweet_mode='extended')

if __name__=="__main__":
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="config.json that contains twitter api keys;", default=".config/keys")
    parser.add_argument('-cmd','--command', help="commands: search, stream", default="search")
    parser.add_argument('-tf','--terms_file', help="terms list to seach for", default="./.config/search_list")
    parser.add_argument('-surl','--solr_url', help="solr core to store the retreived tweets", default='http://localhost:8983/solr/tweets1/')
    parser.add_argument('-wait','--wait_time', help="wait time to check available api keys", type=int, default=0)

    args = parser.parse_args()
    if args.wait_time > WAIT_TIME:
        WAIT_TIME = args.wait_time
    
    if not args.command:
        sys.exit('ERROR: COMMAND is required!')
    if not args.solr_url:
        sys.exit('ERROR: Solr url is required!')
    
    if not os.path.isdir('.log'):
        os.mkdir('.log')
    # if not os.path.isdir('.config'):
    #     os.mkdir('.config')
    
    config = load_api_keys(keys=args.config)
    print(config,'...')
    if len(config)>0:
        try:
            if (args.command == 'search'):
                collect_tweets_by_search_terms(config, args.terms_file, args.solr_url)
            elif (args.command == 'stream'):
                while(True):
                    try:
                        collect_public_tweets(config, args.solr_url)
                    except Exception as exc:
                        update_log('./.log/streamer', exc)
                    time.sleep(10)
                    print("restarting...")
            else:
                raise Exception("command not found!")
        except KeyboardInterrupt:
            print('Exitting as requested...')
            pass
        except Exception as exc:
            update_log('./.log/crawler', exc)
        finally:
            pass