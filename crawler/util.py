#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Sat Jan 11 00:00:00 2020

"""

import json
import time
import re
import nltk
import requests
tokenizer = nltk.TweetTokenizer()
import subprocess
import urllib.parse
from urllib.parse import urlparse
import os

def update_log(file_name, updates):
    print(updates,'\n')
    with open(file_name, 'a+',encoding='utf-8') as logging:
        logging.write("%s%s"%(updates,'\n'))

def dump_dict_to_file(output_file, data_dict, mode = 'w',encoding='utf-8'):
    with open(output_file,mode,encoding='utf-8') as fo:
        json.dump(data_dict,fp=fo, ensure_ascii=False)

def load_api_keys(keys="keys"):
    import pandas as pd
    df = pd.DataFrame()
    api_keys = {"api_keys":dict()}
    import pandas as pd
    try:
        df = pd.read_csv(keys,sep=',')
        if not df.empty:
            for item in df.iterrows():
                api_keys["api_keys"][item[0]] = dict(item[1])
            # print(api_keys)
    except Exception:
        print('error while loading API keys... please configure the API keys by using the configuration tool')
        pass
    
    return api_keys


def get_search_dict(terms_file, since_id=1, geocode = None):
    '''
    a function to create the input dict for searching terms.
    The terms will be split into lists of 15 tokens to make the search more efficient.
    It might write the dict to json file.
    
    Parameters
    ----------
    terms_file : the source file that contains all the tokens to search for.
    output_file : str the json file path that will store all the user names with related data.
    since_id : int the id of the first tweet to consider in the search (default is 1).
    
    '''
    terms_list = []
    with open (terms_file, 'r') as f_in:
        for line in f_in.readlines():
            terms_list.append(line)
    import math
    x = math.ceil(len(terms_list) / 15)
    data_dict = {}
    for i in range(0,x):
        data_dict['search'+str(i)] = dict()
        data_dict['search'+str(i)]["geocode"] = geocode
        data_dict['search'+str(i)]["since_id"] = since_id
        data_dict['search'+str(i)]["terms"] = []
        for j in range(0,15):
            if j+15*i < len(terms_list):
                data_dict['search'+str(i)]["terms"].append(terms_list[j+15*i])
    return data_dict
    
def get_tweet_contents(tweet, isRetweed= False, retweeter = []):
    tweet_n_obj = dict()
    tweet_n_obj['id'] = tweet['id']
    tweet_n_obj['created_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(tweet["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
    tweet_n_obj['user_screen_name'] = tweet["user"]["screen_name"]
    tweet_n_obj['user_name'] = tweet["user"]["name"]
    tweet_n_obj['user_id'] = tweet["user"]["id"]
    tweet_n_obj['users_followers_count'] = tweet["user"]["followers_count"]
    tweet_n_obj['users_friends_count'] = tweet["user"]["friends_count"]
    if not tweet["user"]["description"]:
        tweet_n_obj['users_description'] = ''
    else:
        tweet_n_obj['users_description'] = re.sub("[\n]+"," ",re.sub("[\r\n]+"," ",tweet["user"]["description"]))
    tweet_n_obj['users_location'] = tweet["user"]["location"]
    tweet_n_obj['retweet_count'] = tweet["retweet_count"]
    tweet_n_obj['favorite_count'] = tweet["favorite_count"]
    full_text = ""
    tweet_n_obj['retweeters'] = []
    if 'full_text' in tweet.keys():
        full_text = tweet['full_text']
    elif 'text' in tweet.keys():
        full_text = tweet['text']
    if isRetweed:
        tweet_n_obj['retweeters'] = retweeter

    tweet_n_obj['full_text'] = re.sub("[\n]+"," ",re.sub("[\r\n]+"," ",full_text.strip()))
    tweet_n_obj['hashtags'] = [x for x in tokenizer.tokenize(full_text) if x.startswith('#')]
    tweet_n_obj['mentions'] = [x for x in tokenizer.tokenize(full_text) if x.startswith('@')]
    urls = []
    
    try:
        urls = [x["expanded_url"] for x in tweet["entities"]["urls"]]
    except:
        pass
    tweet_n_obj['urls'] = urls
    
    domains = []
    for url_ in urls:
        domain = urlparse(url_).netloc
        try:
            session = requests.Session()
            new_link = session.head(url_, allow_redirects=True, timeout=30)
            domain = urlparse(new_link.url).netloc
        except (Exception) as e:
            domain = urlparse(url_).netloc
        domains.append(domain.replace('www.',''))
    tweet_n_obj['domain'] = list(set(domains))
    
    return tweet_n_obj


def run_crawler( solr_url, keys_path='./crawler/.config/keys',command = "search",terms_file='./crawler/.config/search_list',wait_time=10):
    if command=='search':
        pid=subprocess.Popen('python ./crawler/twitter_collector.py -c '+ str(keys_path) + ' -tf '+str(terms_file) + ' -cmd ' + str(command) + ' -surl ' + str(solr_url) + ' -wait ' + str(wait_time) + ' &', shell=True)
    else:
        pid=subprocess.Popen('python ./crawler/twitter_collector.py -c '+ str(keys_path) + ' -cmd ' + str(command) + ' -surl ' + str(solr_url) + ' -wait ' + str(wait_time) + ' &', shell=True)
    if pid:
        with open('./crawler/.cache/process','w') as f_out:
            f_out.write(str(pid.pid))
        return True
    return False

def stop_crawler():
    os.system("kill `ps aux | grep twitter_collector.py | awk '{ print $2 }' | sort -n` ")
    # os.system("kill `ps aux | grep twitter_collector.py | awk '{ print $2 }'` ")
    # import signal
    # with open('./crawler/.cache/process','r') as f_in:
    #     for line in f_in:
    #         os.kill(int(line.strip()),signal.SIGSTOP)


# def run_crawler(keys, terms_file, solr, command = "stream",wait_time=10):
#     if command == 'search':
#         pid=subprocess.Popen('python twitter_tracker.py -c '+ str(keys) + ' -tf '+str(terms_file) + ' -cmd ' + str(command) + ' -surl ' + str(solr_url) + ' -wait ' + str(wait_time) + ' ', shell=True)
#     elif command == 'stream':
#         pid=subprocess.Popen('python twitter_streamer.py -c '+ str(keys) + ' -cmd ' + str(command) + ' -surl ' + str(solr_url) + ' -wait ' + str(wait_time) + ' ', shell=True)
#     if pid:    
#         with open('process','w') as f_out:
#             f_out.write(str(pid.pid))
#         return True
#     return False

# def stop_crawler():
#     with open('process','r') as f_in:
#         for line in f_in:
#             os.kill(int(line.trim()))

def write_to_solr(tweet, solr):
    tweets_dict = dict()
    retweeted = False
    retweeter = []        
    if 'retweeted_status' in tweet.keys():
        tweet_obj = tweet["retweeted_status"]
        retweeted = True
        retweeter = [(tweet["user"]["screen_name"]).replace('@','')]
        if 'quoted_status' in tweet_obj.keys():
            tweet_obj = tweet_obj["quoted_status"]
            tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj,False)
    else:
        tweet_obj = tweet
    
    tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj,retweeted,retweeter)
    if 'quoted_status' in tweet.keys():
        tweet_obj = tweet["quoted_status"]
        tweets_dict[tweet_obj['id']] = get_tweet_contents(tweet_obj, False)
    
    
    for k in tweets_dict.keys():
        tmp=solr.search('id:'+str(k))
        for t in tmp:
            if 'retweeters' in t.keys():
                if tweets_dict[k]['retweeters'] in t['retweeters']:
                    tweets_dict[k]['retweeters'] = t['retweeters']
                else:
                    tweets_dict[k]['retweeters'] += t['retweeters']
        tweets_dict[k]['retweeters'] = list(set(tweets_dict[k]['retweeters']))
        solr.add([tweets_dict[k]])
    solr.commit()


def init_schema(url):
    scehma_contents = [{"name":"id","type":"string","stored":True},
             {"name":"created_at","type":"pdates","stored":True},
             {"name":"user_screen_name","type":"string","stored":True},
             {"name":"user_name","type":"string","stored":True},
             {"name":"user_id","type":"string","stored":True},
             {"name":"users_followers_count","type":"pint","stored":True},
             {"name":"users_friends_count","type":"pint","stored":True},
             {"name":"retweet_count","type":"pint","stored":True},
             {"name":"favorite_count","type":"pint","stored":True},
             {"name":"full_text","type":"string","stored":True},
             {"name":"hashtags","type":"string","stored":True,"multiValued":True},
             {"name":"mentions","type":"string","stored":True,"multiValued":True},
             {"name":"retweeters","type":"string","stored":True,"multiValued":True},
             {"name":"users_description","type":"string","stored":True},
             {"name":"users_location","type":"string","stored":True}]
    headers = {'Content-type': 'application/json'}
    
    for item in scehma_contents:
        payload = {"add-field":item}
        r = requests.post(url, json=payload)        
    return (r.status_code == 200)

# def add_core(core_name,url,path):
#     r1 = os.system(path+"/bin/solr create -c " + core_name)
#     r2 = init_schema(url)
#     return (r1 == 0 and r2.status_code == 200)

# def delete_core(core_name,url,path):
#     r = os.system(path+"/bin/solr delete -c " + core_name)
#     return (r == 0)

def add_core(core_name,port,path):
    import os    
    r1 = os.system(path+"/bin/solr create -c " + core_name)
    url = "http://localhost:"+port+"/solr/"+core_name+"/schema"
    r2 = init_schema(url)
    # print(r1 == 0 and r2.status_code == 200)
    return (r1 == 0 and r2)

def delete_core(core_name,path):
    import os
    r = os.system(path+"/bin/solr delete -c " + core_name)
    return(r == 0)
    
def start_solr(port,path):
    import os    
    r1 = os.system(path+"/bin/solr start -p " + port)

    return (r1 == 0)