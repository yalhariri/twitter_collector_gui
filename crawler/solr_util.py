#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

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
    return r

def add_core(core_name,port,path):
    import os    
    r1 = os.system(path+"/bin/solr create -c " + core_name)
    url = "http://localhost:"+port+"/solr/"+core_name+"/schema"
    r2 = init_schema(url)
    print(r1 == 0 and r2.status_code == 200)
    return (r1 == 0 and r2.status_code == 200)

def delete_core(core_name,path):
    import os
    r = os.system(path+"/bin/solr delete -c " + core_name)
    return(r == 0)

def start_solr(port,path):
    import os    
    r1 = os.system(path+"/bin/solr start -p " + port)

    return (r1 == 0)
    
def load_api_keys(keys="keys"):
    import pandas as pd
    df = pd.DataFrame()
    api_keys = {"apikeys":dict()}
    import pandas as pd
    try:
        df = pd.read_csv(keys,sep=',')
        if not df.empty:
            for item in df.iterrows():
                api_keys["apikeys"][item[0]] = dict(item[1])
    except Exception:
        print('error while loading API keys... please configure the API keys by using the configuration tool')
        pass
    
    return api_keys

def get_search_dict(terms_file, since_id=1, geocode = None):
    '''
    a function to create the input json file for collecting the timelines.
    
    Parameters
    ----------
    terms_list : a list of string that contains all the tokens to search for.
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
#    with open(output_file,'w',encoding='utf-8') as fo:
#        json.dump(data_dict,fp=fo, ensure_ascii=False)

