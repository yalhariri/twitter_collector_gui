# -*- coding: utf-8 -*-
"""
Created on Thu Jan  9 16:32:57 2020

"""

import os
import sys
import subprocess
import argparse
# from solr_util import *

#to run the following code you need to call the following command
#python util_run.py -c .config/keys -cmd search -tf .config/search_list -sc  tweets1 -sp 8983
#python util_run.py -c .config/keys -cmd stream -sc  tweets1 -sp 8983

#python util_run.py -cmd create_core -sc tweets1 -spath /disk/data/phd/Youssef/Tools/solr-8.2.0/bin/solr
#python util_run.py -cmd delete_core -sc tweets1 -spath /disk/data/phd/Youssef/Tools/solr-8.2.0/bin/solr

def run_crawler( solr_url, keys_path='./crawler/.config/keys',command = "search",terms_file='./crawler/.config/search_list',wait_time=10):
    if command=='search':
        pid=subprocess.Popen('python ./crawler/twitter_tracker.py -c '+ str(keys_path) + ' -tf '+str(terms_file) + ' -cmd ' + str(command) + ' -surl ' + str(solr_url) + ' -wait ' + str(wait_time) + ' ', shell=True)
    else:
        pid=subprocess.Popen('python ./crawler/twitter_streamer.py -c '+ str(keys_path) + ' -cmd ' + str(command) + ' -surl ' + str(solr_url) + ' -wait ' + str(wait_time) + ' ', shell=True)
    if pid:
        with open('./crawler/.cache/process','w') as f_out:
            f_out.write(str(pid.pid))
        return True
    return False

def stop_crawler():
    import signal
    with open('./crawler/.cache/process','r') as f_in:
        for line in f_in:
            os.kill(int(line.strip()),signal.SIGSTOP)

