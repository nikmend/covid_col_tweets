#!/usr/bin/env python3

#
# This script will walk through all the tweet id files and
# hydrate them with twarc. The line oriented JSON files will
# be placed right next to each tweet id file.
#
# Note: you will need to install twarc, tqdm, and run twarc configure
# from the command line to tell it your Twitter API keys.
#

import gzip
import json

from tqdm import tqdm
from twarc import Twarc
from pathlib import Path
import datetime
print(datetime.datetime.now())

with open('config/cred.json') as json_file:
    cred = json.load(json_file)

twarc = Twarc(cred['CONSUMER_KEY'], cred['CONSUMER_SECRET'], cred['ACCESS_TOKEN'], cred['ACCESS_TOKEN_SECRET'])
#data_dirs = ['2020-01', '2020-02', '2020-03', '2020-04', '2020-05']
base_path = "data-ids/"
data_dirs = ['2020-05']

import threading
import queue

#Number of threads
n_thread = 5
all_ids=[]
#Create queue
queue = queue.Queue()

class ThreadClass(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
    #Assign thread working with queue
        self.queue = queue

    def run(self):
        while True:
        #Get from queue job
            tweet = self.queue.get()
            if tweet['lang']=='es':
                if "colombia" in str(tweet).lower():
                    all_ids.append(tweet['id_str'])
                            #signals to queue job is done
            self.queue.task_done()



def main():
    #Create number process
    for i in range(n_thread):
        t = ThreadClass(queue)
        t.setDaemon(True)
        #Start thread
        t.start()

    for data_dir in data_dirs:
        for path in Path(base_path + data_dir).iterdir():
            if path.name.endswith('.txt'):
               hydrate(path)
        print(datetime.datetime.now())
    


def _reader_generator(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def raw_newline_count(fname):
    """
    Counts number of lines in file
    """
    f = open(fname, 'rb')
    f_gen = _reader_generator(f.raw.read)
    return sum(buf.count(b'\n') for buf in f_gen)


def hydrate(id_file):
    print('hydrating {}'.format(id_file))

    gzip_path = id_file.with_suffix('.out')

    if gzip_path.is_file():
        print('skipping json file already exists: {}'.format(gzip_path))
        return

    num_ids = raw_newline_count(id_file)
    print('Tweets in this file: ', num_ids)
    
    with open(gzip_path, 'w') as out_file:
        with tqdm(total=num_ids) as pbar:
            for tweet in twarc.hydrate(id_file.open()):
                queue.put(tweet)
                pbar.update(1)
            queue.join()
            out_file.write("\n".join(all_ids))
    all_ids.clear()
    
            


if __name__ == "__main__":
    main()