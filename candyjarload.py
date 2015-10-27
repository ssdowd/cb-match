#! env python

import os, sys
from sets import Set
import random
import argparse

from couchbase.bucket import Bucket
from couchbase.exceptions import NotFoundError
#import re

prefix='candyJar::'
#dir='output'
count=1

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-n', '--count', type=int, help='number of iterations', required=True)
parser.add_argument('-u', '--url', help='couchbase url  couchbase://host:port/bucket', required=True)
parser.add_argument('-f', '--file', help='file with key list', required=True)

args = parser.parse_args()

count=args.count    
docsfound=0
docsnotfound=0

def readKey():
    global docsfound, docsnotfound
    key=random.sample(keys,1)[0]
    #print key
    try:
        rv = bucket.get(key)
        docsfound+=1
    except NotFoundError as e:
        docsnotfound+=1
        #print "Item not found", e
    
# Plan 1:   
# Find all key files under dir, make a list of them, then hit them randomly
# (possibly with multiple threads)

#docs = Set()
#for dirpath, dirnames, filenames in os.walk(dir):
#    for filename in [f for f in filenames if f.endswith(".json")]:
#        docs.add(os.path.join(dirpath,filename))
        
#print docs

# Plan 2: read in a list of keys from a file, randomly read keys from that list from Couchbase

keys = Set()
for arg in sys.argv[1:]:
    with open(args.file,'r') as f:
        for line in f:
            keys.add(prefix+line.strip().rstrip())
            
bucket = Bucket(args.url)
while count > 0:
    readKey()
    count=count-1

print 'Found: ', docsfound
print 'Not Found: ', docsnotfound