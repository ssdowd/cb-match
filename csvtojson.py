#! env python

# Invoke as <this_script> file ...

# Writes to output/candyJar::key.json

# Then invoke /opt/couchbase/bin/cbdocloader -u <userID> -p <password> -b <bucketname> output

import json
import csv
import os
import sys

dir='output'
os.mkdir(dir)
suffix='.json'

for arg in sys.argv[1:]:
    with open(arg,'r') as f:
        for line in f:
            key,json=line.rstrip().split(',',1)
            key=key.strip().rstrip()
            json=json.strip('\'')
            jsonfile=open(dir+os.sep+'candyJar::'+key+suffix,'w')
            jsonfile.write('{:s}\n'.format(json))

        
#        letters=list(key)
#        (k1,k2)=letters[0:1]
#        print('{:s}/{%s}'.format(k1,k2))


#Administrator/Administrator

