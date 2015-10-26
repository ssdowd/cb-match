#! env python

# Invoke as <this_script> file ...

# Writes to output/candyJar::key.json

# Then invoke /opt/couchbase/bin/cbdocloader -u <userID> -p <password> -n host:port -b <bucketname> output

import json
import csv
import os, sys

suffix='.json'
depth=4
dir='output'
if not os.path.exists(dir):
    os.makedirs(dir)

for arg in sys.argv[1:]:
    with open(arg,'r') as f:
        for line in f:
            key,json=line.rstrip().split(',',1)
            key=key.strip().rstrip()

            letters=list(key)
            keychars=letters[0:depth]
            keychars.insert(0,dir)
            s=os.sep
            tdir=s.join(keychars)
            if not os.path.exists(tdir):
                os.makedirs(tdir)

            json=json.strip('\'')
            jsonfile=open(tdir+os.sep+'candyJar::'+key+suffix,'w')
            jsonfile.write('{:s}\n'.format(json))


