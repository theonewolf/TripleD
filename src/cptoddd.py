#!/usr/bin/env python

import libtripled, logging, sys, os

# CONSTANTS
log = logging.getLogger('tripled.cptoddd')
CHUNK_SIZE = 64*1024**2

def next_chunk(f):
    data = f.read(CHUNK_SIZE)
    while (data):
        yield data
        data = f.read(CHUNK_SIZE)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    if len(sys.argv) < 4:
        print '%s <master> <local src> <tripled dst>' % (sys.argv[0])
        exit(-1)

    tripled = libtripled.tripled(sys.argv[1])
    with open(sys.argv[2], 'r') as f:
        for i, chunk in enumerate(next_chunk(f)):
                tripled.write_block(sys.argv[3], i, chunk)
