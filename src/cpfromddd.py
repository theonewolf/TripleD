#!/usr/bin/env python

import libtripled, logging, sys, os

# CONSTANTS
log = logging.getLogger('tripled.cpfromddd')

def next_chunk(tripled, path):
    chunks = tripled.read_file(path)
    for chunk in chunks:
       log.debug('reading from worker[%s] path[%s]' % (chunk[0], chunk[1]))
       yield tripled.read_block(chunk[0], chunk[1]) 

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) < 4:
        print '%s <master> <tripled src> <local dst>' % (sys.argv[0])
        exit(-1)

    tripled = libtripled.tripled(sys.argv[1])
    try: os.makedirs(os.path.dirname(sys.argv[3]))
    except OSError: pass
    with open(sys.argv[3], 'w') as f:
        for chunk in next_chunk(tripled, sys.argv[2]):
                f.write(chunk)
