#!/usr/bin/env python

import logging, os, redis, sys

# CONSTANTS
REDIS_HOST = 'localhost'
log = logging.getLogger('tripled.ls')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) < 2:
        print '%s <path>' % (sys.argv[0])
        exit(-1)

    redis = redis.Redis(host=REDIS_HOST, port=6379, db=0)
    search_string = os.path.join(sys.argv[1], '*')
    log.info('searching: %s', search_string)
    print '\n'.join(redis.keys(search_string))
