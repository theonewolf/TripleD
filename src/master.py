#!/usr/bin/env python

import fileinput, hashlib, logging, os, sys, redis, zmq

# CONSTANTS
log = logging.getLogger('tripled.master')
REDIS_SERVER = 'localhost'
CHUNK_DIR = '/tmp/tripled_chunks/'

class master:
    def __init__(self):
        self.redis = redis.Redis(host=REDIS_SERVER, port=6379, db=0)
        self.workers = []
        self.count = 0
        self.written_blocks = 0

    def add_worker(self, worker):
        self.workers.append(worker)
        self.count += 1

    def client_read_file(self, client, file):
        blocks = self.redis.lrange(file, 0, -1)
        client.send_pyobj(blocks, protocol=0)

    def client_write(self, client, file, block):
        worker = self.workers[self.written_blocks % self.count]
        directory = os.path.join(CHUNK_DIR, hashlib.sha256(file).hexdigest())
        path = os.path.join(directory, str(block))
        log.debug('writing to worker[%s] path[%s]'% (worker, path))
        self.written_blocks += 1
        serialized = (worker, path)
        self.redis.rpush(file, serialized)
        client.send_pyobj(serialized, protocol=0)

    def parse_client_command(self, client, command):
        log.debug('command: %s', command)
        if command[0] == 'read':
            self.client_read_file(client, command[1])
        elif command[0] == 'write':
            self.client_write(client, command[1], command[2])
        else:
            log.error('Error parsing client command.  Failing.')
            exit(-1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) < 2:
        print '%s <stdin | [<worker.cfg> [ worker2.cfg ...]]>' % (sys.argv[0])

    workers = []
    for line in fileinput.input():
        workers.append(line.strip())
        log.debug('new worker: %s', workers[-1])
        
    master = master()
    for worker in workers:
        master.add_worker(worker)
     
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:1337')

    while True:
        master.parse_client_command(socket, socket.recv())
