#!/usr/bin/env python

import logging, os, pickle, zmq

log = logging.getLogger('tripled.worker')

class worker:
    def __init__(self):
        pass

    def client_read_block(self, client, path):
        log.info('worker reading block[%s]', path)
        with open(path, 'r') as f:
                client.send(pickle.dumps(f.read()))

    def client_write_block(self, client, command):
        log.info('worker writing block[%s]', command[1])
        try: os.makedirs(os.path.dirname(command[1]))
        except OSError: pass
        with open(command[1], 'w') as f:
                f.write(command[2])
        client.send(pickle.dumps(True))

    def parse_client_command(self, client, command):
        command = pickle.loads(command)
        log.debug('command: %s' % (command[0:1]))
        if command[0] == 'read':
            self.client_read_block(client, command[1])
        elif command[0] == 'write':
            self.client_write_block(client, command)
        else:
            log.error('Error parsing client command. Failing.')
            exit(-1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:8008')

    worker = worker()
    
    while True:
        worker.parse_client_command(socket, socket.recv())
