#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import print_function

from haigha.connection import Connection
from haigha.message import Message

import gevent
from gevent.lock import Semaphore

connection = Connection(
    user='guest', password='guest',
    vhost='/', host='localhost',
    heartbeat=None, debug=False,
    transport='gevent')

running = True
frames_read = 0
msg = None


def frame_loop():
    global frames_read
    while running:
        frames_read += connection.read_frames()


def basic_get():
    ch = connection.channel()
    ch.exchange.declare('test_exchange', 'direct')
    ch.queue.declare('test_queue', auto_delete=True)
    ch.queue.bind('test_queue', 'test_exchange', 'test_key')
    ch.basic.publish(Message('textofbody',
        application_headers={'hello': 'world'}),
        'test_exchange', 'test_key')
    sem = Semaphore()

    def get_cb(m):
        global msg
        msg = m
        sem.release()

    sem.acquire()
    ch.basic.get('test_queue', consumer=get_cb)
    sem.wait()

frame_greenlet = gevent.spawn(frame_loop)
get_greenlet = gevent.spawn(basic_get)
get_greenlet.join(5)
connection.close()

# asserts that the gevent frame loop was responsible for reading frames, and
# it wasn't synchronous behavior in the protocol classes.
assert(frames_read > 0)

assert(isinstance(msg, Message))
assert(msg['delivery_info']['exchange'] == 'test_exchange')
assert(msg['delivery_info']['routing_key'] == 'test_key')
assert(msg['properties'] ==
    {'application_headers': {'hello': 'world'}})
assert(msg['body'] == 'textofbody')
assert(msg['return_info'] is None)
