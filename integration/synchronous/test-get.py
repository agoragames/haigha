#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import print_function

from haigha.connection import Connection
from haigha.message import Message

connection = Connection(
    user='guest', password='guest',
    vhost='/', host='localhost',
    heartbeat=None, debug=False)

ch = connection.channel()
ch.exchange.declare('test_exchange', 'direct')
ch.queue.declare('test_queue', auto_delete=True)
ch.queue.bind('test_queue', 'test_exchange', 'test_key')
ch.basic.publish(Message('textofbody', application_headers={'hello': 'world'}),
    'test_exchange', 'test_key')
msg = ch.basic.get('test_queue')

ch.queue.unbind('test_queue', 'test_exchange', 'test_key')
ch.queue.delete('test_queue')
ch.exchange.delete('test_exchange')
connection.close()

assert(isinstance(msg, Message))
assert(msg['delivery_info']['exchange'] == 'test_exchange')
assert(msg['delivery_info']['routing_key'] == 'test_key')
assert(msg['properties'] ==
    {'application_headers': {'hello': 'world'}})
assert(msg['body'] == 'textofbody')
assert(msg['return_info'] is None)
