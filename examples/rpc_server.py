#!/usr/bin/env python
'''
demostrate how to write a rpc server
'''
import sys
import os
sys.path.append(os.path.abspath(".."))

from haigha.connection import Connection
from haigha.message import Message

connection = Connection(host='localhost', heartbeat=None, debug=True)
channel = connection.channel()
channel.queue.declare(queue='rpc_queue', auto_delete=False)


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


def on_request(msg):
    n = int(msg.body)

    print " [.] fib(%s)" % (n,)
    result = fib(n)

    reply_to = msg.properties["reply_to"]
    correlation_id = msg.properties["correlation_id"]
    resp = Message(str(result), correlation_id=correlation_id)
    channel.basic.publish(resp, '', reply_to)

    delivery_info = msg.delivery_info
    channel.basic.ack(delivery_info["delivery_tag"])

channel.basic.qos(prefetch_count=1)
channel.basic.consume('rpc_queue', on_request, no_ack=False)

print " [x] Awaiting RPC requests"
while not channel.closed:
    connection.read_frames()
