#!/usr/bin/env python
'''
demostrate how to write a rpc client
'''
import sys, os, uuid, time
sys.path.append(os.path.abspath(".."))

from haigha.connection import Connection
from haigha.message import Message

class FibonacciRpcClient(object):
    def __init__(self):
        self.connection = Connection(host='localhost', heartbeat=None, debug=True)

        self.channel = self.connection.channel()

        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result[0]
        print("callback_queue:", self.callback_queue)

        self.channel.basic.consume(self.callback_queue, self.on_response, no_ack=True)

    def on_response(self, msg):
        if msg.properties["correlation_id"] == self.corr_id:
             self.response = msg.body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        msg = Message(str(n), reply_to=self.callback_queue, correlation_id=self.corr_id)
        self.channel.basic.publish(msg, '', 'rpc_queue')
        while self.response is None:
            self.connection.read_frames()
        return int(self.response)

fibonacci_rpc = FibonacciRpcClient()

print " [x] Requesting fib(30)"
response = fibonacci_rpc.call(30)
print " [.] Got %r" % (response,)

