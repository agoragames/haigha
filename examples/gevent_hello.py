#!/usr/bin/env python

""" Demonstrates publishing and receiving a message via Haigha library using
gevent-based transport.

Assumes AMQP broker (e.g., RabbitMQ) is running on same machine (localhost)
and is configured with default parameters:
  user: guest
  password: guest
  port: 5672
  vhost: '/'
"""
import sys
import os
sys.path.append(os.path.abspath("."))
sys.path.append(os.path.abspath(".."))

import logging

import gevent
import gevent.event as gevent_event

from haigha.connection import Connection as haigha_Connection
from haigha.message import Message


class HaighaGeventHello(object):

    def __init__(self, done_cb):
        self._done_cb = done_cb

        # Connect to AMQP broker with default connection and authentication
        # settings (assumes broker is on localhost)
        self._conn = haigha_Connection(transport='gevent',
                                       close_cb=self._connection_closed_cb,
                                       logger=logging.getLogger())

        # Start message pump
        self._message_pump_greenlet = \
            gevent.spawn(self._message_pump_greenthread)

        # Create message channel
        self._channel = self._conn.channel()
        self._channel.add_close_listener(self._channel_closed_cb)

        # Create and configure message exchange and queue
        self._channel.exchange.declare('test_exchange', 'direct')
        self._channel.queue.declare('test_queue', auto_delete=True)
        self._channel.queue.bind('test_queue',
                                 'test_exchange',
                                 'test_routing_key')
        self._channel.basic.consume(queue='test_queue',
                                    consumer=self._handle_incoming_messages)

        # Publish a message on the channel
        msg = Message('body', application_headers={'hello': 'world'})
        print "Publising message: %s" % (msg,)
        self._channel.basic.publish(msg, 'test_exchange', 'test_routing_key')
        return

    def _message_pump_greenthread(self):
        print "Entering Message Pump"
        try:
            while self._conn is not None:
                # Pump
                self._conn.read_frames()

                # Yield to other greenlets so they don't starve
                gevent.sleep()
        finally:
            print "Leaving Message Pump"
            self._done_cb()
        return

    def _handle_incoming_messages(self, msg):
        print
        print "Received message: %s" % (msg,)
        print

        # Initiate graceful closing of the channel
        self._channel.basic.cancel(consumer=self._handle_incoming_messages)
        self._channel.close()
        return

    def _channel_closed_cb(self, ch):
        print "AMQP channel closed; close-info: %s" % (
            self._channel.close_info,)
        self._channel = None

        # Initiate graceful closing of the AMQP broker connection
        self._conn.close()
        return

    def _connection_closed_cb(self):
        print "AMQP broker connection closed; close-info: %s" % (
            self._conn.close_info,)
        self._conn = None
        return


def main():
    waiter = gevent_event.AsyncResult()

    HaighaGeventHello(waiter.set)

    print "Waiting for I/O to complete..."
    waiter.wait()

    print "Done!"
    return


if __name__ == '__main__':
    logging.basicConfig()
    main()
