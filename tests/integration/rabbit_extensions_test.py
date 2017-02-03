'''
Copyright (c) 2011-2017, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

#
# Integration tests for RabbitMQ extensions
#


# Disable "no member" pylint error since Haigha's channel class members get
# added at runtime.
#
# e.g., "E1103: Instance of 'Channel' has no 'exchange' member (but some types
# could not be inferred)"
#
# pylint: disable=E1103


# Disable pylint warning regarding protected member access
#
# pylint: disable=W0212


# Disable pylint notification about missing method docstring
#
# pylint: disable=C0111



import logging
import socket
import unittest

from haigha.connections.rabbit_connection import RabbitConnection
from haigha.message import Message


class TestOptions(object): # pylint: disable=R0903
    '''Configuration settings'''
    user = 'guest'
    password = 'guest'
    vhost = '/'
    host = 'localhost'
    debug = False


_OPTIONS = TestOptions()
_LOG = None


def setUpModule(): # pylint: disable=C0103
    '''Unittest fixture for module-level initialization'''

    global _LOG # pylint: disable=W0603


    # Setup logging
    log_level = logging.DEBUG if _OPTIONS.debug else logging.INFO
    logging.basicConfig(level=log_level,
                        format="[%(levelname)s %(asctime)s] %(message)s")
    _LOG = logging.getLogger('haigha')


class _CallbackSink(object):
    '''Callback sink; an instance of this class may be passed as a callback
    and it will store the callback args in the values instance attribute
    '''

    __slots__ = ('values',)

    def __init__(self):
        self.values = None
        self.reset()

    def reset(self):
        '''Reset the args buffer'''
        self.values = []

    def __repr__(self):
        return "%s(ready=%s, values=%.255r)" % (self.__class__.__name__,
            self.ready,
            self.values)

    def __call__(self, *args):
        self.values.append(args)

    @property
    def ready(self):
        '''True if called; False if not called'''
        return bool(self.values)


class _PubackState(_CallbackSink):
    '''An instance of this class acts as a context manager that registers for
    basic.ack/nack notificagtions and records ACK or NACK callback
    '''

    __slots__ = ('_channel')

    ACK = 1
    NACK = 2

    def __init__(self, channel):
        self._channel = channel
        super(_PubackState, self).__init__()

    def __enter__(self):
        self._channel.basic.set_ack_listener(self.handle_ack)
        self._channel.basic.set_nack_listener(self.handle_nack)
        return self

    def __exit__(self, *_args):
        self._channel.basic.set_ack_listener(None)
        self._channel.basic.set_nack_listener(None)
        self.reset()
        self._channel = None

    def handle_ack(self, delivery_tag):
        '''Message Ack'ed in RabbitMQ Publisher Acknowledgments mode'''
        _LOG.debug("Message ACKed: tag=%s", delivery_tag)

        assert not self.ready, (delivery_tag, self.values)

        self(self.ACK, delivery_tag)

    def handle_nack(self, delivery_tag):
        '''Message Nack'ed in RabbitMQ Publisher Acknowledgments mode'''
        _LOG.error("Message NACKed: tag=%s", delivery_tag)

        assert not self.ready, (delivery_tag, self.values)

        self(self.NACK, delivery_tag)


class RabbitExtensionsTests(unittest.TestCase):
    '''Integration tests for RabbitMQ-specific extensions'''

    def _connect_to_broker(self):
        ''' Connect to broker and regisiter cleanup action to disconnect

        :returns: connection instance
        :rtype: `haigha.connections.rabbit_connection.Connection`
        '''
        sock_opts = {
            (socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1,
        }
        connection = RabbitConnection(
            logger=_LOG,
            debug=_OPTIONS.debug,
            user=_OPTIONS.user,
            password=_OPTIONS.password,
            vhost=_OPTIONS.vhost,
            host=_OPTIONS.host,
            heartbeat=None,
            sock_opts=sock_opts,
            transport='socket')
        self.addCleanup(lambda: connection.close(disconnect=True)
                        if not connection.closed else None)

        return connection

    def test_puback_and_internal_exchange(self):
        # NOTE: this test method was created from the old rabbit_extensions
        # script contents and enhanced with ack validation and waiting for get
        # to complete. It needs some more work, including splitting into
        # multiple tests: sock opts, exchange-to-exchange binding, and
        # publisher-acks.
        connection = self._connect_to_broker()

        ch = connection.channel()
        self.addCleanup(ch.close)

        _LOG.info('Declaring exchange "foo"')
        ch.exchange.declare('foo', 'direct', auto_delete=True)
        self.addCleanup(ch.exchange.delete, 'foo')

        _LOG.info('Declaring internal exchange "fooint"')
        ch.exchange.declare('fooint', 'direct', internal=True, auto_delete=True,
                            arguments={})
        self.addCleanup(ch.exchange.delete, 'fooint')

        _LOG.info('Binding "fooint" to "foo" on route "route"')
        ch.exchange.bind('fooint', 'foo', 'route')

        _LOG.info(
            'Binding queue "bar" to exchange "fooint" on route "route"')
        ch.queue.declare('bar', auto_delete=True)
        self.addCleanup(ch.queue.delete, 'bar')
        ch.queue.bind('bar', 'fooint', 'route')

        _LOG.info('Enabling publisher confirmations')
        ch.confirm.select()

        _LOG.info('Publishing to exchange "foo" on route "route"')

        with _PubackState(ch) as puback_state:
            mid = ch.basic.publish(Message('hello world'), 'foo', 'route')
            _LOG.info('Published message mid %s', mid)
            while not puback_state.ready:
                connection.read_frames()

            ((how, response_tag),) = puback_state.values
            self.assertEqual(how, puback_state.ACK)
            self.assertEqual(response_tag, mid)

        consumer = _CallbackSink()
        msg = ch.basic.get('bar', consumer=consumer)
        _LOG.info('GET %s', msg)
        # Wait for the rest of the message body to come in, not just the
        # Basic.Get-ok frame
        while not consumer.ready:
            connection.read_frames()
        ((msg,),) = consumer.values
        _LOG.info('GOT %s', msg)
        self.assertEqual(msg.body, 'hello world')


        _LOG.info(
            'Publishing to exchange "foo" on route "nullroute" mandatory=False')
        with _PubackState(ch) as puback_state:
            mid = ch.basic.publish(Message('hello world'), 'foo', 'nullroute')
            _LOG.info('Published message mid %s to foo/nullroute', mid)
            while not puback_state.ready:
                connection.read_frames()

            ((how, response_tag),) = puback_state.values
            self.assertEqual(how, puback_state.ACK)
            self.assertEqual(response_tag, mid)

    def test_unroutable_message_is_returned_with_puback(self):
        connection = self._connect_to_broker()

        ch = connection.channel()
        self.addCleanup(ch.close)

        _LOG.info('Declaring exchange "foo"')
        ch.exchange.declare('foo', 'direct')
        self.addCleanup(ch.exchange.delete, 'foo')

        callback_sink = _CallbackSink()
        ch.basic.set_return_listener(callback_sink)

        _LOG.info('Enabling publisher confirmations')
        ch.confirm.select()

        _LOG.info(
            'Publishing to exchange "foo" on route "nullroute" mandatory=True')
        with _PubackState(ch) as puback_state:
            mid = ch.basic.publish(Message('hello world'), 'foo', 'nullroute',
                                   mandatory=True)
            _LOG.info('Published message mid %s to foo/nullroute', mid)

            # Wait for ack
            while not puback_state.ready:
                connection.read_frames()

            ((how, response_tag),) = puback_state.values
            self.assertEqual(how, puback_state.ACK)
            self.assertEqual(response_tag, mid)


        # Verify that unroutable message was returned

        self.assertEqual(len(callback_sink.values), 1)

        ((msg,),) = callback_sink.values
        self.assertEqual(msg.body, 'hello world')

        self.assertIsNone(msg.delivery_info)
        self.assertIsNotNone(msg.return_info)

        return_info = msg.return_info
        self.assertItemsEqual(
            ['channel', 'reply_code', 'reply_text', 'exchange', 'routing_key'],
            return_info.keys())
        self.assertIs(return_info['channel'], ch)
        self.assertEqual(return_info['reply_code'], 312)
        self.assertEqual(return_info['reply_text'], 'NO_ROUTE')
        self.assertEqual(return_info['exchange'], 'foo')
        self.assertEqual(return_info['routing_key'], 'nullroute')

    def test_basic_cancel_from_broker(self):
        connection = self._connect_to_broker()

        ch = connection.channel()
        self.addCleanup(ch.close)

        queue, _, _ = ch.queue.declare('', auto_delete=True)

        # Start consumer
        cancel_cb = _CallbackSink()
        consumer = _CallbackSink()
        consumer_tag = ch.basic._generate_consumer_tag()
        ch.basic.consume(queue, consumer=consumer, consumer_tag=consumer_tag,
                         cancel_cb=cancel_cb)

        # Delete the queue to force consumer cancellation
        ch.queue.delete(queue)

        # Wait for Basic.Cancel from server
        while not cancel_cb.ready:
            connection.read_frames()

        ((rx_consumer_tag,),) = cancel_cb.values
        self.assertEqual(rx_consumer_tag, consumer_tag)


if __name__ == '__main__':
    unittest.main()
