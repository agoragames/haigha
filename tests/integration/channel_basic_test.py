'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

#
# Integration tests for Channel.basic
#


# Disable "no member" pylint error since Haigha's channel class members get
# added at runtime.
#
# e.g., "E1103: Instance of 'Channel' has no 'exchange' member (but some types
# could not be inferred)"
#
# pylint: disable=E1103


import logging
import socket
import unittest


from haigha.channel import Channel
from haigha.connection import Connection
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


class ChannelBasicTests(unittest.TestCase):
  '''Integration tests for Channel.basic'''

  def _connect_to_broker(self):
    ''' Connect to broker and regisiter cleanup action to disconnect

    :returns: connection instance
    :rtype: `haigha.connection.Connection`
    '''
    sock_opts = {
      (socket.IPPROTO_TCP, socket.TCP_NODELAY) : 1,
    }
    connection = Connection(
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

  def test_unroutable_message_is_returned(self):
    connection = self._connect_to_broker()

    ch = connection.channel()
    self.addCleanup(ch.close)

    _LOG.info('Declaring exchange "foo"')
    ch.exchange.declare('foo', 'direct')
    self.addCleanup(ch.exchange.delete, 'foo')

    callback_sink = _CallbackSink()
    ch.basic.set_return_listener(callback_sink)

    _LOG.info(
      'Publishing to exchange "foo" on route "nullroute" mandatory=True')
    mid = ch.basic.publish(Message('hello world'), 'foo', 'nullroute',
                           mandatory=True)
    _LOG.info('Published message mid %s to foo/nullroute', mid)

    # Wait for return of unroutable message
    while not callback_sink.ready:
      connection.read_frames()

    # Validate returned message
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
