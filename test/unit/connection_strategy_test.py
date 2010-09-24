"""
Unit tests for the connection strategy
"""

import mox
import socket
import logging
import lib as connection_strategy

from lib.connection_strategy import ConnectionStrategy, Host, UNCONNECTED, CONNECTED, FAILED

class ConnectionStrategyTest(mox.MoxTestBase):

  def setUp(self):
    mox.MoxTestBase.setUp( self )

    connection_strategy.event = self.mox.CreateMockAnything()
    self.connection = self.mox.CreateMockAnything()
    self.strategy = ConnectionStrategy( self.connection, 'localhost' )

  def test_init_is_doin_it_right(self):
    self.assertEquals( self.strategy._connection, self.connection )
    self.assertEquals( Host(socket.gethostname()), self.strategy._orig_host )
    self.assertEquals( [self.strategy._orig_host], self.strategy._known_hosts )
    self.assertEquals( self.strategy._orig_host, self.strategy._cur_host )
    self.assertFalse(self.strategy._reconnecting)
    self.assertEqual([], self.strategy.reconnect_callbacks)

  def test_init_with_reconnect_cb(self):
    strategy = ConnectionStrategy(self.connection, 'localhost', reconnect_cb = 'my_reconnect_callback')
    self.assertEqual(['my_reconnect_callback'], strategy.reconnect_callbacks)

  def test_set_known_hosts_is_single_entry(self):
    self.assertEquals( [self.strategy._orig_host], self.strategy._known_hosts )
    self.strategy.set_known_hosts( socket.gethostname() )
    self.assertEquals( [self.strategy._orig_host], self.strategy._known_hosts )
  
  def test_set_known_hosts_updates_list_correctly(self):
    self.assertEquals( [self.strategy._orig_host], self.strategy._known_hosts )
    self.strategy.set_known_hosts( 'localhost:4200,localhost,foo:1234' )

    self.assertEquals( [Host('localhost'),Host('localhost:4200'),Host('foo:1234')], self.strategy._known_hosts )

  def test_set_known_hosts_handles_misconfigured_cluster(self):
    self.strategy._cur_host = Host('bar')
    self.strategy._orig_host = Host('foo:5678')
    self.strategy._known_hosts = [ self.strategy._orig_host ]

    self.mox.StubOutWithMock( self.connection, 'log' )
    self.connection.log( "current host bar:5672 not in known hosts [foo:5678, foo:1234], reconnecting to foo:5678 in 5s!", logging.WARNING )
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect( 5 )

    self.mox.ReplayAll()
    self.strategy.set_known_hosts( 'foo:1234' )

  def test_next_host_handles_simple_base_case(self):
    self.strategy._cur_host = Host('localhost')
    self.strategy._known_hosts = [Host('localhost'), Host('foo')]
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect()

    self.mox.ReplayAll()
    self.strategy.next_host()
    self.assertEquals( Host('foo'), self.strategy._cur_host )
    self.assertFalse(self.strategy._reconnecting)

  def test_next_host_finds_first_unconnected_host(self):
    self.strategy._cur_host = Host('localhost')
    self.strategy._known_hosts = [Host('localhost'), Host('foo'), Host('bar')]
    self.strategy._known_hosts[0].state = CONNECTED
    self.strategy._known_hosts[1].state = CONNECTED
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect()

    self.mox.ReplayAll()
    self.strategy.next_host()
    self.assertEquals( Host('bar'), self.strategy._cur_host )
    self.assertFalse(self.strategy._reconnecting)

  def test_next_host_searches_for_unfailed_hosts_if_all_hosts_not_unconnected(self):
    self.strategy._cur_host = Host('foo')
    self.strategy._known_hosts = [Host('localhost'), Host('foo'), Host('bar'), Host('cat')]
    self.strategy._known_hosts[0].state = CONNECTED
    self.strategy._known_hosts[1].state = CONNECTED
    self.strategy._known_hosts[2].state = CONNECTED
    self.strategy._known_hosts[3].state = CONNECTED
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect()

    self.mox.ReplayAll()
    self.strategy.next_host()
    self.assertEquals( Host('localhost'), self.strategy._cur_host )
  
  def test_next_host_searches_for_unfailed_hosts_even_if_orig_host_is_failed(self):
    self.strategy._cur_host = Host('foo')
    self.strategy._known_hosts = [Host('localhost'), Host('foo'), Host('bar'), Host('cat')]
    self.strategy._known_hosts[0].state = FAILED
    self.strategy._known_hosts[1].state = CONNECTED
    self.strategy._known_hosts[2].state = CONNECTED
    self.strategy._known_hosts[3].state = CONNECTED
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect()

    self.mox.ReplayAll()
    self.strategy.next_host()
    self.assertEquals( Host('foo'), self.strategy._cur_host )

  def test_next_host_defaults_to_original_with_delay_if_all_hosts_failed(self):
    self.strategy._orig_host = Host('foo')
    self.strategy._cur_host = Host('bar')
    self.strategy._known_hosts = [Host('foo'), Host('bar'), Host('cat'), Host('dog')]
    self.strategy._known_hosts[0].state = FAILED
    self.strategy._known_hosts[1].state = FAILED
    self.strategy._known_hosts[2].state = FAILED
    self.strategy._known_hosts[3].state = FAILED

    self.mox.StubOutWithMock( self.connection, 'log' )
    self.connection.log( 'Failed to connect to any of [foo:5672, bar:5672, cat:5672, dog:5672], will retry foo:5672 in 5 seconds', logging.WARNING )
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect( 5 )

    self.mox.ReplayAll()
    self.strategy.next_host()
    self.assertEquals( Host('foo'), self.strategy._cur_host )
    self.assertTrue(self.strategy._reconnecting)

  def test_fail_is_not_stoopehd(self):
    self.strategy._cur_host = Host('foo')
    self.assertEquals( UNCONNECTED, self.strategy._cur_host.state )
    
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.mox.StubOutWithMock( self.strategy, 'next_host' )
    
    self.strategy.fail()
    self.mox.ReplayAll()
    self.assertEquals( FAILED, self.strategy._cur_host.state )

# FIXME: These tests need to be fixed
#  def test_connect_basics(self):
#    self.strategy._pending_connect = None

#    self.mox.StubOutWithMock( self.connection, 'disconnect' )
#    self.connection.disconnect()

#    connection_strategy.event.timeout(0, self.strategy._connect_cb).AndReturn('foo')

#    self.mox.ReplayAll()
#    self.strategy.connect()
#    self.assertTrue( self.strategy._pending_connect, 'foo' )

#  def test_connect_handles_disconnect_errors(self):
#    self.strategy._pending_connect = None

#    self.mox.StubOutWithMock( self.connection, 'disconnect' )
#    self.connection.disconnect().AndRaise( Exception("can'na do it cap'n") )
#    
#    self.mox.StubOutWithMock( self.connection, 'log' )
#    self.connection.log( 'error while disconnecting', logging.ERROR )

#    connection_strategy.event.timeout(0, self.strategy._connect_cb)

#    self.mox.ReplayAll()
#    self.strategy.connect()

#  def test_connect_honors_delay(self):
#    self.strategy._pending_connect = None

#    self.mox.StubOutWithMock( self.connection, 'disconnect' )
#    self.connection.disconnect()
#    
#    connection_strategy.event.timeout(42, self.strategy._connect_cb).AndReturn('foo')

#    self.mox.ReplayAll()
#    self.strategy.connect( 42 )
#    print self.strategy._pending_connect
#    self.assertTrue( self.strategy._pending_connect, 'foo' )
  
  def test_connect_had_single_pending_event(self):
    self.strategy._pending_connect = 'foo'

    self.mox.StubOutWithMock( self.connection, 'disconnect' )
    self.connection.disconnect()

    self.mox.ReplayAll()
    self.strategy.connect()
    self.assertTrue( self.strategy._pending_connect, 'foo' )

  def test_connect_cb_when_successful_and_not_reconnecting(self):
    self.strategy._pending_connect = 'foo'
    self.strategy._cur_host = Host('bar')
    self.strategy._reconnecting = False

    self.mox.StubOutWithMock( self.connection, 'connect' )
    self.connection.connect( 'bar', 5672 )
    
    self.mox.StubOutWithMock( self.connection, 'log' )
    self.connection.log( 'Connected to bar:5672', logging.DEBUG )
    
    self.mox.ReplayAll()
    self.strategy._connect_cb()
    self.assertTrue( self.strategy._pending_connect is None )
    self.assertEquals( CONNECTED, self.strategy._cur_host.state )

  def test_connect_cb_when_successful_and_reconnecting(self):
    mock = self.create_mock_anything()
    self.strategy._pending_connect = 'foo'
    self.strategy._cur_host = Host('bar')
    self.strategy._reconnecting = True
    self.strategy.reconnect_callbacks = [lambda: mock.test_call()]

    self.mox.StubOutWithMock( self.connection, 'connect' )
    self.connection.connect( 'bar', 5672 )
    
    self.mox.StubOutWithMock( self.connection, 'log' )
    self.connection.log( 'Connected to bar:5672', logging.INFO )
    
    mock.test_call()
    self.mox.ReplayAll()
    self.strategy._connect_cb()
    self.assertTrue( self.strategy._pending_connect is None )
    self.assertEquals( CONNECTED, self.strategy._cur_host.state )
    self.assertFalse(self.strategy._reconnecting)

  def test_connect_cb_on_fail_and_first_connect_attempt(self):
    self.strategy._cur_host = Host('bar')
    
    self.mox.StubOutWithMock( self.connection, 'connect' )
    self.connection.connect( 'bar', 5672 ).AndRaise( socket.error('fail sauce') )

    self.connection.log( \
      "Failed to connect to bar:5672, will try again in 2 seconds", logging.ERROR )
    self.mox.StubOutWithMock( self.strategy, 'connect' )
    self.strategy.connect( 2 )
    
    self.mox.ReplayAll()
    self.strategy._connect_cb()
    self.assertEquals( FAILED, self.strategy._cur_host.state )
  
  def test_connect_cb_on_fail_and_second_connect_attempt(self):
    self.strategy._cur_host = Host('bar')
    self.strategy._cur_host.state = FAILED
    
    self.mox.StubOutWithMock( self.connection, 'connect' )
    self.connection.connect( 'bar', 5672 ).AndRaise( socket.error('fail sauce') )

    self.connection.log( \
      "Failed to connect to bar:5672", logging.CRITICAL )
    self.mox.StubOutWithMock( self.strategy, 'next_host' )
    self.strategy.next_host()
    
    self.mox.ReplayAll()
    self.strategy._connect_cb()


class HostTest(mox.MoxTestBase):
  
  def setUp(self):
    mox.MoxTestBase.setUp( self )

  def test_init_localhost(self):
    h1 = Host( 'localhost' )
    h2 = Host( '127.0.0.1' )

    self.assertEquals( socket.gethostname(), h1.host )
    self.assertEquals( socket.gethostname(), h2.host )

  def test_init_parses_ports(self):
    h1 = Host( 'localhost' )
    h2 = Host( 'localhost:4200' )

    self.assertEquals( 5672, h1.port )
    self.assertEquals( 4200, h2.port )

  def test_init_handles_unicode(self):
    self.assertEquals( Host(u'localhost'), Host('localhost') )
