'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import gevent
from gevent.coros import Semaphore
from gevent import socket

from haigha.transports import *

class GeventTransportTest(Chai):

  def setUp(self):
    super(GeventTransportTest,self).setUp()

    self.connection = mock()
    self.transport = GeventTransport(self.connection)
    self.transport._host = 'server'

  def test_init(self):
    assert_equals( bytearray(), self.transport._buffer )
    assert_true( isinstance(self.transport._read_lock,Semaphore) )

  def test_connect(self):
    sock = mock()
    mock( gevent_transport, 'socket' )
    expect( gevent_transport.socket.socket ).returns( sock )
    self.connection._connect_timeout = 4.12
    self.connection._sock_opts = {
      ('family','tcp') : 34,
      ('range','ipv6') : 'hex'
    }

    expect( sock.setblocking ).args( True )
    expect( sock.settimeout ).args( 4.12 )
    expect( sock.setsockopt ).any_order().args( 'family', 'tcp', 34 ).any_order()
    expect( sock.setsockopt ).any_order().args( 'range', 'ipv6', 'hex' ).any_order()
    expect( sock.connect ).args( ('host',5309) )

    self.transport.connect( ('host',5309) )

  '''
  def test_read(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()

    expect( self.transport._sock.read ).returns('buffereddata')
    assert_equals( 'buffereddata', self.transport.read() )

  def test_read_when_no_sock(self):
    self.transport.read()

  def test_buffer(self):
    self.transport._sock = mock()
    expect( self.transport._sock.buffer ).args( 'somedata' )
    self.transport.buffer( 'somedata' )

  def test_buffer_when_no_sock(self):
    self.transport.buffer('somedata')

  def test_write(self):
    self.transport._sock = mock()
    expect( self.transport._sock.write ).args( 'somedata' )
    self.transport.write( 'somedata' )

  def test_write_when_no_sock(self):
    self.transport.write('somedata')

  def test_disconnect(self):
    self.transport._sock = mock()
    self.transport._sock.close_cb = 'cb'
    expect( self.transport._sock.close )
    self.transport.disconnect()
    assert_equals( None, self.transport._sock.close_cb )
  
  def test_disconnect_when_no_sock(self):
    self.transport.disconnect()
  '''
