'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import gevent
from gevent.coros import Semaphore
from gevent import socket
from gevent.pool import Pool

from haigha.transports import *

class GeventTransportTest(Chai):

  def setUp(self):
    super(GeventTransportTest,self).setUp()

    self.connection = mock()
    self.transport = GeventTransport(self.connection)
    self.transport._host = 'server:1234'

  def test_init(self):
    assert_equals( bytearray(), self.transport._buffer )
    assert_true( isinstance(self.transport._read_lock,Semaphore) )
    assert_true( isinstance(self.transport._write_lock,Semaphore) )

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
    expect( sock.settimeout ).args( None )

    self.transport.connect( ('host',5309) )

  def test_read(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()
    self.transport.connection.debug = False

    expect( self.transport._read_lock.acquire )
    expect( self.transport._sock.getsockopt ).args(
      socket.SOL_SOCKET, socket.SO_RCVBUF ).returns( 4095 )
    expect( self.transport._sock.recv ).args(4095).returns('buffereddata')
    expect( self.transport._read_lock.release )

    assert_equals( 'buffereddata', self.transport.read() )

  def test_read_when_data_buffered(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()
    self.transport.connection.debug = False
    self.transport._buffer = bytearray('buffered')

    expect( self.transport._read_lock.acquire )
    expect( self.transport._sock.getsockopt ).any_args().returns( 4095 )
    expect( self.transport._sock.recv ).args(4095).returns('data')
    expect( self.transport._read_lock.release )

    assert_equals( 'buffereddata', self.transport.read() )
    assert_equals( bytearray(), self.transport._buffer )

  def test_read_when_debugging(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()
    self.transport.connection.debug = 2

    expect( self.transport._read_lock.acquire )
    expect( self.transport._sock.getsockopt ).any_args().returns( 4095 )
    expect( self.transport._sock.recv ).args(4095).returns('buffereddata')
    expect( self.transport.connection.logger.debug ).args(
      'read 12 bytes from server:1234' )
    expect( self.transport._read_lock.release )

    assert_equals( 'buffereddata', self.transport.read() )

  def test_read_when_socket_closes(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()
    self.transport.connection.debug = 2
    
    expect( self.transport._read_lock.acquire )
    expect( self.transport._sock.getsockopt ).any_args().returns( 4095 )
    expect( self.transport._sock.recv ).args(4095).returns('')
    expect( self.transport._read_lock.release )
    expect( self.transport.connection.transport_closed ).args(
      msg='error reading from server:1234' )
    
    self.transport.read()

  def test_read_when_no_sock(self):
    self.transport.read()

  def test_buffer(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()
    
    expect( self.transport._read_lock.acquire )
    expect( self.transport._read_lock.release )

    self.transport.buffer( bytearray('somedata') )
    assert_equals( bytearray('somedata'), self.transport._buffer )

  def test_buffer_when_already_buffered(self):
    self.transport._sock = mock()
    self.transport._read_lock = mock()
    self.transport._buffer = bytearray('some')
    
    expect( self.transport._read_lock.acquire )
    expect( self.transport._read_lock.release )

    self.transport.buffer( bytearray('data') )
    assert_equals( bytearray('somedata'), self.transport._buffer )

  def test_buffer_when_no_sock(self):
    self.transport.buffer('somedata')

  def test_write(self):
    self.transport._sock = mock()
    self.transport._write_lock = mock()
    self.transport.connection.debug = False

    expect( self.transport._write_lock.acquire )
    expect( self.transport._sock.sendall ).args( 'somedata' )
    expect( self.transport._write_lock.release )
    self.transport.write( 'somedata' )

  def test_write_when_sendall_fails(self):
    self.transport._sock = mock()
    self.transport._write_lock = mock()
    self.transport.connection.debug = False

    expect( self.transport._write_lock.acquire )
    expect( self.transport._sock.sendall ).args( 'somedata' ).raises(Exception('fail'))
    expect( self.transport._write_lock.release )
    assert_raises(Exception, self.transport.write, 'somedata' )

  def test_write_when_debugging(self):
    self.transport._sock = mock()
    self.transport.connection.debug = 2

    expect( self.transport._sock.sendall ).args( 'somedata' )
    expect( self.transport.connection.logger.debug ).args(
      'sent 8 bytes to server:1234' )

    self.transport.write( 'somedata' )

  def test_write_when_no_sock(self):
    self.transport.write('somedata')

  def test_disconnect(self):
    self.transport._sock = mock()
    expect( self.transport._sock.close )
    self.transport.disconnect()
    assert_equals( None, self.transport._sock )
  
  def test_disconnect_when_no_sock(self):
    self.transport.disconnect()

class GeventPoolTransportTest(Chai):

  def setUp(self):
    super(GeventPoolTransportTest,self).setUp()

    self.connection = mock()
    self.transport = GeventPoolTransport(self.connection)
    self.transport._host = 'server:1234'

  def test_init(self):
    assert_equals( bytearray(), self.transport._buffer )
    assert_true( isinstance(self.transport._read_lock,Semaphore) )
    assert_true( isinstance(self.transport.pool, Pool) )

    trans = GeventPoolTransport(self.connection, pool='inground')
    assert_equals( 'inground', trans.pool )

  def test_process_channels(self):
    chs = [ mock(), mock() ]
    self.transport._pool = mock()

    expect( self.transport._pool.spawn ).args( chs[0].process_frames )
    expect( self.transport._pool.spawn ).args( chs[1].process_frames )

    self.transport.process_channels( chs )
