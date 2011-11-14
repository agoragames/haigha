'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.transports import *

class EventTransportTest(Chai):

  def setUp(self):
    super(EventTransportTest,self).setUp()

    self.connection = mock()
    self.transport = EventTransport(self.connection)
    self.transport._host = 'server'

  def test_sock_close_cb(self):
    expect( self.connection.transport_closed ).args( 
      msg='socket to server closed unexpectedly' )
    self.transport._sock_close_cb('sock')

  def test_sock_error_cb(self):
    expect( self.connection.transport_closed ).args( 
      msg='error on connection to server: amsg' )
    self.transport._sock_error_cb('sock', 'amsg')

  def test_sock_read_cb(self):
    expect( self.connection.read_frames )
    self.transport._sock_read_cb('sock')

  def test_connect(self):
    sock = mock()
    mock( event_transport, 'EventSocket' )
    self.connection._connect_timeout = 4.12
    self.connection._sock_opts = {
      ('family','tcp') : 34,
      ('range','ipv6') : 'hex'
    }

    expect( event_transport.EventSocket ).args( 
      read_cb = self.transport._sock_read_cb,
      close_cb = self.transport._sock_close_cb,
      error_cb = self.transport._sock_error_cb,
      debug = self.connection.debug,
      logger = self.connection.logger,
    ).returns( sock )
    expect( sock.setsockopt ).args( 'family', 'tcp', 34 ).any_order()
    expect( sock.setsockopt ).args( 'range', 'ipv6', 'hex' ).any_order()
    expect( sock.setblocking ).args( False )
    expect( sock.connect ).args( ('host',5309), timeout=4.12 )

    self.transport.connect( ('host',5309) )

  def test_read(self):
    self.transport._sock = mock()
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
