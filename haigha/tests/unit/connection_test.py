'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

import logging
from chai import Chai

from haigha import connection, VERSION
from haigha.connection import Connection

class ConnectionTest(Chai):
  
  def setUp(self):
    super(ConnectionTest,self).setUp()

    self.connection = Connection.__new__( Connection )
    self.connection._debug = False
    self.connection._logger = self.mock()
    self.connection._user = 'guest'
    self.connection._password = 'guest'
    self.connection._host = 'localhost'
    self.connection._vhost = '/'
    self.connection._connect_timeout = 5
    self.connection._sock_opts = None
    self.connection._sock = None # mock anything?
    self.connection._heartbeat = None
    self.connection._reconnect_cb = self.mock()
    self.connection._close_cb = self.mock()
    self.connection._login_method = 'AMQPLAIN'
    self.connection._locale = 'en_US'
    self.connection._client_properties = None
    self.connection._properties = {
      'library': 'Haigha',
      'library_version': 'x.y.z',
    }
    self.connection._closed = False
    self.connection._connected = False
    self.connection._close_info = {
      'reply_code'    : -1,
      'reply_text'    : 'first connect',
      'class_id'      : -1,
      'method_id'     : -1
    }
    self.connection._channels = {
      0 : self.mock()
    }
    self.connection._login_response = 'loginresponse'
    self.connection._channel_counter = 0
    self.connection._channel_max = 65535
    self.connection._frame_max = 65535
    self.connection._frames_read = 0
    self.connection._frames_written = 0
    self.connection._strategy = self.mock()
    self.connection._output_frame_buffer = []

  def test_init_without_keyword_args(self):
    conn = Connection.__new__( Connection )
    strategy = mock()
    mock( connection, 'ConnectionChannel' )
    mock( connection, 'ConnectionStrategy' )

    expect(connection.ConnectionChannel).args( conn, 0 ).returns( 'connection_channel' )
    expect(connection.ConnectionStrategy).args( conn, 'localhost', reconnect_cb=None ).returns( strategy )
    expect(strategy.connect)

    conn.__init__()
    
    self.assertFalse( conn._debug )
    self.assertEqual( logging.root, conn._logger )
    self.assertEqual( 'guest', conn._user )
    self.assertEqual( 'guest', conn._password )
    self.assertEqual( 'localhost', conn._host )
    self.assertEqual( '/', conn._vhost )
    self.assertEqual( 5, conn._connect_timeout )
    self.assertEqual( None, conn._sock_opts )
    self.assertEqual( None, conn._sock )
    self.assertEqual( None, conn._heartbeat )
    self.assertEqual( None, conn._reconnect_cb )
    self.assertEqual( None, conn._close_cb )
    self.assertEqual( 'AMQPLAIN', conn._login_method )
    self.assertEqual( 'en_US', conn._locale )
    self.assertEqual( None, conn._client_properties )
    self.assertEqual( conn._properties, {
      'library': 'Haigha',
      'library_version': VERSION,
    } )
    self.assertFalse( conn._closed )
    self.assertFalse( conn._connected )
    self.assertEqual( conn._close_info, {
      'reply_code'    : 0,
      'reply_text'    : 'first connect',
      'class_id'      : 0,
      'method_id'     : 0
    } )
    self.assertEqual( {0:'connection_channel'}, conn._channels )
    self.assertEqual( '\x05LOGINS\x00\x00\x00\x05guest\x08PASSWORDS\x00\x00\x00\x05guest', conn._login_response )
    self.assertEqual( 0, conn._channel_counter )
    self.assertEqual( 65535, conn._channel_max )
    self.assertEqual( 65535, conn._frame_max )
    self.assertEqual( strategy, conn._strategy )
    self.assertEqual( [], conn._output_frame_buffer )

  def test_properties(self):
    self.assertEqual( self.connection._logger, self.connection.logger )
    self.assertEqual( self.connection._debug, self.connection.debug )
    self.assertEqual( self.connection._frame_max, self.connection.frame_max )
    self.assertEqual( self.connection._channel_max, self.connection.channel_max )
    self.assertEqual( self.connection._frames_read, self.connection.frames_read )
    self.assertEqual( self.connection._frames_written, self.connection.frames_written )

  def test_reconnect(self):
    expect( self.connection._strategy.connect )
    self.connection.reconnect()

  def test_connect(self):
    self.connection._connected = 'maybe'
    self.connection._closed = 'possibly'
    self.connection._debug = 'sure'
    self.connection._connect_timeout = 42
    self.connection._sock_opts = {
      ('f1','t1') : 5,
      ('f2','t2') : 6
    }

    sock = mock()
    mock( connection, 'EventSocket' )
    expect( connection.EventSocket ).args( 
      read_cb = self.connection._sock_read_cb,
      close_cb = self.connection._sock_close_cb,
      error_cb = self.connection._sock_error_cb,
      debug = 'sure',
      logger = self.connection._logger ).returns( sock )
    expect( sock.settimeout ).args( 42 )
    expect( sock.setsockopt ).args( 'f1', 't1', 5 ).any_order()
    expect( sock.setsockopt ).args( 'f2', 't2', 6 ).any_order()
    expect( sock.connect ).args( ('host',5672) )
    expect( sock.setblocking ).args( False )
    expect( sock.write ).args( 'AMQP\x00\x00\x09\x01' )

    self.connection.connect( 'host', 5672 )
    assert_false( self.connection._connected )
    assert_false( self.connection._closed )
    assert_equals( self.connection._close_info,
      {
      'reply_code'    : 0,
      'reply_text'    : 'failed to connect to host:5672',
      'class_id'      : 0,
      'method_id'     : 0
      } )
    assert_equals( 'host:5672', self.connection._host )

  def test_disconnect(self):
    sock = self.connection._sock = mock()
    self.connection._connected = 'yup'
    self.connection._sock.close_cb = 'something'
    self.connection._channels = { 0 : 'a', 1 : 'b', 2 : 'c' }

    expect( self.connection._sock.close )
    self.connection.disconnect()

    assert_false( self.connection._connected )
    assert_equals( None, sock.close_cb )
    assert_equals( None, self.connection._sock )
    assert_equals( { 0 : 'a', 1 : 'b', 2 : 'c' }, self.connection._channels )

  def test_add_reconnect_callback(self):
    # have to mock the list because strategy is a mock object and can't
    # mock builtin append()
    self.connection._strategy.reconnect_callbacks = mock()
    expect( self.connection._strategy.reconnect_callbacks.append ).args( 'foo' )
    self.connection.add_reconnect_callback( 'foo' )

  def test_sock_read_cb(self):
    expect( self.connection._read_frames )
    self.connection._sock_read_cb('sock')

  def test_sock_read_cb_logs_when_read_frame_exception(self):
    self.connection._host = 'hostess'
    expect( self.connection._read_frames ).raises( Exception('fail') )
    expect( self.connection._logger.error ).args( 
      'Failed to read frames from %s', 'hostess', exc_info=True )
    expect( self.connection.close ).args( 
      reply_code=501, reply_text='Error parsing frames' )
    self.connection._sock_read_cb('sock')

  def test_sock_close_cb_when_no_user_close_cb(self):
    self.connection._host = 'hostess'
    self.connection._close_cb = None
    self.connection._connected = 'yep'

    expect( self.connection._logger.warning ).args(
      'socket to %s closed unexpectedly', 'hostess' )
    expect( self.connection._callback_close )
    expect( self.connection._strategy.fail )

    self.connection._sock_close_cb('sock')
    assert_false( self.connection._connected )
    assert_equals( self.connection._close_info,
      {
      'reply_code'    : 0,
      'reply_text'    : 'socket closed unexpectedly to hostess',
      'class_id'      : 0,
      'method_id'     : 0
      } )

  def test_sock_error_cb_when_no_user_close_cb(self):
    self.connection._host = 'hostess'
    self.connection._close_cb = None
    self.connection._connected = 'yep'

    expect( self.connection._logger.error ).args(
      'error on connection to %s: %s', 'hostess', 'errormsg' )
    expect( self.connection._callback_close )
    expect( self.connection._strategy.fail )
    expect( self.connection._strategy.next_host )

    self.connection._sock_error_cb('sock', 'errormsg', 'exception')
    assert_false( self.connection._connected )
    assert_equals( self.connection._close_info,
      {
      'reply_code'    : 0,
      'reply_text'    : 'socket error on host hostess: errormsg',
      'class_id'      : 0,
      'method_id'     : 0
      } )

  def test_next_channel_id_when_less_than_max(self):
    self.connection._channel_counter = 32
    self.connection._channel_max = 23423
    assert_equals( 33, self.connection._next_channel_id() )
  
  def test_next_channel_id_when_at_max(self):
    self.connection._channel_counter = 32
    self.connection._channel_max = 32
    assert_equals( 1, self.connection._next_channel_id() )

  def test_channel_creates_new_when_not_at_limit(self):
    ch = mock()
    expect( self.connection._next_channel_id ).returns( 1 )
    mock( connection, 'Channel' )
    expect( connection.Channel ).args( self.connection, 1).returns( ch )
    expect( ch.open )

    self.assert_equals( ch, self.connection.channel() )
    self.assert_equals( ch, self.connection._channels[1] )

  def test_channel_finds_the_first_free_channel_id(self):
    self.connection._channels[1] = 'foo'
    self.connection._channels[2] = 'bar'
    self.connection._channels[4] = 'cat'
    ch = mock()
    expect( self.connection._next_channel_id ).returns( 1 )
    expect( self.connection._next_channel_id ).returns( 2 )
    expect( self.connection._next_channel_id ).returns( 3 )
    mock( connection, 'Channel' )
    expect( connection.Channel ).args( self.connection, 3 ).returns( ch )
    expect( ch.open )

    self.assert_equals( ch, self.connection.channel() )
    self.assert_equals( ch, self.connection._channels[3] )

  def test_channel_raises_toomanychannels(self):
    self.connection._channels[1] = 'foo'
    self.connection._channels[2] = 'bar'
    self.connection._channels[4] = 'cat'
    self.connection._channel_max = 3
    assert_raises( Connection.TooManyChannels, self.connection.channel )

  def test_channel_returns_cached_instance_if_known(self):
    self.connection._channels[1] = 'foo'
    assert_equals( 'foo', self.connection.channel(1) )

  def test_channel_raises_invalidchannel_if_unknown_id(self):
    assert_raises( Connection.InvalidChannel, self.connection.channel, 42 )

  def test_close(self):
    self.connection._channels[0] = mock()
    expect( self.connection._channels[0].close ).times(2)
    
    self.connection.close()
    assert_equals( {'reply_code':0, 'reply_text':'', 'class_id':0, 'method_id':0}, 
      self.connection._close_info )
    
    self.connection.close(1, 'foo', 2, 3)
    assert_equals( {'reply_code':1, 'reply_text':'foo', 'class_id':2, 'method_id':3}, 
      self.connection._close_info )

  def test_callback_close_when_no_cb(self):
    self.connection._close_cb = None
    self.connection._callback_close()

  def test_callback_close_when_user_cb(self):
    self.connection._close_cb = mock()
    expect( self.connection._close_cb )
    self.connection._callback_close()

  def test_callback_close_raises_sysexit_when_user_cb_does(self):
    self.connection._close_cb = mock()
    expect( self.connection._close_cb ).raises( SystemExit() )
    assert_raises( SystemExit, self.connection._callback_close )

  def test_callback_close_logs_when_user_cb_fails(self):
    self.connection._close_cb = mock()
    expect( self.connection._close_cb ).raises( 'fail!' )
    expect( self.connection.logger.error ).args( str )
    self.connection._callback_close()
