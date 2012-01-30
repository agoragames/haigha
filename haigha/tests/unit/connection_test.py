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
      'reply_code'    : 0,
      'reply_text'    : 'first connect',
      'class_id'      : 0,
      'method_id'     : 0
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
    self.connection._transport = mock()

  def test_init_without_keyword_args(self):
    conn = Connection.__new__( Connection )
    strategy = mock()
    mock( connection, 'ConnectionChannel' )

    expect(connection.ConnectionChannel).args( conn, 0 ).returns( 'connection_channel' )
    expect(conn.connect).args( 'localhost', 5672 )

    conn.__init__()
    
    self.assertFalse( conn._debug )
    self.assertEqual( logging.root, conn._logger )
    self.assertEqual( 'guest', conn._user )
    self.assertEqual( 'guest', conn._password )
    self.assertEqual( 'localhost', conn._host )
    self.assertEqual( 5672, conn._port )
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
    self.assertEqual( [], conn._output_frame_buffer )
    self.assertTrue( isinstance(conn._transport, connection.EventTransport) )

  def test_properties(self):
    self.assertEqual( self.connection._logger, self.connection.logger )
    self.assertEqual( self.connection._debug, self.connection.debug )
    self.assertEqual( self.connection._frame_max, self.connection.frame_max )
    self.assertEqual( self.connection._channel_max, self.connection.channel_max )
    self.assertEqual( self.connection._frames_read, self.connection.frames_read )
    self.assertEqual( self.connection._frames_written, self.connection.frames_written )

  def test_connect(self):
    self.connection._connected = 'maybe'
    self.connection._closed = 'possibly'
    self.connection._debug = 'sure'
    self.connection._connect_timeout = 42
    self.connection._sock_opts = {
      ('f1','t1') : 5,
      ('f2','t2') : 6
    }

    expect( self.connection._transport.connect ).args( ('host',5672) )
    expect( self.connection._transport.write ).args( 'AMQP\x00\x00\x09\x01' )

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

  def test_disconnect_when_transport_disconnects(self):
    self.connection._connected = 'yup'

    expect( self.connection._transport.disconnect )
    self.connection.disconnect()

    assert_false( self.connection._connected )
    assert_equals( None, self.connection._transport )

  def test_disconnect_when_transport_disconnects_with_error(self):
    self.connection._connected = 'yup'
    self.connection._host = 'server'

    expect( self.connection._transport.disconnect ).raises( Exception('fail') )
    expect( self.connection.logger.error ).args( "Failed to disconnect from %s", 'server', exc_info=True )
    self.connection.disconnect()

    assert_false( self.connection._connected )
    assert_equals( None, self.connection._transport )

  def test_transport_closed_with_no_args(self):
    self.connection._host = 'server'
    self.connection._connected = 'yes'

    expect( self.connection.logger.warning ).args( 'transport to server closed : unknown cause' ) 
    expect( self.connection._callback_close )

    self.connection.transport_closed()

    assert_equals( 0, self.connection._close_info['reply_code'] )
    assert_equals( 'transport to server closed : unknown cause', self.connection._close_info['reply_text'] )
    assert_equals( 0, self.connection._close_info['class_id'] )
    assert_equals( 0, self.connection._close_info['method_id'] )

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
    expect( ch.add_close_listener ).args( self.connection._channel_closed )
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
    expect( ch.add_close_listener ).args( self.connection._channel_closed )
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

  def test_channel_closed(self):
    ch = mock()
    ch.channel_id = 42
    self.connection._channels[42] = ch

    self.connection._channel_closed(ch)
    assert_false( 42 in self.connection._channels )

    ch.channel_id = 500424834
    self.connection._channel_closed(ch)

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

  def test_read_frames_when_no_transport(self):
    self.connection._transport = None
    self.connection.read_frames()
    assert_equals( 0, self.connection._frames_read )

  def test_read_frames_when_transport_returns_no_data(self):
    expect( self.connection._transport.read ).returns( None )
    self.connection.read_frames()
    assert_equals( 0, self.connection._frames_read )

  def test_read_frames_when_transport_when_frame_data_and_no_debug_and_no_buffer(self):
    reader = mock()
    frame = mock()
    frame.channel_id = 42
    channel = mock()
    mock( connection, 'Reader' )
    
    expect( self.connection._transport.read ).returns( 'data' )
    expect( connection.Reader ).args( 'data' ).returns( reader )
    expect( connection.Frame.read_frames ).args( reader ).returns( [frame] )
    expect( self.connection.channel ).args( 42 ).returns( channel )
    expect( channel.buffer_frame ).args( frame )
    expect( self.connection._transport.process_channels ).args( set([channel]) )
    expect( reader.tell ).returns( 4 )

    self.connection.read_frames()
    assert_equals( 1, self.connection._frames_read )

  def test_read_frames_when_transport_when_frame_data_and_debug_and_buffer(self):
    reader = mock()
    frame = mock()
    frame.channel_id = 42
    channel = mock()
    mock( connection, 'Reader' )
    self.connection._debug = 2
    
    expect( self.connection._transport.read ).returns( 'data' )
    expect( connection.Reader ).args( 'data' ).returns( reader )
    expect( connection.Frame.read_frames ).args( reader ).returns( [frame] )
    expect( self.connection.logger.debug ).args( 'READ: %s', frame )
    expect( self.connection.channel ).args( 42 ).returns( channel )
    expect( channel.buffer_frame ).args( frame )
    expect( self.connection._transport.process_channels ).args( set([channel]) )
    expect( reader.tell ).times(2).returns( 2 )
    expect( self.connection._transport.buffer ).args( 'ta' )

    self.connection.read_frames()
    assert_equals( 1, self.connection._frames_read )

  def test_flush_buffered_frames(self):
    self.connection._output_frame_buffer = ['frame1', 'frame2']
    expect( self.connection.send_frame ).args( 'frame1' )
    expect( self.connection.send_frame ).args( 'frame2' )

    self.connection._flush_buffered_frames()
    assert_equals( [], self.connection._output_frame_buffer )

  def test_send_frame_when_connected_and_transport_and_no_debug(self):
    frame = mock()
    expect( frame.write_frame ).args( var('ba') )
    expect( self.connection._transport.write ).args( var('ba') )
    
    self.connection._connected = True
    self.connection.send_frame( frame )
    assert_true( isinstance(var('ba').value, bytearray) )
    assert_equals( 1, self.connection._frames_written )

  def test_send_frame_when_not_connected_and_not_channel_0(self):
    frame = mock()
    frame.channel_id = 42
    stub( frame.write_frame )
    stub( self.connection._transport.write )
    
    self.connection._connected = False
    self.connection.send_frame( frame )
    assert_equals( [frame], self.connection._output_frame_buffer )

  def test_send_frame_when_not_connected_and_channel_0(self):
    frame = mock()
    frame.channel_id = 0
    expect( frame.write_frame ).args( var('ba') )
    expect( self.connection._transport.write ).args( var('ba') )
    
    self.connection._connected = False
    self.connection.send_frame( frame )
    assert_true( isinstance(var('ba').value, bytearray) )
    assert_equals( 1, self.connection._frames_written )

  def test_send_frame_when_debugging(self):
    frame = mock()
    expect( self.connection.logger.debug ).args( 'WRITE: %s', frame )
    expect( frame.write_frame ).args( var('ba') )
    expect( self.connection._transport.write ).args( var('ba') )
    
    self.connection._connected = True
    self.connection._debug = 2
    self.connection.send_frame( frame )
    assert_true( isinstance(var('ba').value, bytearray) )
    assert_equals( 1, self.connection._frames_written )

  def test_send_frame_when_closed(self):
    self.connection._closed = True
    self.connection._close_info['reply_text'] = 'failed'
    assert_raises( connection.ConnectionClosed,
      self.connection.send_frame, 'frame' )
    
    self.connection._close_info['reply_text'] = ''
    assert_raises( connection.ConnectionClosed,
      self.connection.send_frame, 'frame' )
    
    self.connection._close_info = None
    assert_raises( connection.ConnectionClosed,
      self.connection.send_frame, 'frame' )
