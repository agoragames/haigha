import mox
import logging

from haigha import connection
from haigha.connection import Connection

class ConnectionTestCase(mox.MoxTestBase):
  
  def setUp(self):
    mox.MoxTestBase.setUp( self )

    self.connection = Connection.__new__( Connection )
    self.connection._debug = False
    self.connection._logger = self.create_mock_anything()
    self.connection._user = 'guest'
    self.connection._password = 'guest'
    self.connection._host = 'localhost'
    self.connection._vhost = '/'
    self.connection._connect_timeout = 5
    self.connection._sock_opts = None
    self.connection._sock = None # mock anything?
    self.connection._heartbeat = None
    self.connection._reconnect_cb = self.create_mock_anything()
    self.connection._close_cb = self.create_mock_anything()
    self.connection._login_method = 'AMQPLAIN'
    self.connection._locale = 'en_US'
    self.connection._client_properties = None
    self.connection._properties = {
      'library': 'Haigha',
      'library_version': 'x.y.z',
    }
    self.connection._closed = False
    self.connection._connected = False
    self.connection._output_buffer = []
    self.connection._close_info = {
      'reply_code'    : -1,
      'reply_text'    : 'first connect',
      'class_id'      : -1,
      'method_id'     : -1
    }
    self.connection._channels = {
      0 : self.create_mock_anything()
    }
    self.connection._login_response = 'loginresponse'
    self.connection._channel_counter = 0
    self.connection._channel_max = 65535
    self.connection._frame_max = 65535
    self.connection._strategy = self.create_mock_anything()
    self.connection._input_frame_buffer = []
    self.connection._output_frame_buffer = []


  def test_init_without_keyword_args(self):
    conn = Connection.__new__( Connection )
    strategy = self.create_mock_anything()
    self.mock( connection, 'ConnectionChannel', use_mock_anything=True )
    self.mock( connection, 'ConnectionStrategy', use_mock_anything=True )

    connection.ConnectionChannel( conn, 0 ).AndReturn( 'connection_channel' )
    connection.ConnectionStrategy( conn, 'localhost', reconnect_cb=None ).AndReturn( strategy )
    strategy.connect()

    self.replay_all()
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
      'library_version': '0.0.1',
    } )
    self.assertFalse( conn._closed )
    self.assertFalse( conn._connected )
    self.assertEqual( [], conn._output_buffer )
    self.assertEqual( conn._close_info, {
      'reply_code'    : -1,
      'reply_text'    : 'first connect',
      'class_id'      : -1,
      'method_id'     : -1
    } )
    self.assertEqual( {0:'connection_channel'}, conn._channels )
    self.assertEqual( '\x05LOGINS\x00\x00\x00\x05guest\x08PASSWORDS\x00\x00\x00\x05guest', conn._login_response )
    self.assertEqual( 0, conn._channel_counter )
    self.assertEqual( 65535, conn._channel_max )
    self.assertEqual( 65535, conn._frame_max )
    self.assertEqual( strategy, conn._strategy )
    self.assertEqual( [], conn._input_frame_buffer )
    self.assertEqual( [], conn._output_frame_buffer )
