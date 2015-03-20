'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

import logging
from chai import Chai

from haigha import connection, __version__
from haigha.connection import Connection, ConnectionChannel, ConnectionError, ConnectionClosed
from haigha.channel import Channel
from haigha.frames.frame import Frame
from haigha.frames.method_frame import MethodFrame
from haigha.frames.heartbeat_frame import HeartbeatFrame
from haigha.frames.header_frame import HeaderFrame
from haigha.frames.content_frame import ContentFrame
from haigha.classes.basic_class import BasicClass
from haigha.classes.channel_class import ChannelClass
from haigha.classes.exchange_class import ExchangeClass
from haigha.classes.queue_class import QueueClass
from haigha.classes.transaction_class import TransactionClass
from haigha.classes.protocol_class import ProtocolClass

from haigha.transports import event_transport
from haigha.transports import gevent_transport
from haigha.transports import socket_transport


class ConnectionTest(Chai):

    def setUp(self):
        super(ConnectionTest, self).setUp()

        self.connection = Connection.__new__(Connection)
        self.connection._debug = False
        self.connection._logger = self.mock()
        self.connection._user = 'guest'
        self.connection._password = 'guest'
        self.connection._host = 'localhost'
        self.connection._vhost = '/'
        self.connection._connect_timeout = 5
        self.connection._sock_opts = None
        self.connection._sock = None  # mock anything?
        self.connection._heartbeat = None
        self.connection._open_cb = self.mock()
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
            'reply_code': 0,
            'reply_text': 'first connect',
            'class_id': 0,
            'method_id': 0
        }
        self.connection._class_map = {}
        self.connection._channels = {
            0: self.mock()
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
        self.connection._synchronous = False
        self.connection._synchronous_connect = False

    def test_init_without_keyword_args(self):
        conn = Connection.__new__(Connection)
        strategy = mock()
        transport = mock()
        mock(connection, 'ConnectionChannel')

        expect(connection.ConnectionChannel).args(
            conn, 0, {}).returns('connection_channel')
        expect(socket_transport.SocketTransport).args(conn).returns(transport)
        expect(conn.connect).args('localhost', 5672)

        conn.__init__()

        assert_false(conn._debug)
        assert_equal(logging.root, conn._logger)
        assert_equal('guest', conn._user)
        assert_equal('guest', conn._password)
        assert_equal('localhost', conn._host)
        assert_equal(5672, conn._port)
        assert_equal('/', conn._vhost)
        assert_equal(5, conn._connect_timeout)
        assert_equal(None, conn._sock_opts)
        assert_equal(None, conn._sock)
        assert_equal(None, conn._heartbeat)
        assert_equal(None, conn._open_cb)
        assert_equal(None, conn._close_cb)
        assert_equal('AMQPLAIN', conn._login_method)
        assert_equal('en_US', conn._locale)
        assert_equal(None, conn._client_properties)
        assert_equal(conn._properties, {
            'library': 'Haigha',
            'library_version': __version__,
        })
        assert_false(conn._closed)
        assert_false(conn._connected)
        assert_equal(conn._close_info, {
            'reply_code': 0,
            'reply_text': 'first connect',
            'class_id': 0,
            'method_id': 0
        })
        assert_equals({
            20: ChannelClass,
            40: ExchangeClass,
            50: QueueClass,
            60: BasicClass,
            90: TransactionClass
        }, conn._class_map)
        assert_equal({0: 'connection_channel'}, conn._channels)
        assert_equal(
            '\x05LOGINS\x00\x00\x00\x05guest\x08PASSWORDS\x00\x00\x00\x05guest', conn._login_response)
        assert_equal(0, conn._channel_counter)
        assert_equal(65535, conn._channel_max)
        assert_equal(65535, conn._frame_max)
        assert_equal([], conn._output_frame_buffer)
        assert_equal(transport, conn._transport)

        transport.synchronous = True
        assert_false(conn._synchronous)
        assert_true(conn.synchronous)
        assert_true(conn._synchronous_connect)

    def test_init_with_event_transport(self):
        conn = Connection.__new__(Connection)
        strategy = mock()
        transport = mock()

        mock(connection, 'ConnectionChannel')

        expect(connection.ConnectionChannel).args(
            conn, 0, {}).returns('connection_channel')
        expect(event_transport.EventTransport).args(conn).returns(transport)
        expect(conn.connect).args('localhost', 5672)

        conn.__init__(transport='event')

    def test_properties(self):
        assert_equal(self.connection._logger, self.connection.logger)
        assert_equal(self.connection._debug, self.connection.debug)
        assert_equal(self.connection._frame_max, self.connection.frame_max)
        assert_equal(self.connection._channel_max, self.connection.channel_max)
        assert_equal(self.connection._frames_read, self.connection.frames_read)
        assert_equal(
            self.connection._frames_written, self.connection.frames_written)
        assert_equal(self.connection._closed, self.connection.closed)
        # sync property tested in the test_inits

    def test_synchronous_when_no_transport(self):
        self.connection._transport = None
        with assert_raises(connection.ConnectionClosed):
            self.connection.synchronous

        self.connection._close_info = {
            'reply_code': 100,
            'reply_text': 'breakdown'
        }
        with assert_raises(connection.ConnectionClosed):
            self.connection.synchronous

    def test_synchronous_when_transport(self):
        self.connection._transport.synchronous = True
        assert_true(self.connection.synchronous)
        self.connection._transport.synchronous = False
        assert_false(self.connection.synchronous)

    def test_connect_when_asynchronous_transport(self):
        self.connection._transport.synchronous = False
        self.connection._connected = 'maybe'
        self.connection._closed = 'possibly'
        self.connection._debug = 'sure'
        self.connection._connect_timeout = 42
        self.connection._sock_opts = {
            ('f1', 't1'): 5,
            ('f2', 't2'): 6
        }

        expect(self.connection._transport.connect).args(('host', 5672))
        expect(self.connection._transport.write).args('AMQP\x00\x00\x09\x01')

        self.connection.connect('host', 5672)
        assert_false(self.connection._connected)
        assert_false(self.connection._closed)
        assert_equals(self.connection._close_info,
                      {
                          'reply_code': 0,
                          'reply_text': 'failed to connect to host:5672',
                          'class_id': 0,
                          'method_id': 0
                      })
        assert_equals('host:5672', self.connection._host)

    def test_connect_when_asynchronous_transport_but_synchronous_connect(self):
        self.connection._transport.synchronous = False
        self.connection._synchronous_connect = True
        self.connection._connected = 'maybe'
        self.connection._closed = 'possibly'
        self.connection._debug = 'sure'
        self.connection._connect_timeout = 42
        self.connection._sock_opts = {
            ('f1', 't1'): 5,
            ('f2', 't2'): 6
        }

        expect(self.connection._transport.connect).args(('host', 5672))
        expect(self.connection._transport.write).args('AMQP\x00\x00\x09\x01')
        expect(self.connection._channels[0].add_synchronous_cb).args(
            self.connection._channels[0]._recv_start)

        expect(self.connection.read_frames)
        expect(self.connection.read_frames).side_effect(
            lambda: setattr(self.connection, '_connected', True))

        self.connection.connect('host', 5672)
        assert_true(self.connection._connected)
        assert_false(self.connection._closed)
        assert_equals(self.connection._close_info,
                      {
                          'reply_code': 0,
                          'reply_text': 'failed to connect to host:5672',
                          'class_id': 0,
                          'method_id': 0
                      })
        assert_equals('host:5672', self.connection._host)

    def test_connect_when_synchronous_transport(self):
        self.connection._transport.synchronous = True
        # would have been written in ctor
        self.connection._synchronous_connect = True
        self.connection._connected = 'maybe'
        self.connection._closed = 'possibly'
        self.connection._debug = 'sure'
        self.connection._connect_timeout = 42
        self.connection._sock_opts = {
            ('f1', 't1'): 5,
            ('f2', 't2'): 6
        }

        expect(self.connection._transport.connect).args(('host', 5672))
        expect(self.connection._transport.write).args('AMQP\x00\x00\x09\x01')
        expect(self.connection._channels[0].add_synchronous_cb)

        expect(self.connection.read_frames)
        expect(self.connection.read_frames).side_effect(
            lambda: setattr(self.connection, '_connected', True))

        self.connection.connect('host', 5672)
        assert_true(self.connection._connected)
        assert_false(self.connection._closed)
        assert_equals(self.connection._close_info,
                      {
                          'reply_code': 0,
                          'reply_text': 'failed to connect to host:5672',
                          'class_id': 0,
                          'method_id': 0
                      })
        assert_equals('host:5672', self.connection._host)

    def test_disconnect_when_transport_disconnects(self):
        self.connection._connected = 'yup'

        expect(self.connection._transport.disconnect)
        self.connection.disconnect()

        assert_false(self.connection._connected)
        assert_equals(None, self.connection._transport)

    def test_disconnect_when_transport_disconnects_with_error(self):
        self.connection._connected = 'yup'
        self.connection._host = 'server'

        expect(self.connection._transport.disconnect).raises(
            RuntimeError('fail'))
        expect(self.connection.logger.error).args(
            "Failed to disconnect from %s", 'server', exc_info=True)
        assert_raises(RuntimeError, self.connection.disconnect)

        assert_false(self.connection._connected)
        assert_equals(None, self.connection._transport)

    def test_disconnect_when_systemexit(self):
        self.connection._connected = 'yup'
        self.connection._host = 'server'

        expect(self.connection._transport.disconnect).raises(SystemExit())
        stub(self.connection.logger.error)
        assert_raises(SystemExit, self.connection.disconnect)

        assert_false(self.connection._connected)
        assert_equals(None, self.connection._transport)

    def test_transport_closed_with_no_args(self):
        self.connection._host = 'server'
        self.connection._connected = 'yes'

        expect(self.connection.logger.warning).args(
            'transport to server closed : unknown cause')
        expect(self.connection._callback_close)

        self.connection.transport_closed()

        assert_equals(0, self.connection._close_info['reply_code'])
        assert_equals(
            'unknown cause', self.connection._close_info['reply_text'])
        assert_equals(0, self.connection._close_info['class_id'])
        assert_equals(0, self.connection._close_info['method_id'])

    def test_next_channel_id_when_less_than_max(self):
        self.connection._channel_counter = 32
        self.connection._channel_max = 23423
        assert_equals(33, self.connection._next_channel_id())

    def test_next_channel_id_when_at_max(self):
        self.connection._channel_counter = 32
        self.connection._channel_max = 32
        assert_equals(1, self.connection._next_channel_id())

    def test_channel_creates_new_when_not_at_limit(self):
        ch = mock()
        expect(self.connection._next_channel_id).returns(1)
        mock(connection, 'Channel')
        expect(connection.Channel).args(
            self.connection, 1, self.connection._class_map, synchronous=False).returns(ch)
        expect(ch.add_close_listener).args(self.connection._channel_closed)
        expect(ch.open)

        assert_equals(ch, self.connection.channel())
        assert_equals(ch, self.connection._channels[1])

    def test_channel_creates_optionally_synchronous(self):
        ch = mock()
        expect(self.connection._next_channel_id).returns(1)
        mock(connection, 'Channel')
        expect(connection.Channel).args(
            self.connection, 1, self.connection._class_map, synchronous=True).returns(ch)
        expect(ch.add_close_listener).args(self.connection._channel_closed)
        expect(ch.open)

        assert_equals(ch, self.connection.channel(synchronous=True))
        assert_equals(ch, self.connection._channels[1])

    def test_channel_finds_the_first_free_channel_id(self):
        self.connection._channels[1] = 'foo'
        self.connection._channels[2] = 'bar'
        self.connection._channels[4] = 'cat'
        ch = mock()
        expect(self.connection._next_channel_id).returns(1)
        expect(self.connection._next_channel_id).returns(2)
        expect(self.connection._next_channel_id).returns(3)
        mock(connection, 'Channel')
        expect(connection.Channel).args(
            self.connection, 3, self.connection._class_map, synchronous=False).returns(ch)
        expect(ch.add_close_listener).args(self.connection._channel_closed)
        expect(ch.open)

        assert_equals(ch, self.connection.channel())
        assert_equals(ch, self.connection._channels[3])

    def test_channel_raises_toomanychannels(self):
        self.connection._channels[1] = 'foo'
        self.connection._channels[2] = 'bar'
        self.connection._channels[4] = 'cat'
        self.connection._channel_max = 3
        assert_raises(Connection.TooManyChannels, self.connection.channel)

    def test_channel_returns_cached_instance_if_known(self):
        self.connection._channels[1] = 'foo'
        assert_equals('foo', self.connection.channel(1))

    def test_channel_raises_invalidchannel_if_unknown_id(self):
        assert_raises(Connection.InvalidChannel, self.connection.channel, 42)

    def test_channel_closed(self):
        ch = mock()
        ch.channel_id = 42
        self.connection._channels[42] = ch

        self.connection._channel_closed(ch)
        assert_false(42 in self.connection._channels)

        ch.channel_id = 500424834
        self.connection._channel_closed(ch)

    def test_close(self):
        self.connection._channels[0] = mock()
        expect(self.connection._channels[0].close)

        self.connection.close()
        assert_equals({'reply_code': 0, 'reply_text': '', 'class_id': 0, 'method_id': 0},
                      self.connection._close_info)

        self.connection.close(1, 'foo', 2, 3)
        assert_equals({'reply_code': 1, 'reply_text': 'foo', 'class_id': 2, 'method_id': 3},
                      self.connection._close_info)

    def test_close_when_disconnect(self):
        self.connection._channels[0] = mock()
        stub(self.connection._channels[0].close)

        assert_false(self.connection._closed)
        expect(self.connection.disconnect)
        expect(self.connection._callback_close)
        self.connection.close(1, 'foo', 2, 3, disconnect=True)
        assert_true(self.connection._closed)
        assert_equals({'reply_code': 1, 'reply_text': 'foo', 'class_id': 2, 'method_id': 3},
                      self.connection._close_info)

    def test_callback_open_when_no_cb(self):
        self.connection._open_cb = None
        self.connection._callback_open()

    def test_callback_open_when_user_cb(self):
        self.connection._open_cb = mock()
        expect(self.connection._open_cb)
        self.connection._callback_open()

    def test_callback_open_raises_when_user_cb_does(self):
        self.connection._open_cb = mock()
        expect(self.connection._open_cb).raises(SystemExit())
        assert_raises(SystemExit, self.connection._callback_open)

    def test_callback_close_when_no_cb(self):
        self.connection._close_cb = None
        self.connection._callback_close()

    def test_callback_close_when_user_cb(self):
        self.connection._close_cb = mock()
        expect(self.connection._close_cb)
        self.connection._callback_close()

    def test_callback_close_raises_when_user_cb_does(self):
        self.connection._close_cb = mock()
        expect(self.connection._close_cb).raises(SystemExit())
        assert_raises(SystemExit, self.connection._callback_close)

    def test_read_frames_when_no_transport(self):
        self.connection._transport = None
        self.connection.read_frames()
        assert_equals(0, self.connection._frames_read)

    def test_read_frames_when_transport_returns_no_data(self):
        self.connection._heartbeat = None
        expect(self.connection._channels[0].send_heartbeat)
        expect(self.connection._transport.read).args(None).returns(None)
        self.connection.read_frames()
        assert_equals(0, self.connection._frames_read)

    def test_read_frames_when_transport_when_frame_data_and_no_debug_and_no_buffer(self):
        reader = mock()
        frame = mock()
        frame.channel_id = 42
        channel = mock()
        mock(connection, 'Reader')
        self.connection._heartbeat = 3

        expect(self.connection._channels[0].send_heartbeat)
        expect(self.connection._transport.read).args(3).returns('data')
        expect(connection.Reader).args('data').returns(reader)
        expect(connection.Frame.read_frames).args(reader).returns([frame])
        expect(self.connection.channel).args(42).returns(channel)
        expect(channel.buffer_frame).args(frame)
        expect(self.connection._transport.process_channels).args(
            set([channel]))
        expect(reader.tell).returns(4)

        self.connection.read_frames()
        assert_equals(1, self.connection._frames_read)

    def test_read_frames_when_transport_when_frame_data_and_debug_and_buffer(self):
        reader = mock()
        frame = mock()
        frame.channel_id = 42
        channel = mock()
        mock(connection, 'Reader')
        self.connection._debug = 2

        expect(self.connection._channels[0].send_heartbeat)
        expect(self.connection._transport.read).args(None).returns('data')
        expect(connection.Reader).args('data').returns(reader)
        expect(connection.Frame.read_frames).args(reader).returns([frame])
        expect(self.connection.logger.debug).args('READ: %s', frame)
        expect(self.connection.channel).args(42).returns(channel)
        expect(channel.buffer_frame).args(frame)
        expect(self.connection._transport.process_channels).args(
            set([channel]))
        expect(reader.tell).times(2).returns(2)
        expect(self.connection._transport.buffer).args('ta')

        self.connection.read_frames()
        assert_equals(1, self.connection._frames_read)

    def test_read_frames_when_read_frame_error(self):
        reader = mock()
        frame = mock()
        frame.channel_id = 42
        channel = mock()
        mock(connection, 'Reader')
        self.connection._heartbeat = 3

        expect(self.connection._channels[0].send_heartbeat)
        expect(self.connection._transport.read).args(3).returns('data')
        expect(connection.Reader).args('data').returns(reader)
        expect(connection.Frame.read_frames).args(
            reader).raises(Frame.FrameError)
        stub(self.connection.channel)
        stub(channel.buffer_frame)
        stub(self.connection._transport.process_channels)
        stub(reader.tell)
        stub(self.connection._transport.buffer)
        expect(self.connection.close).args(
            reply_code=501, reply_text=str, class_id=0, method_id=0, disconnect=True)

        assert_raises(ConnectionError, self.connection.read_frames)

    def test_flush_buffered_frames(self):
        self.connection._output_frame_buffer = ['frame1', 'frame2']
        expect(self.connection.send_frame).args('frame1')
        expect(self.connection.send_frame).args('frame2')

        self.connection._flush_buffered_frames()
        assert_equals([], self.connection._output_frame_buffer)

    def test_send_frame_when_connected_and_transport_and_no_debug(self):
        frame = mock()
        expect(frame.write_frame).args(var('ba'))
        expect(self.connection._transport.write).args(var('ba'))

        self.connection._connected = True
        self.connection.send_frame(frame)
        assert_true(isinstance(var('ba').value, bytearray))
        assert_equals(1, self.connection._frames_written)

    def test_send_frame_when_not_connected_and_not_channel_0(self):
        frame = mock()
        frame.channel_id = 42
        stub(frame.write_frame)
        stub(self.connection._transport.write)

        self.connection._connected = False
        self.connection.send_frame(frame)
        assert_equals([frame], self.connection._output_frame_buffer)

    def test_send_frame_when_not_connected_and_channel_0(self):
        frame = mock()
        frame.channel_id = 0
        expect(frame.write_frame).args(var('ba'))
        expect(self.connection._transport.write).args(var('ba'))

        self.connection._connected = False
        self.connection.send_frame(frame)
        assert_true(isinstance(var('ba').value, bytearray))
        assert_equals(1, self.connection._frames_written)

    def test_send_frame_when_debugging(self):
        frame = mock()
        expect(self.connection.logger.debug).args('WRITE: %s', frame)
        expect(frame.write_frame).args(var('ba'))
        expect(self.connection._transport.write).args(var('ba'))

        self.connection._connected = True
        self.connection._debug = 2
        self.connection.send_frame(frame)
        assert_true(isinstance(var('ba').value, bytearray))
        assert_equals(1, self.connection._frames_written)

    def test_send_frame_when_closed(self):
        self.connection._closed = True
        self.connection._close_info['reply_text'] = 'failed'
        assert_raises(connection.ConnectionClosed,
                      self.connection.send_frame, 'frame')

        self.connection._close_info['reply_text'] = ''
        assert_raises(connection.ConnectionClosed,
                      self.connection.send_frame, 'frame')

        self.connection._close_info = None
        assert_raises(connection.ConnectionClosed,
                      self.connection.send_frame, 'frame')

    def test_send_frame_when_frame_overflow(self):
        frame = mock()
        self.connection._frame_max = 100
        expect(frame.write_frame).side_effect(
            lambda buf: buf.extend('a' * 200))
        expect(self.connection.close).args(
            reply_code=501, reply_text=var('reply'), class_id=0, method_id=0, disconnect=True)
        stub(self.connection._transport.write)

        self.connection._connected = True
        with assert_raises(ConnectionClosed):
            self.connection.send_frame(frame)


class ConnectionChannelTest(Chai):

    def setUp(self):
        super(ConnectionChannelTest, self).setUp()
        self.connection = mock()
        self.ch = ConnectionChannel(self.connection, 0, {})

    def test_init(self):
        mock(connection, 'super')
        with expect(connection, 'super').args(is_arg(ConnectionChannel), ConnectionChannel).returns(mock()) as s:
            expect(s.__init__).args('a', 'b')

        c = ConnectionChannel('a', 'b')
        assert_equals(c._method_map,
                      {
                          10: c._recv_start,
                          20: c._recv_secure,
                          30: c._recv_tune,
                          41: c._recv_open_ok,
                          50: c._recv_close,
                          51: c._recv_close_ok,
                      }
                      )
        assert_equal(0, c._last_heartbeat_send)

    def test_dispatch_on_heartbeat_frame(self):
        frame = mock()

        expect(frame.type).returns(HeartbeatFrame.type())
        expect(self.ch.send_heartbeat)

        self.ch.dispatch(frame)

    def test_dispatch_method_frame_class_10(self):
        frame = mock()
        frame.class_id = 10
        frame.method_id = 10
        method = self.ch._method_map[10] = mock()

        expect(frame.type).returns(MethodFrame.type())
        expect(method).args(frame)

        self.ch.dispatch(frame)

    def test_dispatch_runs_callbacks(self):
        frame = mock()
        frame.class_id = 10
        frame.method_id = 10
        method = self.ch._method_map[10] = mock()
        cb = mock()

        expect(frame.type).returns(MethodFrame.type())
        expect(self.ch.clear_synchronous_cb).args(method).returns(cb)
        expect(cb).args(frame)

        self.ch.dispatch(frame)

    def test_dispatch_method_frame_raises_invalidmethod(self):
        frame = mock()
        frame.class_id = 10
        frame.method_id = 500

        expect(frame.type).returns(MethodFrame.type())

        with assert_raises(Channel.InvalidMethod):
            self.ch.dispatch(frame)

    def test_dispatch_method_frame_raises_invalidclass(self):
        frame = mock()
        frame.class_id = 11
        frame.method_id = 10

        expect(frame.type).returns(MethodFrame.type())

        with assert_raises(Channel.InvalidClass):
            self.ch.dispatch(frame)

    def test_dispatch_method_frame_raises_invalidframetype(self):
        frame = mock()

        expect(frame.type).returns(HeaderFrame.type())

        with assert_raises(Frame.InvalidFrameType):
            self.ch.dispatch(frame)

    def test_close(self):
        expect(self.ch._send_close)
        self.ch.close()

    def test_send_heartbeat_when_no_heartbeat(self):
        stub(self.ch.send_frame)
        self.ch.connection._heartbeat = None

        self.ch.send_heartbeat()

    def test_send_heartbeat_when_not_sent_yet(self):
        mock(connection, 'time')
        self.ch.connection._heartbeat = 3
        self.ch._last_heartbeat_send = 0

        expect(connection.time.time).returns(4200.3).times(2)
        expect(self.ch.send_frame).args(HeartbeatFrame)

        self.ch.send_heartbeat()
        assert_equals(4200.3, self.ch._last_heartbeat_send)

    def test_send_heartbeat_when_sent_long_ago(self):
        mock(connection, 'time')
        self.ch.connection._heartbeat = 3
        self.ch._last_heartbeat_send = 4196

        expect(connection.time.time).returns(4200.3).times(2)
        expect(self.ch.send_frame).args(HeartbeatFrame)

        self.ch.send_heartbeat()
        assert_equals(4200.3, self.ch._last_heartbeat_send)

    def test_send_heart_when_sent_recently(self):
        mock(connection, 'time')
        self.ch.connection._heartbeat = 3
        self.ch._last_heartbeat_send = 4199

        expect(connection.time.time).returns(4200.3)
        stub(self.ch.send_frame)

        self.ch.send_heartbeat()
        assert_equals(4199, self.ch._last_heartbeat_send)

    def test_recv_start(self):
        expect(self.ch._send_start_ok)
        self.ch.connection._closed = 'maybe'

        self.ch._recv_start('frame')
        assert_false(self.ch.connection._closed)

    def test_send_start_ok(self):
        self.ch.connection._properties = 'props'
        self.ch.connection._login_method = 'please'
        self.ch.connection._login_response = 'thanks'
        self.ch.connection._locale = 'home'

        with expect(mock(connection, 'Writer')).returns(mock()) as writer:
            expect(writer.write_table).args('props')
            expect(writer.write_shortstr).args('please')
            expect(writer.write_longstr).args('thanks')
            expect(writer.write_shortstr).args('home')

            expect(mock(connection, 'MethodFrame')).args(
                0, 10, 11, writer).returns('frame')
            expect(self.ch.send_frame).args('frame')
        expect(self.ch.add_synchronous_cb).args(self.ch._recv_tune)

        self.ch._send_start_ok()

    def test_recv_tune_when_no_broker_max_and_defined_heartbeat(self):
        self.ch.connection._channel_max = 42
        self.ch.connection._frame_max = 43
        self.ch.connection._heartbeat = 8

        frame = mock()
        expect(frame.args.read_short).returns(0)
        expect(frame.args.read_long).returns(0)

        expect(self.ch._send_tune_ok)
        expect(self.ch._send_open)
        expect(self.ch.send_heartbeat)

        self.ch._recv_tune(frame)
        assert_equals(42, self.ch.connection._channel_max)
        assert_equals(43, self.ch.connection._frame_max)
        assert_equals(8, self.ch.connection._heartbeat)

    def test_recv_tune_when_broker_max_and_undefined_heartbeat(self):
        self.ch.connection._channel_max = 42
        self.ch.connection._frame_max = 43
        self.ch.connection._heartbeat = None

        frame = mock()
        expect(frame.args.read_short).returns(500)
        expect(frame.args.read_long).returns(501)
        expect(frame.args.read_short).returns(7)

        expect(self.ch._send_tune_ok)
        expect(self.ch._send_open)
        expect(self.ch.send_heartbeat)

        self.ch._recv_tune(frame)
        assert_equals(500, self.ch.connection._channel_max)
        assert_equals(501, self.ch.connection._frame_max)
        assert_equals(7, self.ch.connection._heartbeat)

    def test_send_tune_ok_when_heartbeat(self):
        self.ch.connection._channel_max = 42
        self.ch.connection._frame_max = 43
        self.ch.connection._heartbeat = 8

        with expect(mock(connection, 'Writer')).returns(mock()) as writer:
            expect(writer.write_short).args(42)
            expect(writer.write_long).args(43)
            expect(writer.write_short).args(8)

            expect(mock(connection, 'MethodFrame')).args(
                0, 10, 31, writer).returns('frame')
            expect(self.ch.send_frame).args('frame')

        self.ch._send_tune_ok()

    def test_send_tune_ok_when_no_heartbeat(self):
        self.ch.connection._channel_max = 42
        self.ch.connection._frame_max = 43
        self.ch.connection._heartbeat = None

        with expect(mock(connection, 'Writer')).returns(mock()) as writer:
            expect(writer.write_short).args(42)
            expect(writer.write_long).args(43)
            expect(writer.write_short).args(0)

            expect(mock(connection, 'MethodFrame')).args(
                0, 10, 31, writer).returns('frame')
            expect(self.ch.send_frame).args('frame')

        self.ch._send_tune_ok()

    def test_recv_secure(self):
        expect(self.ch._send_open)
        self.ch._recv_secure('frame')

    def test_send_open(self):
        self.connection._vhost = '/foo'

        with expect(mock(connection, 'Writer')).returns(mock()) as writer:
            expect(writer.write_shortstr).args('/foo')
            expect(writer.write_shortstr).args('')
            expect(writer.write_bit).args(True)

            expect(mock(connection, 'MethodFrame')).args(
                0, 10, 40, writer).returns('frame')
            expect(self.ch.send_frame).args('frame')
        expect(self.ch.add_synchronous_cb).args(self.ch._recv_open_ok)

        self.ch._send_open()

    def test_recv_open_ok(self):
        self.ch.connection._connected = False
        expect(self.ch.connection._flush_buffered_frames)
        expect(self.ch.connection._callback_open)

        self.ch._recv_open_ok('frame')
        assert_true(self.ch.connection._connected)

    def test_send_close(self):
        self.ch.connection._close_info = {
            'reply_code': 42,
            'reply_text': 'wrong answer' * 60,
            'class_id': 4,
            'method_id': 20,
        }

        with expect(mock(connection, 'Writer')).returns(mock()) as writer:
            expect(writer.write_short).args(42)
            expect(writer.write_shortstr).args(('wrong answer' * 60)[:255])
            expect(writer.write_short).args(4)
            expect(writer.write_short).args(20)

            expect(mock(connection, 'MethodFrame')).args(
                0, 10, 50, writer).returns('frame')
            expect(self.ch.send_frame).args('frame')
        expect(self.ch.add_synchronous_cb).args(self.ch._recv_close_ok)

        self.ch._send_close()

    def test_recv_close(self):
        self.ch.connection._closed = False

        frame = mock()
        expect(frame.args.read_short).returns(42)
        expect(frame.args.read_shortstr).returns('wrong answer')
        expect(frame.args.read_short).returns(4)
        expect(frame.args.read_short).returns(20)

        expect(self.ch._send_close_ok)
        expect(self.ch.connection.disconnect)
        expect(self.ch.connection._callback_close)

        self.ch._recv_close(frame)
        assert_equals(self.ch.connection._close_info, {
            'reply_code': 42,
            'reply_text': 'wrong answer',
            'class_id': 4,
            'method_id': 20,
        })
        assert_true(self.ch.connection._closed)

    def test_send_close_ok(self):
        expect(mock(connection, 'MethodFrame')).args(
            0, 10, 51).returns('frame')
        expect(self.ch.send_frame).args('frame')
        self.ch._send_close_ok()

    def test_recv_close_ok(self):
        self.ch.connection._closed = False
        expect(self.ch.connection.disconnect)
        expect(self.ch.connection._callback_close)

        self.ch._recv_close_ok('frame')
        assert_true(self.ch.connection._closed)
