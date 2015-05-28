'''
Copyright (c) 2011-2015, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.channel import Channel
from haigha.frames.frame import Frame
from haigha.frames.heartbeat_frame import HeartbeatFrame
from haigha.frames.method_frame import MethodFrame
from haigha.classes.basic_class import BasicClass
from haigha.classes.channel_class import ChannelClass
from haigha.classes.exchange_class import ExchangeClass
from haigha.classes.queue_class import QueueClass
from haigha.classes.transaction_class import TransactionClass
from haigha.writer import Writer
from haigha.reader import Reader
from haigha.transports.transport import Transport
from exceptions import ConnectionError, ConnectionClosed

import haigha
import time

from logging import root as root_logger

# From the rabbitmq mailing list
# AMQP1180 = 0-8
# AMQP1109 = 0-9
# AMQP0091 = 0-9-1
# http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2010-July/008231.html
# PROTOCOL_HEADER = 'AMQP\x01\x01\x09\x01'
PROTOCOL_HEADER = 'AMQP\x00\x00\x09\x01'

# Client property info that gets sent to the server on connection startup
LIBRARY_PROPERTIES = {
    'library': 'Haigha',
    'library_version': haigha.__version__,
}


class Connection(object):

    class TooManyChannels(ConnectionError):
        '''This connection has too many channels open.  Non-fatal.'''

    class InvalidChannel(ConnectionError):
        '''
        The channel id does not correspond to an existing channel.  Non-fatal.
        '''

    def __init__(self, **kwargs):
        '''
        Initialize the connection.
        '''
        self._debug = kwargs.get('debug', False)
        self._logger = kwargs.get('logger', root_logger)

        self._user = kwargs.get('user', 'guest')
        self._password = kwargs.get('password', 'guest')
        self._host = kwargs.get('host', 'localhost')
        self._port = kwargs.get('port', 5672)
        self._vhost = kwargs.get('vhost', '/')

        self._connect_timeout = kwargs.get('connect_timeout', 5)
        self._sock_opts = kwargs.get('sock_opts')
        self._sock = None
        self._heartbeat = kwargs.get('heartbeat')
        self._open_cb = kwargs.get('open_cb')
        self._close_cb = kwargs.get('close_cb')

        self._login_method = kwargs.get('login_method', 'AMQPLAIN')
        self._locale = kwargs.get('locale', 'en_US')
        self._client_properties = kwargs.get('client_properties')

        self._properties = LIBRARY_PROPERTIES.copy()
        if self._client_properties:
            self._properties.update(self._client_properties)

        self._closed = False
        self._connected = False
        self._close_info = {
            'reply_code': 0,
            'reply_text': 'first connect',
            'class_id': 0,
            'method_id': 0
        }

        # Not sure what's better here, setdefaults or require the caller to
        # pass the whole thing in.
        self._class_map = kwargs.get('class_map', {}).copy()
        self._class_map.setdefault(20, ChannelClass)
        self._class_map.setdefault(40, ExchangeClass)
        self._class_map.setdefault(50, QueueClass)
        self._class_map.setdefault(60, BasicClass)
        self._class_map.setdefault(90, TransactionClass)

        self._channels = {
            0: ConnectionChannel(self, 0, {})
        }

        self._last_octet_time = None

        # Login response seems a total hack of protocol
        # Skip the length at the beginning
        login_response = Writer()
        login_response.write_table(
            {'LOGIN': self._user, 'PASSWORD': self._password})
        self._login_response = login_response.buffer()[4:]

        self._channel_counter = 0
        self._channel_max = 65535
        self._frame_max = 65535

        self._frames_read = 0
        self._frames_written = 0

        # Default to the socket strategy
        transport = kwargs.get('transport', 'socket')
        if not isinstance(transport, Transport):
            if transport == 'event':
                from haigha.transports.event_transport import EventTransport
                self._transport = EventTransport(self)
            elif transport == 'gevent':
                from haigha.transports.gevent_transport import GeventTransport
                self._transport = GeventTransport(self)
            elif transport == 'gevent_pool':
                from haigha.transports.gevent_transport import \
                    GeventPoolTransport
                self._transport = GeventPoolTransport(self, **kwargs)
            elif transport == 'socket':
                from haigha.transports.socket_transport import SocketTransport
                self._transport = SocketTransport(self)
        else:
            self._transport = transport

        # Set these after the transport is initialized, so that we can access
        # the synchronous property
        self._synchronous = kwargs.get('synchronous', False)
        self._synchronous_connect = kwargs.get(
            'synchronous_connect', False) or self.synchronous

        self._output_frame_buffer = []
        self.connect(self._host, self._port)

    @property
    def logger(self):
        return self._logger

    @property
    def debug(self):
        return self._debug

    @property
    def frame_max(self):
        return self._frame_max

    @property
    def channel_max(self):
        return self._channel_max

    @property
    def frames_read(self):
        '''Number of frames read in the lifetime of this connection.'''
        return self._frames_read

    @property
    def frames_written(self):
        '''Number of frames written in the lifetime of this connection.'''
        return self._frames_written

    @property
    def closed(self):
        '''Return the closed state of the connection.'''
        return self._closed

    @property
    def close_info(self):
        '''
        Return dict with information on why this connection is closed.  Will
        return None if the connections is open.
        '''
        return self._close_info if (
            self._closed or not self._connected) else None

    @property
    def transport(self):
        '''Get the value of the current transport.'''
        return self._transport

    @property
    def synchronous(self):
        '''
        True if transport is synchronous or the connection has been forced
        into synchronous mode, False otherwise.
        '''
        if self._transport is None:
            if self._close_info and len(self._close_info['reply_text']) > 0:
                raise ConnectionClosed("connection is closed: %s : %s" %
                                       (self._close_info['reply_code'],
                                        self._close_info['reply_text']))
            raise ConnectionClosed("connection is closed")
        return self.transport.synchronous or self._synchronous

    def connect(self, host, port):
        '''
        Connect to a host and port.
        '''
        # Clear the connect state immediately since we're no longer connected
        # at this point.
        self._connected = False

        # Only after the socket has connected do we clear this state; closed
        # must be False so that writes can be buffered in writePacket(). The
        # closed state might have been set to True due to a socket error or a
        # redirect.
        self._host = "%s:%d" % (host, port)
        self._closed = False
        self._close_info = {
            'reply_code': 0,
            'reply_text': 'failed to connect to %s' % (self._host),
            'class_id': 0,
            'method_id': 0
        }

        self._transport.connect((host, port))
        self._transport.write(PROTOCOL_HEADER)

        self._last_octet_time = time.time()

        if self._synchronous_connect:
            # Have to queue this callback just after connect, it can't go
            # into the constructor because the channel needs to be
            # "always there" for frame processing, but the synchronous
            # callback can't be added until after the protocol header has
            # been written. This SHOULD be registered before the protocol
            # header is written, in the case where the header bytes are
            # written, but this thread/greenlet/context does not return until
            # after another thread/greenlet/context has read and processed the
            # recv_start frame. Without more re-write to add_sync_cb though,
            # it will block on reading responses that will never arrive
            # because the protocol header isn't written yet. TBD if needs
            # refactoring. Could encapsulate entirely here, wherein
            # read_frames exits if protocol header not yet written. Like other
            # synchronous behaviors, adding this callback will result in a
            # blocking frame read and process loop until _recv_start and any
            # subsequent synchronous callbacks have been processed. In the
            # event that this is /not/ a synchronous transport, but the
            # caller wants the connect to be synchronous so as to ensure that
            # the connection is ready, then do a read frame loop here.
            self._channels[0].add_synchronous_cb(self._channels[0]._recv_start)
            while not self._connected:
                self.read_frames()

    def disconnect(self):
        '''
        Disconnect from the current host, but do not update the closed state.
        After the transport is disconnected, the closed state will be True if
        this is called after a protocol shutdown, or False if the disconnect
        was in error.

        TODO: do we really need closed vs. connected states? this only adds
        complication and the whole reconnect process has been scrapped anyway.

        '''
        self._connected = False
        if self._transport is not None:
            try:
                self._transport.disconnect()
            except Exception:
                self.logger.error(
                    "Failed to disconnect from %s", self._host, exc_info=True)
                raise
            finally:
                self._transport = None

    ###
    # Transport methods
    ###
    def transport_closed(self, **kwargs):
        """
        Called by Transports when they close unexpectedly, not as a result of
        Connection.disconnect().

        TODO: document args
        """
        msg = 'unknown cause'
        self.logger.warning('transport to %s closed : %s' %
                            (self._host, kwargs.get('msg', msg)))
        self._close_info = {
            'reply_code': kwargs.get('reply_code', 0),
            'reply_text': kwargs.get('msg', msg),
            'class_id': kwargs.get('class_id', 0),
            'method_id': kwargs.get('method_id', 0)
        }

        # We're not connected any more, but we're not closed without an
        # explicit close call.
        self._connected = False
        self._transport = None

        # Call back to a user-provided close function
        self._callback_close()

    ###
    # Connection methods
    ###
    def _next_channel_id(self):
        '''Return the next possible channel id.  Is a circular enumeration.'''
        self._channel_counter += 1
        if self._channel_counter >= self._channel_max:
            self._channel_counter = 1
        return self._channel_counter

    def channel(self, channel_id=None, synchronous=False):
        """
        Fetch a Channel object identified by the numeric channel_id, or
        create that object if it doesn't already exist.  If channel_id is not
        None but no channel exists for that id, will raise InvalidChannel.  If
        there are already too many channels open, will raise TooManyChannels.

        If synchronous=True, then the channel will act synchronous in all cases
        where a protocol method supports `nowait=False`, or where there is an
        implied callback in the protocol.
        """
        if channel_id is None:
            # adjust for channel 0
            if len(self._channels) - 1 >= self._channel_max:
                raise Connection.TooManyChannels(
                    "%d channels already open, max %d",
                    len(self._channels) - 1,
                    self._channel_max)
            channel_id = self._next_channel_id()
            while channel_id in self._channels:
                channel_id = self._next_channel_id()
        elif channel_id in self._channels:
            return self._channels[channel_id]
        else:
            raise Connection.InvalidChannel(
                "%s is not a valid channel id", channel_id)

        # Call open() here so that ConnectionChannel doesn't have it called.
        # Could also solve this other ways, but it's a HACK regardless.
        rval = Channel(
            self, channel_id, self._class_map, synchronous=synchronous)
        self._channels[channel_id] = rval
        rval.add_close_listener(self._channel_closed)
        rval.open()
        return rval

    def _channel_closed(self, channel):
        '''
        Close listener on a channel.
        '''
        try:
            del self._channels[channel.channel_id]
        except KeyError:
            pass

    def close(self, reply_code=0, reply_text='', class_id=0, method_id=0,
              disconnect=False):
        '''
        Close this connection.
        '''
        self._close_info = {
            'reply_code': reply_code,
            'reply_text': reply_text,
            'class_id': class_id,
            'method_id': method_id
        }
        if disconnect:
            self._closed = True
            self.disconnect()
            self._callback_close()
        else:
            self._channels[0].close()

    def _callback_open(self):
        '''
        Callback to any open handler that was provided in the ctor. Handler is
        responsible for exceptions.
        '''
        if self._open_cb:
            self._open_cb()

    def _callback_close(self):
        '''
        Callback to any close handler that was provided in the ctor. Handler is
        responsible for exceptions.
        '''
        if self._close_cb:
            self._close_cb()

    def read_frames(self):
        '''
        Read frames from the transport and process them. Some transports may
        choose to do this in the background, in several threads, and so on.
        '''
        # It's possible in a concurrent environment that our transport handle
        # has gone away, so handle that cleanly.
        # TODO: Consider moving this block into Translator base class. In many
        # ways it belongs there. One of the problems though is that this is
        # essentially the read loop. Each Transport has different rules for
        # how to kick this off, and in the case of gevent, this is how a
        # blocking call to read from the socket is kicked off.
        if self._transport is None:
            return

        # Send a heartbeat (if needed)
        self._channels[0].send_heartbeat()

        data = self._transport.read(self._heartbeat)
        current_time = time.time()

        if data is None:
            # Wait for 2 heartbeat intervals before giving up. See AMQP 4.2.7:
            # "If a peer detects no incoming traffic (i.e. received octets) for two heartbeat intervals or longer,
            # it should close the connection"
            if self._heartbeat and (current_time-self._last_octet_time > 2*self._heartbeat):
                msg = 'Heartbeats not received from %s for %d seconds' % (self._host, 2*self._heartbeat)
                self.transport_closed(msg=msg)
                raise ConnectionClosed('Connection is closed: ' + msg)
            return
        self._last_octet_time = current_time
        reader = Reader(data)
        p_channels = set()

        try:
            for frame in Frame.read_frames(reader):
                if self._debug > 1:
                    self.logger.debug("READ: %s", frame)
                self._frames_read += 1
                ch = self.channel(frame.channel_id)
                ch.buffer_frame(frame)
                p_channels.add(ch)
        except Frame.FrameError as e:
            # Frame error in the peer, disconnect
            self.close(reply_code=501,
                       reply_text='frame error from %s : %s' % (
                           self._host, str(e)),
                       class_id=0, method_id=0, disconnect=True)
            raise ConnectionClosed("connection is closed: %s : %s" %
                                   (self._close_info['reply_code'],
                                    self._close_info['reply_text']))

        self._transport.process_channels(p_channels)

        # HACK: read the buffer contents and re-buffer.  Would prefer to pass
        # buffer back, but there's no good way of asking the total size of the
        # buffer, comparing to tell(), and then re-buffering.  There's also no
        # ability to clear the buffer up to the current position. It would be
        # awesome if we could free that memory without a new allocation.
        if reader.tell() < len(data):
            self._transport.buffer(data[reader.tell():])

    def _flush_buffered_frames(self):
        '''
        Callback when protocol has been initialized on channel 0 and we're
        ready to send out frames to set up any channels that have been
        created.
        '''
        # In the rare case (a bug) where this is called but send_frame thinks
        # they should be buffered, don't clobber.
        frames = self._output_frame_buffer
        self._output_frame_buffer = []
        for frame in frames:
            self.send_frame(frame)

    def send_frame(self, frame):
        '''
        Send a single frame. If there is no transport or we're not connected
        yet, append to the output buffer, else send immediately to the socket.
        This is called from within the MethodFrames.
        '''
        if self._closed:
            if self._close_info and len(self._close_info['reply_text']) > 0:
                raise ConnectionClosed("connection is closed: %s : %s" %
                                       (self._close_info['reply_code'],
                                        self._close_info['reply_text']))
            raise ConnectionClosed("connection is closed")

        if self._transport is None or \
                (not self._connected and frame.channel_id != 0):
            self._output_frame_buffer.append(frame)
            return

        if self._debug > 1:
            self.logger.debug("WRITE: %s", frame)

        buf = bytearray()
        frame.write_frame(buf)
        if len(buf) > self._frame_max:
            self.close(
                reply_code=501,
                reply_text='attempted to send frame of %d bytes, frame max %d' % (
                    len(buf), self._frame_max),
                class_id=0, method_id=0, disconnect=True)
            raise ConnectionClosed(
                "connection is closed: %s : %s" %
                (self._close_info['reply_code'],
                 self._close_info['reply_text']))
        self._transport.write(buf)

        self._frames_written += 1


class ConnectionChannel(Channel):

    '''
    A special channel for the Connection class.  It's used for performing the
    special methods only available on the main connection channel.  It's also
    partly used to hide the 'connection' protocol implementation, which would
    show up as a property, from the more useful 'connection' property that is
    a handle to a Channel's Connection object.
    '''

    def __init__(self, *args):
        super(ConnectionChannel, self).__init__(*args)

        self._method_map = {
            10: self._recv_start,
            20: self._recv_secure,
            30: self._recv_tune,
            41: self._recv_open_ok,
            50: self._recv_close,
            51: self._recv_close_ok,
        }
        self._last_heartbeat_send = 0

    def dispatch(self, frame):
        '''
        Override the default dispatch since we don't need the rest of
        the stack.
        '''
        if frame.type() == HeartbeatFrame.type():
            self.send_heartbeat()

        elif frame.type() == MethodFrame.type():
            if frame.class_id == 10:
                cb = self._method_map.get(frame.method_id)
                if cb:
                    method = self.clear_synchronous_cb(cb)
                    method(frame)
                else:
                    raise Channel.InvalidMethod(
                        "unsupported method %d on channel %d",
                        frame.method_id, self.channel_id)
            else:
                raise Channel.InvalidClass(
                    "class %d is not supported on channel %d",
                    frame.class_id, self.channel_id)

        else:
            raise Frame.InvalidFrameType(
                "frame type %d is not supported on channel %d",
                frame.type(), self.channel_id)

    def close(self, reply_code=0, reply_text='', class_id=0, method_id=0):
        '''
        Close the main connection connection channel.
        '''
        self._send_close()

    def send_heartbeat(self):
        '''
        Send a heartbeat if needed. Tracks last heartbeat send time.
        '''
        # Note that this does not take into account the time that we last
        # sent a frame. Hearbeats are so small the effect should be quite
        # limited. Also note that we're looking for something near to our
        # scheduled interval, because if this is exact, then we'll likely
        # actually send a heartbeat at twice the period, which could cause
        # a broker to kill the connection if the period is large enough. The
        # 90% bound is arbitrary but seems a sensible enough default.
        if self.connection._heartbeat:
            if time.time() >= (self._last_heartbeat_send + 0.9 *
                               self.connection._heartbeat):
                self.send_frame(HeartbeatFrame(self.channel_id))
                self._last_heartbeat_send = time.time()

    def _recv_start(self, method_frame):
        self.connection._closed = False
        self._send_start_ok()

    def _send_start_ok(self):
        '''Send the start_ok message.'''
        args = Writer()
        args.write_table(self.connection._properties)
        args.write_shortstr(self.connection._login_method)
        args.write_longstr(self.connection._login_response)
        args.write_shortstr(self.connection._locale)
        self.send_frame(MethodFrame(self.channel_id, 10, 11, args))

        self.add_synchronous_cb(self._recv_tune)

    def _recv_tune(self, method_frame):
        self.connection._channel_max = method_frame.args.read_short(
        ) or self.connection._channel_max
        self.connection._frame_max = method_frame.args.read_long(
        ) or self.connection._frame_max

        # Note that 'is' test is required here, as 0 and None are distinct
        if self.connection._heartbeat is None:
            self.connection._heartbeat = method_frame.args.read_short()

        self._send_tune_ok()
        self._send_open()

        # 4.2.7: The client should start sending heartbeats after receiving a
        # Connection.Tune method
        self.send_heartbeat()

    def _send_tune_ok(self):
        args = Writer()
        args.write_short(self.connection._channel_max)
        args.write_long(self.connection._frame_max)

        if self.connection._heartbeat:
            args.write_short(self.connection._heartbeat)
        else:
            args.write_short(0)

        self.send_frame(MethodFrame(self.channel_id, 10, 31, args))

    def _recv_secure(self, method_frame):
        self._send_open()

    def _send_open(self):
        args = Writer()
        args.write_shortstr(self.connection._vhost)
        args.write_shortstr('')
        args.write_bit(True)  # insist flag for older amqp, not used in 0.9.1

        self.send_frame(MethodFrame(self.channel_id, 10, 40, args))
        self.add_synchronous_cb(self._recv_open_ok)

    def _recv_open_ok(self, method_frame):
        self.connection._connected = True
        self.connection._flush_buffered_frames()
        self.connection._callback_open()

    def _send_close(self):
        args = Writer()
        args.write_short(self.connection._close_info['reply_code'])
        args.write_shortstr(self.connection._close_info['reply_text'][:255])
        args.write_short(self.connection._close_info['class_id'])
        args.write_short(self.connection._close_info['method_id'])
        self.send_frame(MethodFrame(self.channel_id, 10, 50, args))
        self.add_synchronous_cb(self._recv_close_ok)

    def _recv_close(self, method_frame):
        self.connection._close_info = {
            'reply_code': method_frame.args.read_short(),
            'reply_text': method_frame.args.read_shortstr(),
            'class_id': method_frame.args.read_short(),
            'method_id': method_frame.args.read_short()
        }

        # TODO: wait to disconnect until the close_ok has been flushed, but
        # that's a pain
        self._send_close_ok()

        self.connection._closed = True
        self.connection.disconnect()
        self.connection._callback_close()

    def _send_close_ok(self):
        self.send_frame(MethodFrame(self.channel_id, 10, 51))

    def _recv_close_ok(self, method_frame):
        self.connection._closed = True
        self.connection.disconnect()
        self.connection._callback_close()
