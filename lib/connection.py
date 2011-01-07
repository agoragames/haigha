from haigha.lib.channel import Channel
from haigha.lib.connection_strategy import ConnectionStrategy
from haigha.lib.event_socket import EventSocket
from haigha.lib.frames import *
from haigha.lib.writer import Writer

import event                        # http://code.google.com/p/pyevent/
import socket
import struct
import traceback

from cStringIO import StringIO      # TODO: find suitable alternative
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from logging import root as root_logger

# From the rabbitmq mailing list
# AMQP1180 = 0-8
# AMQP1109 = 0-9
# AMQP0091 = 0-9-1
# http://lists.rabbitmq.com/pipermail/rabbitmq-discuss/2010-July/008231.html
#PROTOCOL_HEADER = 'AMQP\x01\x01\x09\x01'
PROTOCOL_HEADER = 'AMQP\x00\x00\x09\x01'

#
# Client property info that gets sent to the server on connection startup
#
LIBRARY_PROPERTIES = {
  'library': 'Haigha',
  'library_version': '0.0.1',
}

class Connection(object):

  class ConnectionError(Exception): '''Base class for all connection exceptions'''
  class TooManyChannels(ConnectionError): '''This connection has too many channels open.  Non-fatal.'''
  class InvalidChannel(ConnectionError): '''The channel id does not correspond to an existing channel.  Non-fatal.'''
  class Closed(ConnectionError): '''The operation is invalid because the connection is closed.'''

  def __init__(self, **kwargs):
    '''
    Initialize the connection.
    '''
    self._debug = kwargs.get('debug', False)
    self._logger = kwargs.get('logger', root_logger)

    # TODO: make host and port dynamic enough to handle a list.
    self._user = kwargs.get('user', 'guest')
    self._password = kwargs.get('user', 'guest')
    self._host = kwargs.get('host', 'localhost')
    self._vhost = kwargs.get('vhost', '/')

    self._connect_timeout = kwargs.get('connect_timeout', 5)
    self._sock_opts = kwargs.get('sock_opts')
    self._sock = None
    self._heartbeat = kwargs.get('heartbeat')
    self._reconnect_cb = kwargs.get('reconnect_cb')
    self._close_cb = kwargs.get('close_cb')

    self._login_method = kwargs.get('login_method', 'AMQPLAIN')
    self._locale = kwargs.get('locale', 'en_US')
    self._client_properties = kwargs.get('client_properties')

    self._properties = LIBRARY_PROPERTIES.copy()
    if self._client_properties:
      self._properties.update( self._client_properties )

    self._closed = False
    self._connected = False
    self._output_buffer = []
    self._close_info = {
      'reply_code'    : -1,
      'reply_text'    : 'first connect',
      'class_id'      : -1,
      'method_id'     : -1
    }
    
    self._channels = {
      0 : ConnectionChannel(self, 0)
    } 
    
    login_response = Writer()
    login_response.write_table({'LOGIN': self._user, 'PASSWORD': self._password})
    stream = StringIO()
    login_response.flush(stream)
    self._login_response = stream.getvalue()[4:]  #Skip the length
                                                      #at the beginning
    
    self._channel_counter = 0
    self._channel_max = 65535
    self._frame_max = 65535

    self._strategy = kwargs.get('connection_strategy')
    if not self._strategy:
      self._strategy = ConnectionStrategy( self, self._host, reconnect_cb = self._reconnect_cb )
    self._strategy.connect()

    self._input_frame_buffer = []
    self._output_frame_buffer = []
    
  @property
  def logger(self):
    return self._logger

  @property
  def debug(self):
    return self._debug
  
  def reconnect(self):
    '''Reconnect to the configured host and port.'''
    self.strategy.connect()
  
  def connect(self, host, port):
    '''
    Connect to a host and port.
    '''
    # Clear the connect state immediately since we're no longer connected
    # at this point.
    self._connected = False
    
    # NOTE: purposefully leave output_buffer alone so that pending writes can
    # still occur.  this allows the reconnect to occur silently without
    # completely breaking any pending data on, say, a channel that was just
    # opened.
    self._sock = EventSocket( read_cb=self._sock_read_cb,
      close_cb=self._sock_close_cb, error_cb=self._sock_error_cb,
      debug=self._debug, logger=self._logger )
    self._sock.settimeout( self._connect_timeout )
    if self._sock_opts:
      for k,v in self._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.connect( (host,port) )
    self._sock.setblocking( False )

    # Only after the socket has connected do we clear this state; closed must
    # be False so that writes can be buffered in writePacket().  The closed
    # state might have been set to True due to a socket error or a redirect.
    self._closed = False
    self._close_info = {
      'reply_code'    : -1,
      'reply_text'    : 'failed to connect to %s:%d'%(host,port),
      'class_id'      : -1,
      'method_id'     : -1
    }

    self._host = "%s:%d"%(host,port)
    self._sock.write( PROTOCOL_HEADER )
  
  def disconnect(self):
    '''
    Disconnect from the current host, but otherwise leave this object "open"
    so that it can be reconnected.  All channels (except our own) are nuked as
    they'll be useless when we reconnect.
    '''
    self.connected = False
    if self._sock!=None:
      self._sock.close_cb = None
      self._sock.close()
      self._sock = None
    
    # It's possible that this is being called after we've done a standard socket
    # closure and the strategy is trying to reconnect.  In that case, we might
    # not have a socket anymore but the channels are still around.  
    for channel_id in self._channels.keys():
      #if channel_id != self.channel_id: 
      if channel_id != 0:
        del self._channels[channel_id]
  
  def add_reconnect_callback(self, callback):
    '''Adds a reconnect callback to the strategy.  This can be used to
    resubscribe to exchanges, etc.'''
    self.strategy.reconnect_callbacks.append(callback)

  ###
  ### EventSocket callbacks
  ###
  def _sock_read_cb(self, sock):
    '''
    Callback when there's data to read on the socket.
    '''
    self._read_frames()

  def _sock_close_cb(self, sock):
    """
    Callback when socket closed.  This is intended to be the callback when the
    closure is unexpected.
    """
    self.logger.warning( 'socket to %s closed unexpectedly', self._host )
    self._close_info = {
      'reply_code'    : -1,
      'reply_text'    : 'socket closed unexpectedly',
      'class_id'      : -1,
      'method_id'     : -1
    }

    # we're not connected any more (we're not closed but we're definitely not
    # connected)
    self._connected = False

    # 16 Aug 2010 - The connection strategy just isn't enough for dealing with
    # disconnects, or at least not of the nature that we've encountered at TM.
    # So for now, callback to our close callback handler which we know will raise
    # a SystemExit.
    self._close_cb and self._close_cb()

    # Removed check for `self.connected==True` because the strategy does the
    # right job in letting us reconnect when there's a transient error.  If
    # you haven't configured permissions and that's why the socket is closing,
    # at least it will only try to reconnect every few seconds.
    self._strategy.fail()
#    self._strategy.next_host()
  
  def _sock_error_cb(self, sock, msg, exception=None):
    """
    Callback when there's an error on the socket.
    """
    self.logger.error( 'error on connection to %s - %s', self._host, msg)
    self._close_info = {
      'reply_code'    : -1,
      'reply_text'    : "socket error: %s"%(msg),
      'class_id'      : -1,
      'method_id'     : -1
    }
    
    # we're not connected any more (we're not closed but we're definitely not
    # connected)
    self._connected = False

    # 16 Aug 2010 - The connection strategy just isn't enough for dealing with
    # disconnects, or at least not of the nature that we've encountered at TM.
    # So for now, callback to our close callback handler which we know will raise
    # a SystemExit.
    self._close_cb and self._close_cb()

    # Removed check for `self.connected==True` because the strategy does the
    # right job in letting us reconnect when there's a transient error.  If
    # you haven't configured permissions and that's why the socket is closing,
    # at least it will only try to reconnect every few seconds.
    self._strategy.fail()
    self._strategy.next_host()

  ###
  ### Connection methods
  ###
  def _next_channel_id(self):
    '''Return the next possible channel id.  Is a circular enumeration.'''
    self._channel_counter += 1
    if self._channel_counter >= self._channel_max:
      self._channel_counter = 1
    return self._channel_counter

  def channel(self, channel_id=None):
    """
    Fetch a Channel object identified by the numeric channel_id, or
    create that object if it doesn't already exist.  If channel_id is not
    None but no channel exists for that id, will raise InvalidChannel.  If
    there are already too many channels open, will raise TooManyChannels.
    """
    if channel_id is None:
      # adjust for channel 0
      if len(self._channels)-1 >= self._channel_max:
        raise Connection.TooManyChannels( "%d channels already open, max %d",
          len(self._channels)-1, self._channel_max )
      channel_id = self._next_channel_id()
      while channel_id in self._channels:
        channel_id = self._next_channel_id()
    elif channel_id in self._channels:
      return self._channels[channel_id]
    else:
      raise Connect.InvalidChannel("%s is not a valid channel id", channel_id )

    # Call open() here so that ConnectionChannel doesn't have it called.  Could
    # also solve this other ways, but it's a HACK regardless.
    rval = Channel(self, channel_id)
    self._channels[ channel_id ] = rval
    rval.open()
    return rval

  def close(self):
    '''
    Close this connection.
    '''
    # TODO: Allow caller to specify why closing
    # TODO: Let channel keep track of the current method being executed,
    # and use that for class and method ids here.
    self._close_info = {
      'reply_code'    : 0,  #reply_code,
      'reply_text'    : '', #reply_text,
      'class_id'      : 0,  #method_sig[0],
      'method_id'     : 0,  #method_sig[1]
    }
    self._channels[0].close()

  def handle_open_ok(self):
    '''Callback from protocol when connection has been opened.'''
    self._connected = True
    # TODO: once we have an output buffer setup, flush it
    #for (pkt,channel_id) in self.output_buffer:
    #  if channel_id in self.channels:
    #    self.writePacket( pkt, channel_id )
    #self.output_buffer = []

  def handle_close_ok(self, args):
    '''Callback from protocol.'''
    self._close_info = {\
      'reply_code'    : args.read_short(),\
      'reply_text'    : args.read_shortstr(),\
      'class_id'      : args.read_short(),\
      'method_id'     : args.read_short()\
    }

    # Clear the socket close callback because we should be expecting it.  The fact
    # that it is called in practice means that we flush the data, rabbit processes
    # and then closes the socket before the timer below fires.  I don't know what
    # this means, but it is surprising. - AW
    if self._sock != None:
      self._sock.close_cb = None

    # Schedule the actual close for later so that handshake IO can take place.
    event.timeout(0, self._close_socket)

    # Likewise, call any potential close callback on a delay
    event.timeout( 0, self._close_cb )

  def _close_socket(self):
    '''Close the socket.'''
    # The assumption here is that we don't want auto-reconnect to kick in if
    # the socket is purposefully closed.
    self._closed = True

    # By the time we hear about the protocol-level closure, the socket may
    # have already gone away.
    if self._sock != None:
      self._sock.close_cb = None
      try:
        self._sock.close()
      except:
        self.logger.error( 'error closing socket' )
      self._sock = None

  def _close_cb(self):
    '''Callback to any close handler.'''
    if self._close_cb:
      try: self._close_cb( self )
      except SystemExit: raise
      except: self.logger.error( 'error calling close callback' )


  def _read_frames(self):
    '''
    Read frames from the socket.
    '''
    try:
      # TODO: old implementation had a wrapper to handle errors.  Consider if that's
      # still needed, and if so, consider a decorator for much happiness @AW

      # Because of the timer callback to dataRead when we re-buffered, there's a
      # chance that in between we've lost the socket.  If that's the case, just
      # silently return as some code elsewhere would have already notified us.
      # That bug could be fixed by improving the message reading so that we consume
      # all possible messages and ensure that only a partial message was rebuffered,
      # so that we can rely on the next read event to read the subsequent message.
      # TODO: Re-parse that comment and figure out if it still applies. @AW
      if self._sock is None:
        return
      
      buffer = self._sock.read()     # StringIO buffer
      
      try:
        self._input_frame_buffer.extend( Frame.read_frames(buffer) )
      except Frame.FrameError as e:
        self.logger.exception( "Framing error", exc_info=True )
        # TODO:

      # HACK: read the buffer contents and re-buffer.  Would prefer to pass
      # buffer back, but there's no good way of asking the total size of the
      # buffer, comparing to tell(), and then re-buffering.  There's also no
      # ability to clear the buffer up to the current position.
      # TODO: resolve this so there's much less copying going on
      remaining = buffer.read()
      if len(remaining)>0:
        self._sock.buffer( remaining )

        # If data remaining and no read error, re-schedule to continue processing
        # the buffer.
        # TODO: Consider not putting this on a delay, but rather calling it after
        # processMessage().  Need to consider what affect this might have on IO.
        # It would mandate that the client process as quickly as incoming data,
        # else the broker might drop the connection.  In the end, it would likely
        # be the case that only a few messages would be in the buffer at any one
        # time.
        # NOTE: I don't think this applies now because of the way Frame.read_frames()
        # is implemented.  If there's anything remaining, it's because there is a
        # single, partial frame remaining and the only way it will complete is if
        # new data comes in on the socket, at which point we'll get an event and
        # try this method again. @AW
        #if not read_error:
        #  event.timeout( 0, self._read_frames )

      if self._debug > 1:
        for frame in self._input_frame_buffer:
          self.logger.debug( "READ: %s", frame )

      # Even if there was a frame error, process whatever is on the input buffer.
      self._process_input_frames()
    except Exception, e:
      # TODO: log the exception
      # TODO: if there was an exception, but there's still input frames, try
      #       to recover
      traceback.print_exc()
      print (type(e))
      print e

  def _process_input_frames(self):
    content_frames = None
    while len(self._input_frame_buffer):
      frame = self._input_frame_buffer.pop(0)

      if len( self._input_frame_buffer ) and isinstance(self._input_frame_buffer[0],HeaderFrame):
        header = self._input_frame_buffer.pop(0)
        content_frames = []
        while sum( [len(cf.payload) for cf in content_frames] ) < header.size:
          content_frames.append( self._input_frame_buffer.pop(0) )
          if not isinstance( content_frames[-1], ContentFrame ):
            raise Exception("TODO: Invalid content frame %s", content_frames[-1])
        content_frames.insert(0, header)
      
      if content_frames:
        self.channel(frame.channel_id).dispatch(frame, *content_frames)
      else:
        self.channel(frame.channel_id).dispatch(frame)
      
  
  def send_frame(self, frame):
    if self._closed:
      if self._close_info and len(self._close_info['reply_text'])>0:
        raise Connection.Closed("connection is closed: %s : %s"%\
          (self._close_info['reply_code'],self._close_info['reply_text']) )
      raise Connection.Closed("connection is closed")

    if self._sock==None or (not self._connected and frame.channel_id!=0):
      self._output_frame_buffer.append( frame )
      return

    stream = StringIO()
    frame.write_frame(stream)
    
    if self._debug > 1:
      self.logger.debug( "WRITE: %s", frame )
  
    self._sock.write(stream.getvalue())
    

class ConnectionChannel(Channel):
  '''
  A special channel for the Connection class.  It's used for performing the special
  methods only available on the main connection channel.  It's also partly used to
  hide the 'connection' protocol implementation, which would show up as a property,
  from the more useful 'connection' property that is a handle to a Channel's 
  Connection object.
  '''

  def __init__(self, *args):
    super(ConnectionChannel,self).__init__(*args)

    self._method_map = {
      10 : self._recv_start,
      20 : self._recv_secure,
      30 : self._recv_tune,
      41 : self._recv_open_ok,
      50 : self._recv_close,
      51 : self._recv_close_ok,

      # HACK: for older AMQP protocols:
      60 : self._recv_close,
      61 : self._recv_close_ok,
    }

  def dispatch(self, frame, *content_frames):
    '''
    Override the default dispatch since we don't need the rest of the stack.
    '''
    if len(content_frames):
      # TODO: raise connection exception with 504 code (per spec)
      raise Frame.FrameError("heartbeat followed by content frames on channel %d",
        self.channel_id)

    if frame.type()==HeartbeatFrame.type():
      self._send_heartbeat()

    elif frame.type()==MethodFrame.type():
      if frame.class_id==10:
        cb = self._method_map.get( frame.method_id )
        if cb:
          #self.logger.debug('DEBUG: connection callback %s:%s to %s', frame.class_id, frame.method_id, cb)
          cb( frame )
        else:
          self.logger.warning("WARNING: TODO: RAISE INVALIDMETHOD EXCEPTION for %s", frame.method_id)
      else:
        raise Channel.InvalidClass( "class %d is not supported on channel %d", 
          frame.class_id, self.channel_id )

    else:
      raise Frame.InvalidFrameType("frame type %d is not supported on channel %d",
        frame.type(), self.channel_id)

  def close(self):
    '''
    Close the main connection connection channel.
    '''
    self._send_close()

  def _send_heartbeat(self):
    self.send_frame( HeartbeatFrame(self.channel_id) )

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
    self.send_frame( MethodFrame(self.channel_id, 10, 11, args) )

  def _recv_tune(self, method_frame):
    # TODO: make this a bit smarter, such that if the client defines a value
    # which is smaller than the broker, that we adhere to that.  Confirm with
    # spec that client has an equal role in defining this.
    self.connection._channel_max = method_frame.args.read_short() or self.connection._channel_max
    self.connection._frame_max = method_frame.args.read_long() or self.connection._frame_max

    # Note that 'is' test is required here, as 0 and None are distinct
    if self.connection._heartbeat is None:
      self.connection.heartbeat = method_frame.args.read_short()

    self._send_tune_ok()
    self._send_open()

  def _send_tune_ok(self):
    args = Writer()
    args.write_short( self.connection._channel_max )
    args.write_long( self.connection._frame_max )
    
    if self.connection._heartbeat:
      args.write_short( self.connection._heartbeat )
    else:
      args.write_short( 0 )

    #self.logger.debug( 'channel max %d, frame max %d, heartbeat %s', self.connection._channel_max, self.connection._frame_max, self.connection._heartbeat )
    self.send_frame( MethodFrame(self.channel_id, 10, 31, args) )

  def _recv_secure(self, method_frame):
    self._send_open()

  def _send_open(self):
    args = Writer()
    args.write_shortstr(self.connection._vhost)
    #args.write_shortstr(self.connection._capabilities)
    args.write_shortstr('') # TODO: Implement capabilites for connection
    args.write_bit(True)  # HACK: insist flag for older amqp
    
    self.send_frame( MethodFrame(self.channel_id, 10, 40, args) )

  def _recv_open_ok(self, method_frame):
    # TODO: re-implement the frame buffering scheme and flush now that connection
    # is ready.
    self.connection._connected = True
    for frame in self.connection._output_frame_buffer:
      self.connection.send_frame( frame )

    self.connection._output_frame_buffer = []
    #for (pkt,channel_id) in self.output_buffer:
    #  if channel_id in self.channels:
    #    self.writePacket( pkt, channel_id )
    #self.output_buffer = []

  def _send_close(self):
    args = Writer()
    args.write_short( self.connection._close_info['reply_code'] )
    args.write_shortstr( self.connection._close_info['reply_text'] )
    args.write_short( self.connection._close_info['class_id'] )
    args.write_short( self.connection._close_info['method_id'] )
    self.send_frame( MethodFrame(10, 60, args) )    # TODO: HACK: Use 50 for newer AMQP

  def _recv_close(self, method_frame):
    self.connection._close_info = {
      'reply_code'    : method_frame.args.read_short(),
      'reply_text'    : method_frame.args.read_shortstr(),
      'class_id'      : method_frame.args.read_short(),
      'method_id'     : method_frame.args.read_short()
    }

    self._send_close_ok()

    # Clear the socket close callback because we should be expecting it.  The fact
    # that it is called in practice means that we flush the data, rabbit processes
    # and then closes the socket before the timer below fires.  I don't know what
    # this means, but it is surprising. - AW
    if self.connection._sock != None:
      self.connection._sock.close_cb = None

    # Schedule the actual close for later so that handshake IO can take place.
    event.timeout(0, self.connection._close_socket)

    # Likewise, call any potential close callback on a delay
    event.timeout( 0, self.connection._close_cb )

  def _send_close_ok(self):
    self.send_frame( MethodFrame(self.channel_id, 10, 61) )

  def _recv_close_ok(self, method_frame):
    self.connection._close_socket()
