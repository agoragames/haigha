from haigha.channel import Channel
#from haigha.message import Message
from haigha.lib.connection_strategy import ConnectionStrategy
from haigha.lib.event_socket import EventSocket
from haigha.lib.frames import *

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
PROTOCOL_HEADER = 'AMQP\x01\x01\x09\x01'

#
# Client property info that gets sent to the server on connection startup
#
LIBRARY_PROPERTIES = {
  'library': 'Haigha',
  'library_version': '0.1',
}

class Connection(object):

  class ConnectionError(Exception): '''Base class for all connection exceptions'''
  class TooManyChannels(ConnectionError): '''This connection has too many channels open.  Non-fatal.'''
  class InvalidChannel(ConnectionError): '''The channel id does not correspond to an existing channel.  Non-fatal.'''

  def __init__(self, **kwargs):
    '''
    Initialize the connection.
    '''
    self._debug = kwargs.get('debug', False)
    self._logger = kwargs.get('logger', root_logger) # TODO: be sure that root logger is the one we want

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
    self._output_buffer = []  # TODO: confirm that this still applies
    self._close_info = {
      'reply_code'    : -1,
      'reply_text'    : 'first connect',
      'class_id'      : -1,
      'method_id'     : -1
    }
    
    self._channels = {
      0 : self.channel(0)
    } 
    
    self._channel_counter = 0
    self._channel_max = 65535

    self._strategy = kwargs.get('connection_strategy')
    if not self._strategy:
      self._strategy = ConnectionStrategy( self, self._host, reconnect_cb = self._reconnect_cb )
    self._strategy.connect()

    self._input_frame_buffer = []
    
  @property
  def logger(self):
    return self._logger
  
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
      self.log("disconnect and we have a socket")
      self._sock.close_cb = None
      self._sock.close()
      self._sock = None
    
    # It's possible that this is being called after we've done a standard socket
    # closure and the strategy is trying to reconnect.  In that case, we might
    # not have a socket anymore but the channels are still around.  
    for channel_id in self._channels.keys():
      if channel_id != self.channel_id: 
        self.log("removing channel with id %s" % channel_id)
        del self._channels[channel_id]
  
  def add_reconnect_callback(self, callback):
    '''Adds a reconnect callback to the strategy.  This can be used to
    resubscribe to exchanges, etc.'''
    self.strategy.reconnect_callbacks.append(callback)

  def __del__(self):
    '''
    When the connection goes out of scope, close it.
    '''
    if not self.closed:
      self.close()

  # NOTE: not sure I want to keep this message, technically logging with (str, *args) should
  # be faster in cases where the logs aren't going to be output.
  def log(self, msg, level=INFO):
    '''
    Log a message.  If it's an exception, a stack trace will be included.
    '''
    if level!=ERROR and level!=CRITICAL:
      self.logger.log( level, msg )
    else:
      self.logger.log( level, msg, exc_info=True )

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
    self.channel_counter += 1
    if self.channel_counter >= self.channel_max:
      self.channel_counter = 1
    return self.channel_counter

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
          len(self._channels), self._channel_max )
      channel_id = self._next_channel_id()
      while channel_id in self._channels:
        channel_id = self._next_channel_id()
    elif channel_id in self._channels:
      return self._channels[channel_id]
    else:
      raise Connect.InvalidChannel("%s is not a valid channel id", channel_id )

    rval = Channel(self, channel_id)
    self._channels[ channel_id ] = rval
    return rval

  def close(self):
    '''
    Close this connection.
    '''
    # TODO: confirm this matches the channel interface
    self._channels[0].connection.close()

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
      
      self.log("ready to read frames")
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

      # DEBUG:
      self.log("--------- buffered frame list --------")
      for frame in self._input_frame_buffer:
        self.log( str(frame) )
      self.log("--------- END ----------") 

      # Even if there was a frame error, process whatever is on the input buffer.
      self._process_input_frames()
    except Exception, e:
      traceback.print_exc()
      print (type(e))
      print e

  def _process_input_frames(self):
    while True:
      try:
        frame = self._input_frame_buffer.pop()
        
        if isinstance(frame, HeartbeatFrame):
          # TODO: Respond
          pass

      except IndexError:
        break;
