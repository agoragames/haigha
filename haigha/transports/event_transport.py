'''
Copyright (c) 2011-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.transports import Transport

try:
  from eventsocket import EventSocket
  import event
except ImportError:
  print 'Failed to load EventSocket and event modules'
  EventSocket = None
  event = None

class EventTransport(Transport):
  '''
  Transport using libevent-based EventSocket.
  '''
  
  def __init__(self, *args):
    super(EventTransport,self).__init__(*args)
    self._synchronous = False

  ###
  ### EventSocket callbacks
  ###
  def _sock_close_cb(self, sock):
    self._connection.transport_closed(
      msg='socket to %s closed unexpectedly'%(self._host),
    )

  def _sock_error_cb(self, sock, msg, exception=None):
    self._connection.transport_closed(
      msg='error on connection to %s: %s'%(self._host, msg)
    )

  def _sock_read_cb(self, sock):
    self.connection.read_frames()
  
  ###
  ### Transport API
  ###
  def connect(self, (host,port)):
    '''
    Connect assuming a host and port tuple. Implemented as non-blocking, and
    will close the transport if there's an error
    '''
    self._host = "%s:%s"%(host,port)
    self._sock = EventSocket(
      read_cb=self._sock_read_cb,
      close_cb=self._sock_close_cb, 
      error_cb=self._sock_error_cb,
      debug=self.connection.debug, 
      logger=self.connection.logger )
    if self.connection._sock_opts:
      for k,v in self.connection._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.setblocking( False )
    self._sock.connect( (host,port), timeout=self.connection._connect_timeout )
    self._heartbeat_timeout = None

  def read(self, timeout=None):
    '''
    Read from the transport. If no data is available, should return None. The
    timeout is ignored as this returns only data that has already been buffered
    locally.
    '''
    # NOTE: copying over this comment from Connection, because there is
    # knowledge captured here, even if the details are stale
    # Because of the timer callback to dataRead when we re-buffered, there's a
    # chance that in between we've lost the socket.  If that's the case, just
    # silently return as some code elsewhere would have already notified us.
    # That bug could be fixed by improving the message reading so that we consume
    # all possible messages and ensure that only a partial message was rebuffered,
    # so that we can rely on the next read event to read the subsequent message.
    if not hasattr(self,'_sock'):
      return None

    # This is sort of a hack because we're faking that data is ready, but it
    # works for purposes of supporting timeouts
    if timeout:
      if self._heartbeat_timeout:
        self._heartbeat_timeout.delete()
      self._heartbeat_timeout = \
        event.timeout( timeout, self._sock_read_cb, self._sock )
    elif self._heartbeat_timeout:
      self._heartbeat_timeout.delete()
      self._heartbeat_timeout = None
      
    return self._sock.read()

  def buffer(self, data):
    '''
    Buffer unused bytes from the input stream.
    '''
    if not hasattr(self,'_sock'):
      return None
    self._sock.buffer( data )

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    if not hasattr(self,'_sock'):
      return
    self._sock.write( data )
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.

    The transport is encouraged to allow for any pending writes to complete
    before closing the socket.
    '''
    if not hasattr(self,'_sock'):
      return

    # TODO: If there are bytes left on the output, queue the close for later.
    self._sock.close_cb = None
    self._sock.close()
