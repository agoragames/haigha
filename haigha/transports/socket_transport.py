'''
Copyright (c) 2012, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.transports import Transport

import errno
import socket

class SocketTransport(Transport):
  '''
  A simple blocking socket transport.
  '''

  def __init__(self, *args):
    super(SocketTransport,self).__init__(*args)
    self._synchronous = True
    self._buffer = bytearray()

  ###
  ### Transport API
  ###
  def connect(self, (host,port), klass=socket.socket):
    '''
    Connect assuming a host and port tuple.
    '''
    self._host = "%s:%s"%(host,port)
    self._sock = klass()
    self._sock.setblocking( True )
    self._sock.settimeout( self.connection._connect_timeout )
    if self.connection._sock_opts:
      for k,v in self.connection._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.connect( (host,port) )

    # After connecting, switch to full-blocking mode.
    self._sock.settimeout( None )

  def read(self, timeout=None):
    '''
    Read from the transport. If timeout>0, will only block for `timeout` 
    seconds.
    '''
    if not hasattr(self,'_sock'):
      return None

    try:
      # Note that we ignore both None and 0, i.e. we either block with a
      # timeout or block completely and let gevent sort it out.
      if timeout:
        self._sock.settimeout( timeout )
      else:
        self._sock.settimeout( None )
      data = self._sock.recv( self._sock.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF) )

      if len(data):
        if self.connection.debug > 1:
          self.connection.logger.debug( 'read %d bytes from %s'%(len(data), self._host) )
        if len(self._buffer):
          self._buffer.extend( data )
          data = self._buffer
          self._buffer = bytearray()
        return data

      # Note that no data means the socket is closed and we'll mark that
      # below

    except socket.timeout as e:
      # Note that this is implemented differently and though it would be
      # caught as an EnvironmentError, it has no errno. Not sure whose
      # fault that is.
      return None

    except EnvironmentError as e:
      # thrown if we have a timeout and no data
      if e.errno in (errno.EAGAIN,errno.EWOULDBLOCK):
        return None
      # gevent throws this too, and rather than handle separately just catch
      # that case here
      elif isinstance(e, socket.timeout):
        return None
      
      self.connection.logger.exception( 'error reading from %s'%(self._host) )

    self.connection.transport_closed( msg='error reading from %s'%(self._host) )
  
  def buffer(self, data):
    '''
    Buffer unused bytes from the input stream.
    '''
    if not hasattr(self,'_sock'):
      return None

    # data will always be a byte array
    if len(self._buffer):
      self._buffer.extend( data )
    else:
      self._buffer = bytearray(data)

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    if not hasattr(self,'_sock'):
      return None

    try:
      self._sock.sendall( data )

      if self.connection.debug > 1:
        self.connection.logger.debug( 'sent %d bytes to %s'%(len(data), self._host) )

      return
    except EnvironmentError as e:
      # sockets raise this type of error, and since if sendall() fails we're
      # left in an indeterminate state, assume that any error we catch means
      # that the connection is dead. Note that this assumption requires this
      # to be a blocking socket; if we ever support non-blocking in this class
      # then this whole method has to change a lot.
      self.connection.logger.exception( 'error writing to %s'%(self._host) )
    
    self.connection.transport_closed( msg='error writing to %s'%(self._host) )
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.
    '''
    if not hasattr(self,'_sock'):
      return None

    try:
      self._sock.close()
    finally:
      self._sock = None
