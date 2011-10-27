'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.transports import Transport

import gevent
from gevent import monkey

import socket

class GeventTransport(Transport):
  '''
  Transport using gevent backend
  '''

  def __init__(self, *args):
    # TODO: init this elsewhere
    super(GeventTransport,self).__init__(*args)
    monkey.patch_all()

    self._buffer = bytearray()
    self._read_lock = gevent.coros.Semaphore()
    self._write_lock = gevent.coros.Semaphore()

  ###
  ### Transport API
  ###
  def connect(self, (host,port)):
    '''
    Connect assuming a host and port tuple.
    '''
    self._host = "%s:%s"%(host,port)
    self._sock = socket.socket()
    self._sock.settimeout( self.connection._connect_timeout )
    if self.connection._sock_opts:
      for k,v in self.connection._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.connect( (host,port) )
    #self._sock.setblocking( False )

  def read(self):
    '''
    Read from the transport. If no data is available, should return None.
    '''
    # NOTE: copying over this comment from Connection, because there is
    # knowledge captured here, even if the details are stale
    # Because of the timer callback to dataRead when we re-buffered, there's a
    # chance that in between we've lost the socket.  If that's the case, just
    # silently return as some code elsewhere would have already notified us.
    # That bug could be fixed by improving the message reading so that we consume
    # all possible messages and ensure that only a partial message was rebuffered,
    # so that we can rely on the next read event to read the subsequent message.
    if self._sock is None:
      return None
    #return self._sock.read()

    self._read_lock.acquire()
    try:
      data = self._sock.recv( self._sock.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF) )

      if len(data):
        if len(self._buffer):
          self._buffer.extend( data )
          data = self._buffer
          self._buffer = bytearray()
        return data
    finally:
      self._read_lock.release()

    self.connection.transport_closed( msg='error reading from socket' )

  def buffer(self, data):
    '''
    Buffer unused bytes from the input stream.
    '''
    if self._sock is None:
      return None

    # data will always be a byte array
    if len(self._buffer):
      self._buffer.extend( data )
    else:
      self._buffer = data

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    self._write_lock.acquire()
    try:
      sent = self._sock.sendall( data )
    finally:
      self._write_lock.release()
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.

    The transport is encouraged to allow for any pending writes to complete
    before closing the socket.
    '''
    self._sock.close()
