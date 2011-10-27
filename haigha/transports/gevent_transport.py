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
  Transport using gevent backend. It relies on gevent's implementation of 
  sendall to send whole frames at a time. On the input side, it uses a gevent
  semaphore to ensure exclusive access to the socket and input buffer.

  NOTE:
  This is new to haigha and there may be integration issues with some versions
  of RabbitMQ. In particular, what may happen when a blocking call to send a
  frame allows another thread to queue another frame, but the frames can't be 
  interlaced due to how the protocol is defined or implemented. A lot of the
  'synchronous' calls have very specific expectations. If this becomes a
  problem then the quickest way to solve it would be to switch to non-blocking
  so that the same thread of execution is allowed to send all of its frames in
  sequence. In that case, the gevent implementation may be pushed into the
  EventSocket as another supported concurrency lib.

  Note also that the blocking nature of the sockets means that the any threads
  running IO should actively yield with a sleep(0) to ensure other threads are
  serviced. In a saturated environment, failure to do so will lead to 
  significant lags in signal handling or other IO. In practice, a typical
  client is attaching to other data stores so those could be enough to yield.
  '''

  def __init__(self, *args):
    # TODO: init this elsewhere
    super(GeventTransport,self).__init__(*args)
    monkey.patch_all()

    self._buffer = bytearray()
    self._read_lock = gevent.coros.Semaphore()

  ###
  ### Transport API
  ###
  def connect(self, (host,port)):
    '''
    Connect assuming a host and port tuple.
    '''
    self._host = "%s:%s"%(host,port)
    self._sock = gevent.socket.socket()
    self._sock.settimeout( self.connection._connect_timeout )
    if self.connection._sock_opts:
      for k,v in self.connection._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.connect( (host,port) )
    self._sock.setblocking( True )

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

    self._read_lock.acquire()
    try:
      data = self._sock.recv( self._sock.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF) )

      if self.connection.debug: # this is a lot of lookup, and slow
        self.connection.logger.debug( 'read %d bytes from %s'%(len(data), self._host) )
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

    self._read_lock.acquire()
    # data will always be a byte array
    if len(self._buffer):
      self._buffer.extend( data )
    else:
      self._buffer = data
    self._read_lock.release()

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    sent = self._sock.sendall( data )
    if self.connection.debug: # this is a lot of lookup, and slow
      self.connection.logger.debug( 'sent %d bytes to %s'%(len(data), self._host) )
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.

    The transport is encouraged to allow for any pending writes to complete
    before closing the socket.
    '''
    try:
      self._sock.close()
    finally:
      self._sock = None
