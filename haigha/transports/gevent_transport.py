'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.transports import Transport

import gevent
from gevent.coros import Semaphore
from gevent import socket
from gevent import pool

#import socket

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
    super(GeventTransport,self).__init__(*args)

    self._buffer = bytearray()
    self._read_lock = Semaphore()

  ###
  ### Transport API
  ###
  def connect(self, (host,port)):
    '''
    Connect assuming a host and port tuple.
    '''
    self._host = "%s:%s"%(host,port)
    self._sock = socket.socket()
    self._sock.setblocking( True )
    self._sock.settimeout( self.connection._connect_timeout )
    if self.connection._sock_opts:
      for k,v in self.connection._sock_opts.iteritems():
        family,type = k
        self._sock.setsockopt(family, type, v)
    self._sock.connect( (host,port) )

    # After connecting, switch to full-blocking mode.
    self._sock.settimeout( None )

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
    if not hasattr(self,'_sock'):
      return None

    self._read_lock.acquire()
    try:
      data = self._sock.recv( self._sock.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF) )

      if len(data):
        if self.connection.debug > 1:
          self.connection.logger.debug( 'read %d bytes from %s'%(len(data), self._host) )
        if len(self._buffer):
          self._buffer.extend( data )
          data = self._buffer
          self._buffer = bytearray()
        return data
        
    finally:
      self._read_lock.release()

    self.connection.transport_closed( msg='error reading from %s'%(self._host) )

  def buffer(self, data):
    '''
    Buffer unused bytes from the input stream.
    '''
    if not hasattr(self,'_sock'):
      return None

    self._read_lock.acquire()
    try:
      # data will always be a byte array
      if len(self._buffer):
        self._buffer.extend( data )
      else:
        self._buffer = bytearray(data)
    finally:
      self._read_lock.release()

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    if not hasattr(self,'_sock'):
      return None

    self._sock.sendall( data )

    if self.connection.debug > 1:
      self.connection.logger.debug( 'sent %d bytes to %s'%(len(data), self._host) )
    
  def disconnect(self):
    '''
    Disconnect from the transport. Typically socket.close(). This call is 
    welcome to raise exceptions, which the Connection will catch.

    The transport is encouraged to allow for any pending writes to complete
    before closing the socket.
    '''
    if not hasattr(self,'_sock'):
      return None

    try:
      self._sock.close()
    finally:
      self._sock = None

class GeventPoolTransport(GeventTransport):

  def __init__(self, *args, **kwargs):
    super(GeventPoolTransport,self).__init__(*args)

    self._pool = kwargs.get('pool',None)
    if not self._pool:
      self._pool = gevent.pool.Pool()

  @property
  def pool(self):
    '''Get a handle to the gevent pool.'''
    return self._pool

  def process_channels(self, channels):
    '''
    Process a set of channels by calling Channel.process_frames() on each. 
    Some transports may choose to do this in unique ways, such as through 
    a pool of threads.

    The default implementation will simply iterate over them and call 
    process_frames() on each.
    '''
    for channel in channels:
      self._pool.spawn( channel.process_frames )
