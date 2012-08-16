'''
Copyright (c) 2012, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.transports.socket_transport import SocketTransport

import errno
try:
  import gevent
  from gevent.coros import Semaphore
  from gevent import socket
  from gevent import pool
except ImportError:
  print 'Failed to load gevent modules'
  gevent = None
  Semaphore = None
  socket = None
  pool = None


class GeventTransport(SocketTransport):
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

  def __init__(self, *args, **kwargs):
    super(GeventTransport,self).__init__(*args)

    self._synchronous = kwargs.get('synchronous',False)
    self._read_lock = Semaphore()
    self._write_lock = Semaphore()

  ###
  ### Transport API
  ###

  def connect(self, (host,port)):
    '''
    Connect using a host,port tuple
    '''
    super(GeventTransport,self).connect( (host,port), klass=socket.socket )

  def read(self, timeout=None):
    '''
    Read from the transport. If no data is available, should return None. If
    timeout>0, will only block for `timeout` seconds.
    '''
    self._read_lock.acquire()
    try:
      return super(GeventTransport,self).read(timeout=timeout)
    finally:
      self._read_lock.release()

  def buffer(self, data):
    '''
    Buffer unused bytes from the input stream.
    '''
    self._read_lock.acquire()
    try:
      return super(GeventTransport,self).buffer(data)
    finally:
      self._read_lock.release()

  def write(self, data):
    '''
    Write some bytes to the transport.
    '''
    # MUST use a lock here else gevent could raise an exception if 2 greenlets
    # try to write at the same time. I was hoping that sendall() would do that
    # blocking for me, but I guess not. May require an eventsocket-like buffer
    # to speed up under high load.
    self._write_lock.acquire()
    try:
      return super(GeventTransport,self).write(data)
    finally:
      self._write_lock.release()


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
