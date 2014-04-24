'''
Copyright (c) 2011-2014, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

import warnings

from haigha.transports.socket_transport import SocketTransport

try:
    import gevent
    from gevent.event import Event
    try:
        # Semaphore moved here since gevent-1.0b2
        from gevent.lock import Semaphore
    except ImportError:
        from gevent.coros import Semaphore
    from gevent import socket
    from gevent import pool
except ImportError:
    warnings.warn('Failed to load gevent modules')
    gevent = None
    Event = None
    Semaphore = None
    socket = None
    pool = None


class GeventTransport(SocketTransport):

    '''
    Transport using gevent backend. It relies on gevent's implementation of
    sendall to send whole frames at a time. On the input side, it uses a gevent
    semaphore to ensure exclusive access to the socket and input buffer.
    '''

    def __init__(self, *args, **kwargs):
        super(GeventTransport, self).__init__(*args)

        self._synchronous = False
        self._read_lock = Semaphore()
        self._write_lock = Semaphore()
        self._read_wait = Event()

    ###
    # Transport API
    ###

    def connect(self, (host, port)):
        '''
        Connect using a host,port tuple
        '''
        super(GeventTransport, self).connect((host, port), klass=socket.socket)

    def read(self, timeout=None):
        '''
        Read from the transport. If no data is available, should return None.
        If timeout>0, will only block for `timeout` seconds.
        '''
        # If currently locked, another greenlet is trying to read, so yield
        # control and then return none. Required if a Connection is configured
        # to be synchronous, a sync callback is trying to read, and there's
        # another read loop running read_frames. Without it, the run loop will
        # release the lock but then immediately acquire it again. Yielding
        # control in the reading thread after bytes are read won't fix
        # anything, because it's quite possible the bytes read resulted in a
        # frame that satisfied the synchronous callback, and so this needs to
        # return immediately to first check the current status of synchronous
        # callbacks before attempting to read again.
        if self._read_lock.locked():
            self._read_wait.wait(timeout)
            return None

        self._read_lock.acquire()
        try:
            return super(GeventTransport, self).read(timeout=timeout)
        finally:
            self._read_lock.release()
            self._read_wait.set()
            self._read_wait.clear()

    def buffer(self, data):
        '''
        Buffer unused bytes from the input stream.
        '''
        self._read_lock.acquire()
        try:
            return super(GeventTransport, self).buffer(data)
        finally:
            self._read_lock.release()

    def write(self, data):
        '''
        Write some bytes to the transport.
        '''
        # MUST use a lock here else gevent could raise an exception if 2
        # greenlets try to write at the same time. I was hoping that
        # sendall() would do that blocking for me, but I guess not. May
        # require an eventsocket-like buffer to speed up under high load.
        self._write_lock.acquire()
        try:
            return super(GeventTransport, self).write(data)
        finally:
            self._write_lock.release()


class GeventPoolTransport(GeventTransport):

    def __init__(self, *args, **kwargs):
        super(GeventPoolTransport, self).__init__(*args)

        self._pool = kwargs.get('pool', None)
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
            self._pool.spawn(channel.process_frames)
