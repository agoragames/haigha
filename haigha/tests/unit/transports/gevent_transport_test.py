'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import errno
import gevent
from gevent.coros import Semaphore
from gevent import socket
from gevent.pool import Pool

from haigha.transports import gevent_transport
from haigha.transports.gevent_transport import *

class GeventTransportTest(Chai):

  def setUp(self):
    super(GeventTransportTest,self).setUp()

    self.connection = mock()
    self.transport = GeventTransport(self.connection)
    self.transport._host = 'server:1234'

  def test_init(self):
    assert_equals( bytearray(), self.transport._buffer )
    assert_false( self.transport._synchronous )
    assert_true( isinstance(self.transport._read_lock,Semaphore) )
    assert_true( isinstance(self.transport._write_lock,Semaphore) )

  def test_connect(self):
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.connect ).args(('host','port'), klass=is_arg(socket.socket)).returns( 'somedata' )

    self.transport.connect( ('host','port') )

  def test_read(self):
    self.transport._read_lock = mock()
    expect( self.transport._read_lock.acquire )
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.read ).args(timeout=None).returns( 'somedata' )
    expect( self.transport._read_lock.release )

    assert_equals( 'somedata', self.transport.read() )

  def test_read_when_raises_exception(self):
    self.transport._read_lock = mock()
    expect( self.transport._read_lock.acquire )
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.read ).args(timeout='5').raises(Exception('fail'))
    expect( self.transport._read_lock.release )

    assert_raises(Exception, self.transport.read, timeout='5')

  def test_buffer(self):
    self.transport._read_lock = mock()
    expect( self.transport._read_lock.acquire )
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.buffer ).args('datas')
    expect( self.transport._read_lock.release )

    self.transport.buffer('datas')

  def test_buffer_when_raises_exception(self):
    self.transport._read_lock = mock()
    expect( self.transport._read_lock.acquire )
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.buffer ).args('datas').raises(Exception('fail'))
    expect( self.transport._read_lock.release )

    assert_raises(Exception, self.transport.buffer, 'datas')

  def test_write(self):
    self.transport._write_lock = mock()
    expect( self.transport._write_lock.acquire )
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.write ).args('datas')
    expect( self.transport._write_lock.release )

    self.transport.write('datas')

  def test_write_when_raises_an_exception(self):
    self.transport._write_lock = mock()
    expect( self.transport._write_lock.acquire )
    with expect( mock(gevent_transport,'super') ).args(is_arg(GeventTransport), GeventTransport).returns(mock()) as parent:
      expect( parent.write ).args('datas').raises(Exception('fail'))
    expect( self.transport._write_lock.release )

    assert_raises(Exception, self.transport.write, 'datas')

class GeventPoolTransportTest(Chai):

  def setUp(self):
    super(GeventPoolTransportTest,self).setUp()

    self.connection = mock()
    self.transport = GeventPoolTransport(self.connection)
    self.transport._host = 'server:1234'

  def test_init(self):
    assert_equals( bytearray(), self.transport._buffer )
    assert_true( isinstance(self.transport._read_lock,Semaphore) )
    assert_true( isinstance(self.transport.pool, Pool) )

    trans = GeventPoolTransport(self.connection, pool='inground')
    assert_equals( 'inground', trans.pool )

  def test_process_channels(self):
    chs = [ mock(), mock() ]
    self.transport._pool = mock()

    expect( self.transport._pool.spawn ).args( chs[0].process_frames )
    expect( self.transport._pool.spawn ).args( chs[1].process_frames )

    self.transport.process_channels( chs )
