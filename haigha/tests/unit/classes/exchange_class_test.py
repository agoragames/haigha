'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''
from collections import deque

from chai import Chai

from haigha.classes import exchange_class, ProtocolClass, ExchangeClass
from haigha.frames import MethodFrame
from haigha.writer import Writer

class ExchangeClassTest(Chai):

  def setUp(self):
    super(ExchangeClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = ExchangeClass( ch )
  
  def test_init(self):
    expect(ProtocolClass.__init__).args('foo', a='b' )
    
    klass = ExchangeClass.__new__(ExchangeClass)
    klass.__init__('foo', a='b')

    assert_equals( 
      {
        11 : klass._recv_declare_ok,
        21 : klass._recv_delete_ok,
      }, klass.dispatch_map )
    assert_equals( deque(), klass._declare_cb )
    assert_equals( deque(), klass._delete_cb )

  def test_cleanup(self):
    self.klass._cleanup()
    assert_equals( None, self.klass._declare_cb )
    assert_equals( None, self.klass._delete_cb )
    assert_equals( None, self.klass._channel )
    assert_equals( None, self.klass.dispatch_map )

  def test_declare_default_args(self):
    w = mock()
    expect( mock(exchange_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'topic' ).returns( w )
    expect( w.write_bits ).args( False, False, True, False, True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(exchange_class, 'MethodFrame') ).args(42, 40, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    self.klass.declare('exchange', 'topic')
    assert_equals( deque(), self.klass._declare_cb )

  def test_declare_with_args(self):
    w = mock()
    expect( mock(exchange_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'topic' ).returns( w )
    expect( w.write_bits ).args( 'p', 'd', 'a', 'i', False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( mock(exchange_class, 'MethodFrame') ).args(42, 40, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_declare_ok )

    self.klass.declare('exchange', 'topic', passive='p', durable='d', 
      auto_delete='a', internal='i', nowait=False, arguments='table', ticket='t')
    assert_equals( deque([None]), self.klass._declare_cb )

  def test_declare_with_cb(self):
    w = mock()
    expect( mock(exchange_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'topic' ).returns( w )
    expect( w.write_bits ).args( 'p', 'd', 'a', 'i', False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( mock(exchange_class, 'MethodFrame') ).args(42, 40, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_declare_ok )

    self.klass.declare('exchange', 'topic', passive='p', durable='d', 
      auto_delete='a', internal='i', nowait=True, arguments='table', ticket='t',
      cb='foo')
    assert_equals( deque(['foo']), self.klass._declare_cb )
  
  def test_recv_declare_ok_no_cb(self):
    self.klass._declare_cb = deque([None])
    self.klass._recv_declare_ok('frame')
    assert_equals( deque(), self.klass._declare_cb )
  
  def test_recv_declare_ok_with_cb(self):
    cb = mock()
    self.klass._declare_cb = deque([cb])
    expect( cb )
    self.klass._recv_declare_ok('frame')
    assert_equals( deque(), self.klass._declare_cb )

  def test_delete_default_args(self):
    w = mock()
    expect( mock(exchange_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_bits ).args( False, True )
    expect( mock(exchange_class, 'MethodFrame') ).args(42, 40, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    self.klass.delete('exchange')
    assert_equals( deque(), self.klass._delete_cb )

  def test_delete_with_args(self):
    w = mock()
    expect( mock(exchange_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_bits ).args( 'maybe', False )
    expect( mock(exchange_class, 'MethodFrame') ).args(42, 40, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_delete_ok )

    self.klass.delete('exchange', if_unused='maybe', nowait=False, ticket='t')
    assert_equals( deque([None]), self.klass._delete_cb )

  def test_delete_with_cb(self):
    w = mock()
    expect( mock(exchange_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_bits ).args( 'maybe', False )
    expect( mock(exchange_class, 'MethodFrame') ).args(42, 40, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_delete_ok )

    self.klass.delete('exchange', if_unused='maybe', nowait=True, ticket='t', cb='foo')
    assert_equals( deque(['foo']), self.klass._delete_cb )

  def test_recv_delete_ok_no_cb(self):
    self.klass._delete_cb = deque([None])
    self.klass._recv_delete_ok('frame')
    assert_equals( deque(), self.klass._delete_cb )

  def test_recv_delete_ok_with_cb(self):
    cb = mock()
    self.klass._delete_cb = deque([cb])
    expect( cb )
    self.klass._recv_delete_ok('frame')
    assert_equals( deque(), self.klass._delete_cb )
