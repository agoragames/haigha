'''
Copyright (c) 2012, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from collections import deque
import logging
from chai import Chai

from haigha.connections import rabbit_connection
from haigha.connections.rabbit_connection import *
from haigha.connection import Connection
from haigha.writer import Writer
from haigha.frames import *
from haigha.classes import *

class RabbitConnectionTest(Chai):

  def test_init(self):
    with expect( mock(rabbit_connection, 'super') ).args(is_arg(RabbitConnection), RabbitConnection).returns(mock()) as c:
      expect( c.__init__ ).args(class_map=var('classes'), foo='bar')

    rc = RabbitConnection(foo='bar')
    assert_equals( var('classes').value, {
      40 : RabbitExchangeClass,
      60 : RabbitBasicClass,
      85 : RabbitConfirmClass
    } )

class RabbitExchangeClassTest(Chai):

  def setUp(self):
    super(RabbitExchangeClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = RabbitExchangeClass( ch )

  def test_init(self):
    assert_equals( self.klass.dispatch_map[31], self.klass._recv_bind_ok )
    assert_equals( self.klass.dispatch_map[51], self.klass._recv_unbind_ok )
    assert_equals( deque(), self.klass._bind_cb )
    assert_equals( deque(), self.klass._unbind_cb )

  def test_declare_default_args(self):
    w = mock()
    expect( self.klass.allow_nowait ).returns( True )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'topic' ).returns( w )
    expect( w.write_bits ).args( False, False, True, False, True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    self.klass.declare('exchange', 'topic')
    assert_equals( deque(), self.klass._declare_cb )

  def test_declare_with_args(self):
    w = mock()
    stub( self.klass.allow_nowait )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'topic' ).returns( w )
    expect( w.write_bits ).args( 'p', 'd', 'ad', 'yes', False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_declare_ok )

    self.klass.declare('exchange', 'topic', passive='p', durable='d', 
      nowait=False, arguments='table', ticket='t', 
      auto_delete='ad', internal='yes')
    assert_equals( deque([None]), self.klass._declare_cb )

  def test_declare_with_cb(self):
    w = mock()
    expect( self.klass.allow_nowait ).returns( True )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'topic' ).returns( w )
    expect( w.write_bits ).args( 'p', 'd', True, False, False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_declare_ok )

    self.klass.declare('exchange', 'topic', passive='p', durable='d', 
      nowait=True, arguments='table', ticket='t', cb='foo')
    assert_equals( deque(['foo']), self.klass._declare_cb )

  def test_bind_default_args(self):
    w = mock()
    
    expect( self.klass.allow_nowait ).returns( True )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 0 ).returns( w )
    expect( w.write_shortstr ).args( 'destination' ).returns( w )
    expect( w.write_shortstr ).args( 'source' ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bit ).args( True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.bind('destination', 'source')
    assert_equals( deque(), self.klass._bind_cb )

  def test_bind_with_args(self):
    w = mock()
    
    stub( self.klass.allow_nowait )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'destination' ).returns( w )
    expect( w.write_shortstr ).args( 'source' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bit ).args( False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_bind_ok )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.bind('destination', 'source', routing_key='route', 
      ticket='t', nowait=False, arguments='table')
    assert_equals( deque([None]), self.klass._bind_cb )

  def test_bind_with_cb(self):
    w = mock()
    
    expect( self.klass.allow_nowait ).returns( True )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'destination' ).returns( w )
    expect( w.write_shortstr ).args( 'source' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bit ).args( False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_bind_ok )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.bind('destination', 'source', routing_key='route', 
      ticket='t', arguments='table', cb='foo')
    assert_equals( deque(['foo']), self.klass._bind_cb )
  
  def test_recv_bind_ok_no_cb(self):
    self.klass._bind_cb = deque([None])
    self.klass._recv_bind_ok('frame')
    assert_equals( deque(), self.klass._bind_cb )
  
  def test_recv_bind_ok_with_cb(self):
    cb = mock()
    self.klass._bind_cb = deque([cb])
    expect( cb )
    self.klass._recv_bind_ok('frame')
    assert_equals( deque(), self.klass._bind_cb )

  def test_unbind_default_args(self):
    w = mock()
    
    expect( self.klass.allow_nowait ).returns( True )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 0 ).returns( w )
    expect( w.write_shortstr ).args( 'destination' ).returns( w )
    expect( w.write_shortstr ).args( 'source' ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bit ).args( True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 40, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.unbind('destination', 'source')
    assert_equals( deque(), self.klass._unbind_cb )

  def test_unbind_with_args(self):
    w = mock()
    
    stub( self.klass.allow_nowait )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'destination' ).returns( w )
    expect( w.write_shortstr ).args( 'source' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bit ).args( False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_unbind_ok )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 40, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.unbind('destination', 'source', routing_key='route', 
      ticket='t', nowait=False, arguments='table')
    assert_equals( deque([None]), self.klass._unbind_cb )

  def test_unbind_with_cb(self):
    w = mock()
    
    expect( self.klass.allow_nowait ).returns( True )
    expect( mock(rabbit_connection, 'Writer') ).returns( w )
    expect( w.write_short ).args( 't' ).returns(w)
    expect( w.write_shortstr ).args( 'destination' ).returns( w )
    expect( w.write_shortstr ).args( 'source' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bit ).args( False ).returns( w )
    expect( w.write_table ).args( 'table' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_unbind_ok )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 40, 40, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.unbind('destination', 'source', routing_key='route', 
      ticket='t', arguments='table', cb='foo')
    assert_equals( deque(['foo']), self.klass._unbind_cb )
  
  def test_recv_unbind_ok_no_cb(self):
    self.klass._unbind_cb = deque([None])
    self.klass._recv_unbind_ok('frame')
    assert_equals( deque(), self.klass._unbind_cb )
  
  def test_recv_unbind_ok_with_cb(self):
    cb = mock()
    self.klass._unbind_cb = deque([cb])
    expect( cb )
    self.klass._recv_unbind_ok('frame')
    assert_equals( deque(), self.klass._unbind_cb )

class RabbitBasicClassTest(Chai):

  def setUp(self):
    super(RabbitBasicClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = RabbitBasicClass( ch )

  def test_init(self):
    assert_equals( self.klass.dispatch_map[80], self.klass._recv_ack )
    assert_equals( self.klass.dispatch_map[120], self.klass._recv_nack )
    assert_equals( None, self.klass._ack_listener )
    assert_equals( None, self.klass._nack_listener )
    assert_equals( 0, self.klass._msg_id )
    assert_equals( 0, self.klass._last_ack_id )

  def test_set_ack_listener(self):
    self.klass.set_ack_listener('foo')
    assert_equals( 'foo', self.klass._ack_listener )

  def test_set_nack_listener(self):
    self.klass.set_nack_listener('foo')
    assert_equals( 'foo', self.klass._nack_listener )

  def test_publish_when_not_confirming(self):
    self.klass.channel.confirm._enabled = False
    with expect( mock(rabbit_connection, 'super') ).args(
    is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
      expect( klass.publish ).args( 'a', 'b', c='d' )

    assert_equals( 0, self.klass.publish('a','b', c='d') )
    assert_equals( 0, self.klass._msg_id )

  def test_publish_when_confirming(self):
    self.klass.channel.confirm._enabled = True
    with expect( mock(rabbit_connection, 'super') ).args(
    is_arg(RabbitBasicClass), RabbitBasicClass).returns(mock()) as klass:
      expect( klass.publish ).args( 'a', 'b', c='d' )

    assert_equals( 1, self.klass.publish('a','b', c='d') )
    assert_equals( 1, self.klass._msg_id )

  def test_recv_ack_no_listener(self):
    self.klass._recv_ack('frame')

  def test_recv_ack_with_listener_single_msg(self):
    self.klass._ack_listener = mock()
    frame = mock()
    expect( frame.args.read_longlong ).returns( 42 )
    expect( frame.args.read_bit ).returns( False )
    expect( self.klass._ack_listener ).args( 42 )
    
    self.klass._recv_ack( frame )
    assert_equals( 42, self.klass._last_ack_id )

  def test_recv_ack_with_listener_multiple_msg(self):
    self.klass._ack_listener = mock()
    self.klass._last_ack_id = 40
    frame = mock()
    expect( frame.args.read_longlong ).returns( 42 )
    expect( frame.args.read_bit ).returns( True )
    expect( self.klass._ack_listener ).args( 41 )
    expect( self.klass._ack_listener ).args( 42 )
    
    self.klass._recv_ack( frame )
    assert_equals( 42, self.klass._last_ack_id )
  
  def test_nack_default_args(self):
    w = mock()
    expect( mock( rabbit_connection, 'Writer' ) ).returns( w )
    expect( w.write_longlong ).args( 8675309 ).returns( w )
    expect( w.write_bits ).args( False, False )
    expect( mock( rabbit_connection, 'MethodFrame' ) ).args( 42, 60, 120, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.nack( 8675309 )
  
  def test_nack_with_args(self):
    w = mock()
    expect( mock( rabbit_connection, 'Writer' ) ).returns( w )
    expect( w.write_longlong ).args( 8675309 ).returns( w )
    expect( w.write_bits ).args( 'many', 'sure' )
    expect( mock( rabbit_connection, 'MethodFrame' ) ).args( 42, 60, 120, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.nack( 8675309, multiple='many', requeue='sure' )

  def test_recv_nack_no_listener(self):
    self.klass._recv_nack('frame')

  def test_recv_nack_with_listener_single_msg(self):
    self.klass._nack_listener = mock()
    frame = mock()
    expect( frame.args.read_longlong ).returns( 42 )
    expect( frame.args.read_bits ).args(2).returns( (False,False) )
    expect( self.klass._nack_listener ).args( 42, False )
    
    self.klass._recv_nack( frame )
    assert_equals( 42, self.klass._last_ack_id )

  def test_recv_nack_with_listener_multiple_msg(self):
    self.klass._nack_listener = mock()
    self.klass._last_ack_id = 40
    frame = mock()
    expect( frame.args.read_longlong ).returns( 42 )
    expect( frame.args.read_bits ).args(2).returns( (True,True) )
    expect( self.klass._nack_listener ).args( 41, True )
    expect( self.klass._nack_listener ).args( 42, True )
    
    self.klass._recv_nack( frame )
    assert_equals( 42, self.klass._last_ack_id )

class RabbitConfirmClassTest(Chai):

  def setUp(self):
    super(RabbitConfirmClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = RabbitConfirmClass( ch )

  def test_init(self):
    assert_equals( {11:self.klass._recv_select_ok}, self.klass.dispatch_map )
    assert_false( self.klass._enabled )
    assert_equals( deque(), self.klass._select_cb )

  def test_name(self):
    assert_equals( 'confirm', self.klass.name )

  def test_select_when_not_enabled_and_no_cb(self):
    self.klass._enabled = False
    w = mock()
    expect( mock( rabbit_connection, 'Writer' ) ).returns( w )
    expect( self.klass.allow_nowait ).returns( True )
    expect( w.write_bit ).args( True )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 85, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    self.klass.select()
    assert_true( self.klass._enabled )
    assert_equals( deque(), self.klass._select_cb )

  def test_select_when_not_enabled_and_no_cb_but_synchronous(self):
    self.klass._enabled = False
    w = mock()
    expect( mock( rabbit_connection, 'Writer' ) ).returns( w )
    stub( self.klass.allow_nowait )
    expect( w.write_bit ).args( False )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 85, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_select_ok )

    self.klass.select(nowait=False)
    assert_true( self.klass._enabled )
    assert_equals( deque([None]), self.klass._select_cb )

  def test_select_when_not_enabled_with_cb(self):
    self.klass._enabled = False
    w = mock()
    expect( mock( rabbit_connection, 'Writer' ) ).returns( w )
    expect( self.klass.allow_nowait ).returns( True )
    expect( w.write_bit ).args( False )
    expect( mock(rabbit_connection, 'MethodFrame') ).args(42, 85, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_select_ok )

    self.klass.select(cb='foo')
    assert_true( self.klass._enabled )
    assert_equals( deque(['foo']), self.klass._select_cb )

  def test_select_when_already_enabled(self):
    self.klass._enabled = True
    stub( self.klass.allow_nowait )
    stub( self.klass.send_frame )
    expect( self.klass.allow_nowait ).returns( True )

    assert_equals( deque(), self.klass._select_cb )
    self.klass.select()
    assert_equals( deque(), self.klass._select_cb )
  
  def test_recv_select_ok_no_cb(self):
    self.klass._select_cb = deque([None])
    self.klass._recv_select_ok('frame')
    assert_equals( deque(), self.klass._select_cb )
  
  def test_recv_select_ok_with_cb(self):
    cb = mock()
    self.klass._select_cb = deque([cb])
    expect( cb )
    self.klass._recv_select_ok('frame')
    assert_equals( deque(), self.klass._select_cb )
