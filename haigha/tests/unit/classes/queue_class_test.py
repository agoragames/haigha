'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.classes import queue_class, ProtocolClass, QueueClass
from haigha.frames import MethodFrame
from haigha.writer import Writer

from collections import deque

class QueueClassTest(Chai):

  def setUp(self):
    super(QueueClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = QueueClass( ch )

  def test_init(self):
    expect(ProtocolClass.__init__).args('foo', a='b')
    
    klass = QueueClass.__new__(QueueClass)
    klass.__init__('foo', a='b')

    assert_equals( 
      {
        11 : klass._recv_declare_ok,
        21 : klass._recv_bind_ok,
        31 : klass._recv_purge_ok,
        41 : klass._recv_delete_ok,
        51 : klass._recv_unbind_ok,

      }, klass.dispatch_map )
    assert_equals( deque(), self.klass._declare_cb )
    assert_equals( deque(), self.klass._bind_cb )
    assert_equals( deque(), self.klass._unbind_cb )
    assert_equals( deque(), self.klass._delete_cb )
    assert_equals( deque(), self.klass._purge_cb )

  def test_cleanup(self):
    self.klass._cleanup()
    assert_equals( None, self.klass._declare_cb )
    assert_equals( None, self.klass._bind_cb )
    assert_equals( None, self.klass._unbind_cb )
    assert_equals( None, self.klass._delete_cb )
    assert_equals( None, self.klass._purge_cb )
    assert_equals( None, self.klass._channel )
    assert_equals( None, self.klass.dispatch_map )

  def test_declare_default_args(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bits ).args( False, False, False, True, True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    assert_equals( deque(), self.klass._declare_cb )
    self.klass.declare()
    assert_equals( deque(), self.klass._declare_cb )

  def test_declare_with_args_and_no_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bits ).args( 'p', 'd', 'e', 'a', False ).returns( w )
    expect( w.write_table ).args( {'foo':'bar'} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_declare_ok )

    assert_equals( deque(), self.klass._declare_cb )
    self.klass.declare('queue', passive='p', durable='d', exclusive='e', 
      auto_delete='a', nowait=False, arguments={'foo':'bar'}, ticket='ticket')
    assert_equals( deque([None]), self.klass._declare_cb )

  def test_declare_with_args_and_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bits ).args( 'p', 'd', 'e', 'a', False ).returns( w )
    expect( w.write_table ).args( {'foo':'bar'} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 10, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_declare_ok )

    # assert it's put in the right spot too
    self.klass._declare_cb = deque(['blargh'])
    self.klass.declare('queue', passive='p', durable='d', exclusive='e', 
      auto_delete='a', nowait=True, arguments={'foo':'bar'}, ticket='ticket',
      cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._declare_cb )

  def test_recv_declare_ok_with_callback(self):
    rframe = mock()
    cb = mock()
    self.klass._declare_cb.append( cb )
    self.klass._declare_cb.append( mock() ) # assert not called

    expect( rframe.args.read_shortstr ).returns( 'queue' )
    expect( rframe.args.read_long ).returns( 32 )
    expect( rframe.args.read_long ).returns( 5 )
    expect( cb ).args( 'queue', 32, 5 )

    self.klass._recv_declare_ok( rframe )
    assert_equals( 1, len(self.klass._declare_cb) )
    assert_false( cb in self.klass._declare_cb )

  def test_recv_declare_ok_without_callback(self):
    rframe = mock()
    cb = mock()
    self.klass._declare_cb.append( None )
    self.klass._declare_cb.append( cb )

    expect( rframe.args.read_shortstr ).returns( 'queue' )
    expect( rframe.args.read_long ).returns( 32 )
    expect( rframe.args.read_long ).returns( 5 )

    self.klass._recv_declare_ok( rframe )
    assert_equals( 1, len(self.klass._declare_cb) )
    assert_false( None in self.klass._declare_cb )

  def test_bind_default_args(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bit ).args( True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    assert_equals( deque(), self.klass._declare_cb )
    self.klass.bind('queue', 'exchange')
    assert_equals( deque(), self.klass._declare_cb )

  def test_bind_with_args_and_no_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bit ).args( False ).returns( w )
    expect( w.write_table ).args( {'foo':'bar'} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_bind_ok )

    assert_equals( deque(), self.klass._bind_cb )
    self.klass.bind('queue', 'exchange', routing_key='route', nowait=False,
      arguments={'foo':'bar'}, ticket='ticket')
    assert_equals( deque([None]), self.klass._bind_cb )

  def test_bind_with_args_and_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bit ).args( False ).returns( w )
    expect( w.write_table ).args( {'foo':'bar'} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_bind_ok )

    self.klass._bind_cb = deque(['blargh'])
    self.klass.bind('queue', 'exchange', routing_key='route', nowait=True,
      arguments={'foo':'bar'}, ticket='ticket', cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._bind_cb )

  def test_recv_bind_ok_with_cb(self):
    cb = mock()
    self.klass._bind_cb.append( cb )
    self.klass._bind_cb.append( mock() ) # assert not called

    expect( cb )

    self.klass._recv_bind_ok( 'frame' )
    assert_equals( 1, len(self.klass._bind_cb) )
    assert_false( cb in self.klass._bind_cb )

  def test_recv_bind_ok_without_cb(self):
    self.klass._bind_cb.append( None )
    self.klass._bind_cb.append( mock() ) # assert not called

    self.klass._recv_bind_ok( 'frame' )
    assert_equals( 1, len(self.klass._bind_cb) )
    assert_false( None in self.klass._bind_cb )

  def test_unbind_default_args(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 50, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_unbind_ok )

    assert_equals( deque(), self.klass._unbind_cb )
    self.klass.unbind('queue', 'exchange')
    assert_equals( deque([None]), self.klass._unbind_cb )

  def test_unbind_with_args(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_table ).args( {'foo':'bar'} )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 50, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_unbind_ok )

    self.klass._unbind_cb = deque(['blargh'])
    self.klass.unbind('queue', 'exchange', routing_key='route', 
      arguments={'foo':'bar'}, ticket='ticket', cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._unbind_cb )

  def test_recv_unbind_ok_with_cb(self):
    cb = mock()
    self.klass._unbind_cb.append( cb )
    self.klass._unbind_cb.append( mock() ) # assert not called

    expect( cb )

    self.klass._recv_unbind_ok( 'frame' )
    assert_equals( 1, len(self.klass._unbind_cb) )
    assert_false( cb in self.klass._unbind_cb )

  def test_recv_unbind_ok_without_cb(self):
    self.klass._unbind_cb.append( None )
    self.klass._unbind_cb.append( mock() ) # assert not called

    self.klass._recv_unbind_ok( 'frame' )
    assert_equals( 1, len(self.klass._unbind_cb) )
    assert_false( None in self.klass._unbind_cb )

  def test_purge_default_args(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bit ).args( True )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    assert_equals( deque(), self.klass._purge_cb )
    self.klass.purge('queue')
    assert_equals( deque(), self.klass._purge_cb )

  def test_purge_with_args_and_no_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_purge_ok )

    assert_equals( deque(), self.klass._purge_cb )
    self.klass.purge('queue', nowait=False, ticket='ticket')
    assert_equals( deque([None]), self.klass._purge_cb )

  def test_purge_with_args_and_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_purge_ok )

    self.klass._purge_cb = deque(['blargh'])
    self.klass.purge('queue', nowait=True, ticket='ticket', cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._purge_cb )

  def test_recv_purge_ok_with_cb(self):
    rframe = mock()
    cb = mock()
    self.klass._purge_cb.append( cb )
    self.klass._purge_cb.append( mock() ) # assert not called

    expect( rframe.args.read_long ).returns( 42 )
    expect( cb ).args( 42 )

    self.klass._recv_purge_ok( rframe )
    assert_equals( 1, len(self.klass._purge_cb) )
    assert_false( cb in self.klass._purge_cb )

  def test_recv_purge_ok_without_cb(self):
    rframe = mock()
    self.klass._purge_cb.append( None )
    self.klass._purge_cb.append( mock() ) # assert not called

    expect( rframe.args.read_long ).returns( 42 )

    self.klass._recv_purge_ok( rframe )
    assert_equals( 1, len(self.klass._purge_cb) )
    assert_false( None in self.klass._purge_cb )

  def test_delete_default_args(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bits ).args( False, False, True )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 40, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    stub( self.klass.channel.add_synchronous_cb )

    assert_equals( deque(), self.klass._delete_cb )
    self.klass.delete('queue')
    assert_equals( deque(), self.klass._delete_cb )

  def test_delete_with_args_and_no_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bits ).args( 'yes', 'no', False )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 40, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_delete_ok )

    assert_equals( deque(), self.klass._delete_cb )
    self.klass.delete('queue', if_unused='yes', if_empty='no', nowait=False,
      ticket='ticket')
    assert_equals( deque([None]), self.klass._delete_cb )

  def test_delete_with_args_and_cb(self):
    w = mock()
    expect( mock(queue_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns(w)
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bits ).args( 'yes', 'no', False )
    expect( mock(queue_class, 'MethodFrame') ).args(42, 50, 40, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_delete_ok )

    self.klass._delete_cb = deque(['blargh'])
    self.klass.delete('queue', if_unused='yes', if_empty='no', nowait=True,
      ticket='ticket', cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._delete_cb )

  def test_recv_delete_ok_with_cb(self):
    rframe = mock()
    cb = mock()
    self.klass._delete_cb.append( cb )
    self.klass._delete_cb.append( mock() ) # assert not called

    expect( rframe.args.read_long ).returns( 42 )
    expect( cb ).args( 42 )

    self.klass._recv_delete_ok( rframe )
    assert_equals( 1, len(self.klass._delete_cb) )
    assert_false( cb in self.klass._delete_cb )

  def test_recv_delete_ok_without_cb(self):
    rframe = mock()
    self.klass._delete_cb.append( None )
    self.klass._delete_cb.append( mock() ) # assert not called

    expect( rframe.args.read_long ).returns( 42 )

    self.klass._recv_delete_ok( rframe )
    assert_equals( 1, len(self.klass._delete_cb) )
    assert_false( None in self.klass._delete_cb )
