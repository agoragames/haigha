'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.classes import transaction_class, ProtocolClass, TransactionClass
from haigha.frames import MethodFrame
from haigha.writer import Writer

from collections import deque

class TransactionClassTest(Chai):

  def setUp(self):
    super(TransactionClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = TransactionClass( ch )

  def test_init(self):
    expect(ProtocolClass.__init__).args('foo', a='b' )
    
    klass = TransactionClass.__new__(TransactionClass)
    klass.__init__('foo', a='b')

    assert_equals( 
      {
        11 : klass._recv_select_ok,
        21 : klass._recv_commit_ok,
        31 : klass._recv_rollback_ok,
      }, klass.dispatch_map )
    assert_false( klass._enabled )
    assert_equals( deque(), klass._select_cb )
    assert_equals( deque(), klass._commit_cb )
    assert_equals( deque(), klass._rollback_cb )

  def test_cleanup(self):
    self.klass._cleanup()
    assert_equals( None, self.klass._select_cb )
    assert_equals( None, self.klass._commit_cb )
    assert_equals( None, self.klass._rollback_cb )
    assert_equals( None, self.klass._channel )
    assert_equals( None, self.klass.dispatch_map )

  def test_properties(self):
    self.klass._enabled = 'maybe'
    assert_equals( 'maybe', self.klass.enabled )

  def test_select_when_not_enabled_and_no_cb(self):
    self.klass._enabled = False
    expect( mock(transaction_class, 'MethodFrame') ).args(42, 90, 10).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_select_ok )

    self.klass.select()
    assert_true( self.klass.enabled )
    assert_equals( deque([None]), self.klass._select_cb )

  def test_select_when_not_enabled_with_cb(self):
    self.klass._enabled = False
    expect( mock(transaction_class, 'MethodFrame') ).args(42, 90, 10).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_select_ok )

    self.klass.select(cb='foo')
    assert_true( self.klass.enabled )
    assert_equals( deque(['foo']), self.klass._select_cb )

  def test_select_when_already_enabled(self):
    self.klass._enabled = True
    stub( self.klass.send_frame )

    assert_equals( deque(), self.klass._select_cb )
    self.klass.select()
    assert_equals( deque(), self.klass._select_cb )

  def test_recv_select_ok_with_cb(self):
    cb = mock()
    self.klass._select_cb.append( cb )
    self.klass._select_cb.append( mock() )
    expect(cb)
    self.klass._recv_select_ok( 'frame' )
    assert_equals( 1, len(self.klass._select_cb) )
    assert_false( cb in self.klass._select_cb )

  def test_recv_select_ok_without_cb(self):
    self.klass._select_cb.append( None )
    self.klass._select_cb.append( mock() )
    
    self.klass._recv_select_ok( 'frame' )
    assert_equals( 1, len(self.klass._select_cb) )
    assert_false( None in self.klass._select_cb )

  def test_commit_when_enabled_no_cb(self):
    self.klass._enabled = True
    
    expect( mock(transaction_class, 'MethodFrame') ).args(42, 90, 20).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_commit_ok )

    assert_equals( deque(), self.klass._commit_cb )
    self.klass.commit()
    assert_equals( deque([None]), self.klass._commit_cb )

  def test_commit_when_enabled_with_cb(self):
    self.klass._enabled = True
    
    expect( mock(transaction_class, 'MethodFrame') ).args(42, 90, 20).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_commit_ok )

    self.klass._commit_cb = deque(['blargh'])
    self.klass.commit(cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._commit_cb )

  def test_commit_raises_transactionsnotenabled_when_not_enabled(self):
    self.klass._enabled = False
    assert_raises( TransactionClass.TransactionsNotEnabled, self.klass.commit )

  def test_recv_commit_ok_with_cb(self):
    cb = mock()
    self.klass._commit_cb.append( cb )
    self.klass._commit_cb.append( mock() )
    expect(cb)
    
    self.klass._recv_commit_ok('frame')
    assert_equals( 1, len(self.klass._commit_cb) )
    assert_false( cb in self.klass._commit_cb )

  def test_recv_commit_ok_without_cb(self):
    self.klass._commit_cb.append( None )
    self.klass._commit_cb.append( mock() )
    
    self.klass._recv_commit_ok('frame')
    assert_equals( 1, len(self.klass._commit_cb) )
    assert_false( None in self.klass._commit_cb )

  def test_rollback_when_enabled_no_cb(self):
    self.klass._enabled = True
    
    expect( mock(transaction_class, 'MethodFrame') ).args(42, 90, 30).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_rollback_ok )

    assert_equals( deque(), self.klass._rollback_cb )
    self.klass.rollback()
    assert_equals( deque([None]), self.klass._rollback_cb )

  def test_rollback_when_enabled_with_cb(self):
    self.klass._enabled = True
    
    expect( mock(transaction_class, 'MethodFrame') ).args(42, 90, 30).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_rollback_ok )

    self.klass._rollback_cb = deque(['blargh'])
    self.klass.rollback(cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._rollback_cb )

  def test_rollback_raises_transactionsnotenabled_when_not_enabled(self):
    self.klass._enabled = False
    assert_raises( TransactionClass.TransactionsNotEnabled, self.klass.rollback )

  def test_recv_rollback_ok_with_cb(self):
    cb = mock()
    self.klass._rollback_cb.append( cb )
    self.klass._rollback_cb.append( mock() )
    expect(cb)
    
    self.klass._recv_rollback_ok('frame')
    assert_equals( 1, len(self.klass._rollback_cb) )
    assert_false( cb in self.klass._rollback_cb )

  def test_recv_rollback_ok_without_cb(self):
    self.klass._rollback_cb.append( None )
    self.klass._rollback_cb.append( mock() )
    
    self.klass._recv_rollback_ok('frame')
    assert_equals( 1, len(self.klass._rollback_cb) )
    assert_false( None in self.klass._rollback_cb )
