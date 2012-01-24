'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.classes import basic_class, ProtocolClass, BasicClass
from haigha.frames import MethodFrame
from haigha.writer import Writer
from haigha.reader import Reader
from haigha.message import Message
from haigha.connection import Connection

from collections import deque

class BasicClassTest(Chai):

  def setUp(self):
    super(BasicClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = BasicClass( ch )
  
  def test_init(self):
    expect(ProtocolClass.__init__).args('foo', a='b' )

    klass = BasicClass.__new__(BasicClass)
    klass.__init__('foo', a='b')

    assert_equals(
      {
        11 : klass._recv_qos_ok,
        21 : klass._recv_consume_ok,
        31 : klass._recv_cancel_ok,
        50 : klass._recv_return,
        60 : klass._recv_deliver,
        71 : klass._recv_get_response,
        72 : klass._recv_get_response,
        111 : klass._recv_recover_ok,
      }, klass.dispatch_map )
    assert_equals( 0, klass._consumer_tag_id )
    assert_equals( deque(), klass._pending_consumers )
    assert_equals( {}, klass._consumer_cb )
    assert_equals( deque(), klass._get_cb )
    assert_equals( deque(), klass._recover_cb )
    assert_equals( deque(), klass._cancel_cb )

  def test_cleanup(self):
    self.klass._cleanup()
    assert_equals( None, self.klass._pending_consumers )
    assert_equals( None, self.klass._consumer_cb )
    assert_equals( None, self.klass._get_cb )
    assert_equals( None, self.klass._recover_cb )
    assert_equals( None, self.klass._cancel_cb )
    assert_equals( None, self.klass._channel )
    assert_equals( None, self.klass.dispatch_map )

  def test_generate_consumer_tag(self):
    assert_equals( 0, self.klass._consumer_tag_id )
    assert_equals( 'channel-42-1', self.klass._generate_consumer_tag() )
    assert_equals( 1, self.klass._consumer_tag_id )

  def test_qos_default_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_long ).args( 0 ).returns( w )
    expect( w.write_short ).args( 0 ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 10, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_qos_ok )

    self.klass.qos()

  def test_qos_with_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_long ).args( 1 ).returns( w )
    expect( w.write_short ).args( 2 ).returns( w )
    expect( w.write_bit ).args( 3 )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 10, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_qos_ok )

    self.klass.qos(prefetch_size=1, prefetch_count=2, is_global=3)

  def test_recv_qos_ok(self):
    self.klass._recv_qos_ok( 'frame' )

  def test_consume_default_args(self):
    w = mock()
    expect( self.klass._generate_consumer_tag ).returns( 'ctag' )
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns( w )
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'ctag' ).returns( w )
    expect( w.write_bits ).args( False, True, False, True ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    assert_equals( deque(), self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )
    self.klass.consume( 'queue', 'consumer' )
    assert_equals( deque(), self.klass._pending_consumers )
    assert_equals( {'ctag':'consumer'}, self.klass._consumer_cb )

  def test_consume_with_args_including_nowait_and_ticket(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'train' ).returns( w )
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'stag' ).returns( w )
    expect( w.write_bits ).args( 'nloc', 'nack', 'mine', False ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_consume_ok )

    assert_equals( deque(), self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )
    self.klass.consume( 'queue', 'consumer', consumer_tag='stag', no_local='nloc',
      no_ack='nack', exclusive='mine', nowait=False, ticket='train' )
    assert_equals( deque(['consumer']), self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )

  def test_consume_with_args_including_nowait_no_ticket(self):
    w = mock()
    stub( self.klass._generate_consumer_tag )
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns( w )
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_shortstr ).args( 'stag' ).returns( w )
    expect( w.write_bits ).args( 'nloc', 'nack', 'mine', False ).returns( w )
    expect( w.write_table ).args( {} )
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 20, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_consume_ok )

    self.klass._pending_consumers = deque(['blargh'])
    assert_equals( {}, self.klass._consumer_cb )
    self.klass.consume( 'queue', 'consumer', consumer_tag='stag', no_local='nloc',
      no_ack='nack', exclusive='mine', nowait=False )
    assert_equals( deque(['blargh','consumer']), self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )

  def test_recv_consume_ok(self):
    frame = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    self.klass._pending_consumers = deque(['consumer', 'blargh'])
    
    assert_equals( {}, self.klass._consumer_cb )
    self.klass._recv_consume_ok( frame )
    assert_equals( {'ctag':'consumer'}, self.klass._consumer_cb )
    assert_equals( deque(['blargh']), self.klass._pending_consumers )

  def test_cancel_default_args(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bit ).args( True )
    
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    
    self.klass._consumer_cb[ '' ] = 'foo'
    assert_equals( deque(), self.klass._cancel_cb )
    self.klass.cancel()
    assert_equals( deque(), self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_nowait_and_consumer_tag_not_registered(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_shortstr ).args( 'ctag' ).returns( w )
    expect( w.write_bit ).args( True )
    
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    expect( self.klass.logger.warning ).args( 
      'no callback registered for consumer tag " %s "', 'ctag' )
    
    assert_equals( deque(), self.klass._cancel_cb )
    self.klass.cancel( consumer_tag='ctag' )
    assert_equals( deque(), self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_wait_without_cb(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bit ).args( False )
    
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_cancel_ok )
    
    assert_equals( deque(), self.klass._cancel_cb )
    self.klass.cancel( nowait=False )
    assert_equals( deque([None]), self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_wait_with_cb(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_shortstr ).args( '' ).returns( w )
    expect( w.write_bit ).args( False )
    
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_cancel_ok )
    
    self.klass._cancel_cb = deque(['blargh'])
    self.klass.cancel( nowait=False, cb='user_cb' )
    assert_equals( deque(['blargh','user_cb']), self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_resolves_to_ctag_when_consumer_arg_supplied(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_shortstr ).args( 'ctag' ).returns( w )
    expect( w.write_bit ).args( True )
    
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 30, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass._consumer_cb[ 'ctag' ] = 'consumer'
    assert_equals( deque(), self.klass._cancel_cb )
    self.klass.cancel( consumer='consumer' )
    assert_equals( deque(), self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_recv_cancel_ok_when_consumer_and_callback(self):
    frame = mock()
    cancel_cb = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    self.klass._consumer_cb['ctag'] = 'foo'
    self.klass._cancel_cb = deque([ cancel_cb, mock() ])
    expect( cancel_cb )

    self.klass._recv_cancel_ok( frame )
    assert_equals( 1,len(self.klass._cancel_cb) )
    assert_false( cancel_cb in self.klass._cancel_cb )

  def test_recv_cancel_ok_when_no_consumer_or_callback(self):
    frame = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    expect( self.klass.logger.warning ).args( 
      'no callback registered for consumer tag " %s "', 'ctag' )
    self.klass._cancel_cb = deque( [ None, mock() ] )

    self.klass._recv_cancel_ok( frame )
    assert_equals( 1, len(self.klass._cancel_cb) )
    assert_false( None in self.klass._cancel_cb  )

  def test_publish_default_args(self):
    args = Writer()
    msg = Message('hello, world')
    args.write_short(0)\
        .write_shortstr('exchange')\
        .write_shortstr('routing_key')\
        .write_bits(False, False)
    self.klass.channel.connection.frame_max = 3
    
    expect( mock(basic_class, 'MethodFrame') ).args(42, 60, 40, args).returns( 'methodframe' )
    expect( mock(basic_class, 'HeaderFrame') ).args(42, 60, 0, len(msg), msg.properties).returns( 'headerframe' )
    expect(mock(basic_class, 'ContentFrame').create_frames).args(42, msg.body, 3).returns(['f0', 'f1', 'f2'])
    expect(self.klass.send_frame).args('methodframe')
    expect(self.klass.send_frame).args('headerframe')
    expect(self.klass.send_frame).args('f0')
    expect(self.klass.send_frame).args('f1')
    expect(self.klass.send_frame).args('f2')
    self.klass.publish(msg, 'exchange', 'routing_key')

  def test_publish_with_args(self):
    w = mock()
    msg = Message('hello, world')
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns( w )
    expect( w.write_shortstr ).args( 'exchange' ).returns( w )
    expect( w.write_shortstr ).args( 'route' ).returns( w )
    expect( w.write_bits ).args( 'm','i' )
    self.klass.channel.connection.frame_max = 3
    
    expect( mock(basic_class, 'MethodFrame') ).args(42, 60, 40, w).returns( 'methodframe' )
    expect( mock(basic_class, 'HeaderFrame') ).args(42, 60, 0, len(msg), msg.properties).returns( 'headerframe' )
    expect(mock(basic_class, 'ContentFrame').create_frames).args(42, msg.body, 3).returns(['f0', 'f1', 'f2'])
    expect(self.klass.send_frame).args('methodframe')
    expect(self.klass.send_frame).args('headerframe')
    expect(self.klass.send_frame).args('f0')
    expect(self.klass.send_frame).args('f1')
    expect(self.klass.send_frame).args('f2')

    self.klass.publish(msg, 'exchange', 'route', mandatory='m', immediate='i', ticket='ticket' )

  def test_return_msg(self):
    args = Writer()
    args.write_short(3)
    args.write_shortstr('reply_text')
    args.write_shortstr('exchange')
    args.write_shortstr('routing_key')
    expect(mock(basic_class, 'MethodFrame')).args(42, 60, 50, args).returns('frame')
    expect(self.klass.send_frame).args('frame')
    self.klass.return_msg(3, 'reply_text', 'exchange', 'routing_key')

  def test_recv_return(self):
    self.klass._recv_return( 'frame' )

  def test_recv_deliver_with_cb(self):
    msg = mock()
    msg.delivery_info = {'consumer_tag':'ctag'}
    cb = mock()
    self.klass._consumer_cb['ctag'] = cb

    expect( self.klass._read_msg ).args( 'frame').returns( msg )
    expect( cb ).args( msg )

    self.klass._recv_deliver('frame')

  def test_recv_deliver_without_cb(self):
    msg = mock()
    msg.delivery_info = {'consumer_tag':'ctag'}

    expect( self.klass._read_msg ).args( 'frame').returns( msg )

    self.klass._recv_deliver('frame')

  def test_get_default_args(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( self.klass.default_ticket ).returns( w )
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bit ).args( True )
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 70, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_get_response )

    assert_equals( deque(), self.klass._get_cb )
    self.klass.get('queue', 'consumer')
    assert_equals( deque(['consumer']), self.klass._get_cb )

  def test_get_with_args(self):
    w = mock()
    expect( mock(basic_class, 'Writer') ).returns( w )
    expect( w.write_short ).args( 'ticket' ).returns( w )
    expect( w.write_shortstr ).args( 'queue' ).returns( w )
    expect( w.write_bit ).args( 'ack' )
    expect( mock(basic_class,'MethodFrame') ).args(42, 60, 70, w).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_get_response )

    self.klass._get_cb = deque(['blargh'])
    self.klass.get('queue', 'consumer', no_ack='ack', ticket='ticket')
    assert_equals( deque(['blargh','consumer']), self.klass._get_cb )

  def test_recv_get_response(self):
    frame = mock()
    frame.method_id = 71
    expect( self.klass._recv_get_ok ).args( frame )
    self.klass._recv_get_response(frame)

    frame.method_id = 72
    expect( self.klass._recv_get_empty ).args( frame )
    self.klass._recv_get_response(frame)

  def test_recv_get_ok_with_cb(self):
    cb = mock()
    self.klass._get_cb.append( cb )
    self.klass._get_cb.append( mock() )

    expect( self.klass._read_msg ).args( 'frame' ).returns( 'msg' )
    expect( cb ).args( 'msg' )

    self.klass._recv_get_ok( 'frame' )
    assert_equals( 1, len(self.klass._get_cb) )
    assert_false( cb in self.klass._get_cb )

  def test_recv_get_ok_without_cb(self):
    self.klass._get_cb.append( None )
    self.klass._get_cb.append( mock() )

    expect( self.klass._read_msg ).args( 'frame' ).returns( 'msg' )

    self.klass._recv_get_ok( 'frame' )
    assert_equals( 1, len(self.klass._get_cb) )
    assert_false( None in self.klass._get_cb )

  def test_recv_get_empty_with_cb(self):
    cb = mock()
    self.klass._get_cb.append( cb )
    self.klass._get_cb.append( mock() )

    expect( cb ).args( None )

    self.klass._recv_get_empty( 'frame' )
    assert_equals( 1, len(self.klass._get_cb) )
    assert_false( cb in self.klass._get_cb )

  def test_recv_get_empty_without_cb(self):
    self.klass._get_cb.append( None )
    self.klass._get_cb.append( mock() )

    self.klass._recv_get_empty( 'frame' )
    assert_equals( 1, len(self.klass._get_cb) )
    assert_false( None in self.klass._get_cb )
  
  def test_ack_default_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_longlong ).args( 8675309 ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 80, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.ack( 8675309 )
  
  def test_ack_with_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_longlong ).args( 8675309 ).returns( w )
    expect( w.write_bit ).args( 'many' )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 80, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.ack( 8675309, multiple='many' )
  
  def test_reject_default_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_longlong ).args( 8675309 ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 90, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.reject( 8675309 )
  
  def test_reject_with_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_longlong ).args( 8675309 ).returns( w )
    expect( w.write_bit ).args( 'sure' )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 90, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.reject( 8675309, requeue='sure' )

  def test_recover_async(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 100, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass.recover_async()

  def test_recover_default_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_bit ).args( False )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 110, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_recover_ok )

    self.klass.recover()
    assert_equals( deque([None]), self.klass._recover_cb )

  def test_recover_with_args(self):
    w = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( w )
    expect( w.write_bit ).args( 'requeue' )
    expect( mock( basic_class, 'MethodFrame' ) ).args( 42, 60, 110, w ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_recover_ok )

    self.klass._recover_cb = deque(['blargh'])
    self.klass.recover(requeue='requeue', cb='callback')
    assert_equals( deque(['blargh','callback']), self.klass._recover_cb )

  def test_recv_recover_ok_with_cb(self):
    cb = mock()
    self.klass._recover_cb.append( cb )
    self.klass._recover_cb.append( mock() )

    expect( cb )

    self.klass._recv_recover_ok( 'frame' )
    assert_equals( 1, len(self.klass._recover_cb) )
    assert_false( cb in self.klass._recover_cb )

  def test_recv_recover_ok_without_cb(self):
    self.klass._recover_cb.append( None )
    self.klass._recover_cb.append( mock() )

    self.klass._recv_recover_ok( 'frame' )
    assert_equals( 1, len(self.klass._recover_cb) )
    assert_false( None in self.klass._recover_cb )
    
  def test_read_msg_raises_frameunderflow_when_no_header_frame(self):
    expect(self.klass.channel.next_frame).returns(None)
    expect(self.klass.channel.requeue_frames).args(['method_frame'])
    assert_raises(self.klass.FrameUnderflow, self.klass._read_msg, 'method_frame')
  
  def test_read_msg_raises_frameunderflow_when_no_content_frames(self):
    header_frame = mock()
    header_frame.size = 1000000
    expect(self.klass.channel.next_frame).returns(header_frame)
    expect(self.klass.channel.next_frame).returns(None)
    expect(self.klass.channel.requeue_frames).args(deque([header_frame, 'method_frame']))
    assert_raises(self.klass.FrameUnderflow, self.klass._read_msg, 'method_frame')

  def test_read_msg_when_body_length_0_no_cb(self):
    method_frame = mock()
    header_frame = mock()
    header_frame.size = 0
    header_frame.properties = {'foo':'bar'}
    delivery_info = {'channel': self.klass.channel,
                     'consumer_tag': 'consumer_tag',
                     'delivery_tag': 9,
                     'redelivered': False,
                     'exchange': 'exchange',
                     'routing_key': 'routing_key'}

    expect(self.klass.channel.next_frame).returns(header_frame)
    expect(method_frame.args.read_shortstr).returns('consumer_tag')
    expect(method_frame.args.read_longlong).returns(9)
    expect(method_frame.args.read_bit).returns(False)
    expect(method_frame.args.read_shortstr).returns('exchange')
    expect(method_frame.args.read_shortstr).returns('routing_key')
    expect(mock(basic_class, 'Message')).args(
      body=bytearray(), delivery_info=delivery_info, foo='bar').returns('message')

    assert_equals( 'message', self.klass._read_msg(method_frame) )

  def test_read_msg_when_body_length_greater_than_0_with_cb(self):
    method_frame = mock()
    header_frame = mock()
    header_frame.size = 100
    header_frame.properties = {}
    cframe1 = mock()
    cframe2 = mock()
    self.klass._consumer_cb['ctag'] = mock()
    delivery_info = {'channel': self.klass.channel,
                     'consumer_tag': 'ctag',
                     'delivery_tag': 'dtag',
                     'redelivered': 'no',
                     'exchange': 'exchange',
                     'routing_key': 'routing_key'}

    expect(self.klass.channel.next_frame).returns(header_frame)
    expect(self.klass.channel.next_frame).returns(cframe1)
    expect(cframe1.payload.buffer).returns('x'*50)
    expect(self.klass.channel.next_frame).returns(cframe2)
    expect(cframe2.payload.buffer).returns('x'*50)
    expect(method_frame.args.read_shortstr).returns('ctag')
    expect(method_frame.args.read_longlong).returns('dtag')
    expect(method_frame.args.read_bit).returns('no')
    expect(method_frame.args.read_shortstr).returns('exchange')
    expect(method_frame.args.read_shortstr).returns('routing_key')
    expect(mock(basic_class, 'Message')).args(
      body=bytearray('x'*100), delivery_info=delivery_info).returns('message')

    assert_equals( 'message', self.klass._read_msg(method_frame) )
