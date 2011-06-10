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
    self.klass._pending_consumers = deque(['blargh','consumer'])
    
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
    self.klass._cancel_cb = deque([ mock(), cancel_cb ])
    expect( cancel_cb )

    self.klass._recv_cancel_ok( frame )
    assert_equals( 1,len(self.klass._cancel_cb) )
    assert_false( cancel_cb in self.klass._cancel_cb )

  def test_recv_cancel_ok_when_no_consumer_or_callback(self):
    frame = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    expect( self.klass.logger.warning ).args( 
      'no callback registered for consumer tag " %s "', 'ctag' )
    self.klass._cancel_cb = deque( [ mock(), None ] )

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
    self.klass.publish(msg, 'exchange', 'routing_key', ticket=0)

  def test_publish_with_ticket(self):
    args = Writer()
    msg = Message('hello, world')
    args.write_short(3)\
        .write_shortstr('exchange')\
        .write_shortstr('routing_key')\
        .write_bits(False, False)
    expect( mock(basic_class, 'MethodFrame') ).args(42, 60, 40, args).returns( 'methodframe' )
    expect( mock(basic_class, 'HeaderFrame') ).args(42, 60, 0, len(msg), msg.properties).returns( 'headerframe' )
    self.klass.channel.connection.frame_max = 3
    expect(mock(basic_class, 'ContentFrame').create_frames)\
      .args(42, msg.body, 3).returns(['f0', 'f1', 'f2'])
    expect(self.klass.send_frame).args('methodframe')
    expect(self.klass.send_frame).args('headerframe')
    expect(self.klass.send_frame).args('f0')
    expect(self.klass.send_frame).args('f1')
    expect(self.klass.send_frame).args('f2')
    self.klass.publish(msg, 'exchange', 'routing_key', ticket=3)

  def test_return_msg(self):
    method_frame = mock()
    args = Writer()
    args.write_short(3)
    args.write_shortstr('reply_text')
    args.write_shortstr('exchange')
    args.write_shortstr('routing_key')
    expect(mock(basic_class, 'MethodFrame')).args(42, 60, 50, args).returns(method_frame)
    expect(self.klass.send_frame).args(method_frame)
    self.klass.return_msg(3, 'reply_text', 'exchange', 'routing_key')

  def test__recv_return(self):
    pass

  def test__recv_deliver_no_frames(self):
    expect(self.klass.channel.next_frame).returns(None)
    expect(self.klass.channel.requeue_frames).args(['method_frame'])
    assert_raises(self.klass.FrameUnderflow, self.klass._recv_deliver, 'method_frame')

  def test__recv_deliver_underflow(self):
    header_frame = mock()
    header_frame.size = 1000000
    expect(self.klass.channel.next_frame).returns(header_frame)
    expect(self.klass.channel.next_frame).returns(None)
    expect(self.klass.channel.requeue_frames).args(deque([header_frame, 'method_frame']))
    assert_raises(self.klass.FrameUnderflow, self.klass._recv_deliver, 'method_frame')

  def test__recv_deliver_no_cb(self):
    i = []
    def f():
      i.append(0)

    args = mock()
    method_frame = mock()
    header_frame = mock()
    header_frame.size = 100
    header_frame.properties = {}
    frame = mock()
    delivery_info = {'channel': self.klass.channel,
                     'consumer_tag': 'consumer_tag',
                     'delivery_tag': 9,
                     'redelivered': False,
                     'exchange': 'exchange',
                     'routing_key': 'routing_key'}
    expect(self.klass.channel.next_frame).returns(header_frame)
    expect(self.klass.channel.next_frame).returns(frame)
    expect(frame.payload.buffer).returns('x'*100)
    expect(method_frame.args.read_shortstr).returns('consumer_tag')
    expect(method_frame.args.read_longlong).returns(9)
    expect(method_frame.args.read_bit).returns(False)
    expect(method_frame.args.read_shortstr).returns('exchange')
    expect(method_frame.args.read_shortstr).returns('routing_key')
    expect(mock(basic_class, 'Message')).args(body=bytearray('x'*100), delivery_info=delivery_info, **header_frame.properties)
    self.klass._recv_deliver(method_frame)
