from chai import Chai

from haigha.classes import basic_class, ProtocolClass, BasicClass
from haigha.frames import MethodFrame
from haigha.writer import Writer
from haigha.message import Message
from haigha.connection import Connection

class BasicClassTest(Chai):

  def setUp(self):
    super(BasicClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = BasicClass( ch )
    self.sample_tag = 'Consumer Tag'
  
  def _BasicClass(self):
    # generator for instances
    ch = mock()
    ch.channel_id = 42
    return BasicClass( ch )

  def test_init(self):
    expect( ProtocolClass.__init__ ).args( 'args' )
    c = BasicClass( 'args' )

    assert_equals( 0, c._consumer_tag_id )
    assert_equals( [], c._pending_consumers )
    assert_equals( {}, c._consumer_cb )
    assert_equals( [], c._get_cb )
    assert_equals( [], c._recover_cb )
    assert_equals( [], c._cancel_cb )

  def test_generate_consumer_tag(self):
    assert_equals( 0, self.klass._consumer_tag_id )
    assert_equals( 'channel-42-1', self.klass._generate_consumer_tag() )
    assert_equals( 1, self.klass._consumer_tag_id )

  def test_qos_default_args(self):
    writer = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( writer )
    expect( writer.write_long ).args( 0 )
    expect( writer.write_short ).args( 0 )
    expect( writer.write_bit ).args( False )
    expect( mock( basic_class, 'MethodFrame' ) ).args( self.klass.channel_id, 60, 10, writer ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_qos_ok )

    self.klass.qos()

  def test_qos_with_args(self):
    writer = mock()
    expect( mock( basic_class, 'Writer' ) ).returns( writer )
    expect( writer.write_long ).args( 1 )
    expect( writer.write_short ).args( 2 )
    expect( writer.write_bit ).args( 3 )
    expect( mock( basic_class, 'MethodFrame' ) ).args( self.klass.channel_id, 60, 10, writer ).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_qos_ok )

    self.klass.qos(1, 2, 3)

  def test_recv_qos_ok(self):
    self.klass._recv_qos_ok( 'frame' )

  def test_consume_default_args(self):
    writer = mock()
    expect( self.klass._generate_consumer_tag ).returns( 'ctag' )
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_short ).args( self.klass.default_ticket )
    expect( writer.write_shortstr ).args( 'queue' )
    expect( writer.write_shortstr ).args( 'ctag' )
    expect( writer.write_bits ).args( False, True, False, True )
    expect( writer.write_table ).args( {} )
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 20, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    #expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_consume_ok )

    assert_equals( [], self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )
    self.klass.consume( 'queue', 'consumer' )
    assert_equals( [], self.klass._pending_consumers )
    assert_equals( {'ctag':'consumer'}, self.klass._consumer_cb )

  def test_consume_with_args_including_nowait_and_ticket(self):
    writer = mock()
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_short ).args( 'train' )
    expect( writer.write_shortstr ).args( 'queue' )
    expect( writer.write_shortstr ).args( 'stag' )
    expect( writer.write_bits ).args( 'nloc', 'nack', 'mine', False )
    expect( writer.write_table ).args( {} )
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 20, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_consume_ok )

    assert_equals( [], self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )
    self.klass.consume( 'queue', 'consumer', consumer_tag='stag', no_local='nloc',
      no_ack='nack', exclusive='mine', nowait=False, ticket='train' )
    assert_equals( ['consumer'], self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )

  def test_consume_with_args_including_nowait_no_ticket(self):
    writer = mock()
    stub( self.klass._generate_consumer_tag )
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_short ).args( self.klass.default_ticket )
    expect( writer.write_shortstr ).args( 'queue' )
    expect( writer.write_shortstr ).args( 'stag' )
    expect( writer.write_bits ).args( 'nloc', 'nack', 'mine', False )
    expect( writer.write_table ).args( {} )
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 20, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_consume_ok )

    assert_equals( [], self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )
    self.klass.consume( 'queue', 'consumer', consumer_tag='stag', no_local='nloc',
      no_ack='nack', exclusive='mine', nowait=False )
    assert_equals( ['consumer'], self.klass._pending_consumers )
    assert_equals( {}, self.klass._consumer_cb )

  def test_recv_consume_ok(self):
    frame = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    self.klass._pending_consumers = ['consumer']
    
    assert_equals( {}, self.klass._consumer_cb )
    self.klass._recv_consume_ok( frame )
    assert_equals( {'ctag':'consumer'}, self.klass._consumer_cb )
    assert_equals( [], self.klass._pending_consumers )

  def test_cancel_default_args0(self):
    writer = mock()
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    expect(self.klass.send_frame).args('frame')
    expect(self.klass.logger.warning).args(ignore(), ignore())
    self.klass.cancel()

  def test_cancel_consumer_tag(self):
    writer = mock()
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    expect(self.klass.send_frame).args('frame')
    expect(self.klass.logger.warning).args(ignore(), ignore())
    self.klass.cancel(self.sample_tag)

  def test_cancel_existing_consumer_tag(self):
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    self.klass._consumer_cb[self.sample_tag] = object()
    expect(self.klass.send_frame).args('frame')
    self.klass.cancel(self.sample_tag)
    assertTrue(self.sample_tag not in self.klass._consumer_cb)

  def test_cancel_consumer_no_cb(self):
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    expect(self.klass.send_frame).args('frame')
    expect(self.klass.logger.warning).args(ignore(), ignore())
    self.klass.cancel(consumer=object())

  def test_cancel_consumer_with_cb(self):
    consumer = object()
    self.klass._consumer_cb[self.sample_tag] = consumer
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    expect(self.klass.send_frame).args('frame')
    self.klass.cancel(consumer_tag=self.sample_tag, consumer=consumer)
    assertTrue(self.sample_tag not in self.klass._consumer_cb)
    assertTrue(consumer not in self.klass._consumer_cb.values())

  def test_cancel_nowait_false(self):
    f = lambda:None
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    expect(self.klass.send_frame).args('frame')
    expect(self.klass.channel.add_synchronous_cb).args(self.klass._recv_cancel_ok)
    self.klass.cancel(nowait=False, cb=f)
    assertTrue(f in self.klass._cancel_cb)

  def test_cancel_nowait_false_no_cb(self):
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 30, ignore()).returns( 'frame' )
    expect(self.klass.send_frame).args('frame')
    expect(self.klass.channel.add_synchronous_cb).args(self.klass._recv_cancel_ok)
    self.klass.cancel(nowait=False)
    assertTrue(None in self.klass._cancel_cb)

  def test__recv_cancel_ok(self):
    frame = mock()
    expect(frame.args.read_shortstr).returns(self.sample_tag)
    expect(self.klass.logger.warning).args(ignore(), ignore())
    self.klass._recv_cancel_ok(frame)

  def test__recv_cancel_ok_with_cb(self):
    # python sucks
    i = []
    def f():
      i.append(0)

    frame = mock()
    self.klass._consumer_cb[self.sample_tag] = f
    self.klass._cancel_cb.append(f)
    expect(frame.args.read_shortstr).returns(self.sample_tag)
    assertEquals(i, [])
    self.klass._recv_cancel_ok(frame)
    assertEquals(i, [0])
    assertTrue(f not in self.klass._consumer_cb)
    assertTrue(f not in self.klass._cancel_cb)

  def test_cancel_default_args1(self):
    writer = mock()
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_shortstr ).args( '' )
    expect( writer.write_bit ).args( True )
    
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 30, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    
    self.klass._consumer_cb[ '' ] = 'foo'
    assert_equals( [], self.klass._cancel_cb )
    self.klass.cancel()
    assert_equals( [], self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_nowait_and_consumer_tag_not_registered(self):
    writer = mock()
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_shortstr ).args( 'ctag' )
    expect( writer.write_bit ).args( True )
    
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 30, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    expect( self.klass.logger.warning ).args( 
      'no callback registered for consumer tag " %s "', 'ctag' )
    
    assert_equals( [], self.klass._cancel_cb )
    self.klass.cancel( consumer_tag='ctag' )
    assert_equals( [], self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_wait(self):
    writer = mock()
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_shortstr ).args( '' )
    expect( writer.write_bit ).args( False )
    
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 30, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_cancel_ok )
    
    assert_equals( [], self.klass._cancel_cb )
    self.klass.cancel( nowait=False )
    assert_equals( [None], self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_wait_with_user_cb(self):
    writer = mock()
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_shortstr ).args( '' )
    expect( writer.write_bit ).args( False )
    
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 30, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_cancel_ok )
    
    assert_equals( [], self.klass._cancel_cb )
    self.klass.cancel( nowait=False, cb='user_cb' )
    assert_equals( ['user_cb'], self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_cancel_resolves_to_ctag_when_consumer_arg_supplied(self):
    writer = mock()
    expect( mock(basic_class, 'Writer') ).returns( writer )
    expect( writer.write_shortstr ).args( 'ctag' )
    expect( writer.write_bit ).args( True )
    
    expect( mock(basic_class,'MethodFrame') ).args(self.klass.channel_id, 60, 30, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass._consumer_cb[ 'ctag' ] = 'consumer'
    assert_equals( [], self.klass._cancel_cb )
    self.klass.cancel( consumer='consumer' )
    assert_equals( [], self.klass._cancel_cb )
    assert_equals( {}, self.klass._consumer_cb )

  def test_recv_cancel_ok_when_consumer_and_callback(self):
    frame = mock()
    cancel_cb = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    self.klass._consumer_cb['ctag'] = 'foo'
    self.klass._cancel_cb = [ cancel_cb ]
    expect( cancel_cb )

    self.klass._recv_cancel_ok( frame )

  def test_recv_cancel_ok_when_no_consumer_or_callback(self):
    frame = mock()
    expect( frame.args.read_shortstr ).returns( 'ctag' )
    expect( self.klass.logger.warning ).args( 
      'no callback registered for consumer tag " %s "', 'ctag' )
    self.klass._cancel_cb = [ None ]

    self.klass._recv_cancel_ok( frame )

  def publish_helper(self, ticket):
    args = Writer()
    exchange = 'exchange'
    routing_key = 'routing_key'
    msg = Message('hello, world')
    args.write_short(ticket if ticket is not None else self.klass.default_ticket)\
        .write_shortstr(exchange)\
        .write_shortstr(routing_key)\
        .write_bits(False, False)
    expect( mock(basic_class, 'MethodFrame') ).args(self.klass.channel_id, 60, 40, args).returns( 'methodframe' )
    expect( mock(basic_class, 'HeaderFrame') ).args(self.klass.channel_id, 60, 0, len(msg), msg.properties).returns( 'headerframe' )
    frame_max = 3
    self.klass.channel.connection.frame_max = frame_max
    frames = ['f0', 'f1', 'f2']
    expect(mock(basic_class, 'ContentFrame').create_frames).args(self.klass.channel_id, msg.body, frame_max).returns(frames)
    expect(self.klass.send_frame).args('methodframe')
    expect(self.klass.send_frame).args('headerframe')
    for frame in frames:
      expect(self.klass.send_frame).args(frame)
    self.klass.publish(msg, exchange, routing_key, ticket=ticket)

  def test_publish_default_args(self):
    self.publish_helper(None)

  def test_publish_with_ticket(self):
    self.publish_helper(3)
