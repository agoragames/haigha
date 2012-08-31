'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
from collections import deque

from haigha import channel
from haigha.channel import Channel, SyncWrapper
from haigha.exceptions import ChannelError, ChannelClosed
from haigha.classes import *
from haigha.frames import *
from haigha.classes import *

class SyncWrapperTest(Chai):

  def test_init(self):
    s = SyncWrapper('cb')
    assert_equals('cb', s._cb)
    assert_true( s._read )
    assert_equals(None, s._result)

  def test_eq_when_other_is_same_cb(self):
    s = SyncWrapper('cb')
    assert_equals( 'cb', s )
    assert_not_equals( 'bb', s )

  def test_eq_when_other_has_same_cb(self):
    s = SyncWrapper('cb')
    other = SyncWrapper('cb')
    another = SyncWrapper('bb')

    assert_equals( s, other )
    assert_not_equals( s, another )

  def test_call(self):
    cb = mock()
    s = SyncWrapper( cb )

    expect( cb ).args('foo', 'bar', hello='mars')
    s('foo', 'bar', hello='mars')
    assert_false( s._read )
    

class ChannelTest(Chai):

  def test_init(self):
    c = Channel('connection', 'id', {
      20: ChannelClass,
      40: ExchangeClass,
      50: QueueClass,
      60: BasicClass,
      90: TransactionClass,
    })
    self.assertEquals( 'connection', c._connection )
    self.assertEquals( 'id', c._channel_id )
    self.assertTrue( isinstance(c.channel, ChannelClass) )
    self.assertTrue( isinstance(c.exchange, ExchangeClass) )
    self.assertTrue( isinstance(c.queue, QueueClass) )
    self.assertTrue( isinstance(c.basic, BasicClass) )
    self.assertTrue( isinstance(c.tx, TransactionClass) )
    self.assertEquals( c._class_map[20], c.channel )
    self.assertEquals( c._class_map[40], c.exchange )
    self.assertEquals( c._class_map[50], c.queue )
    self.assertEquals( c._class_map[60], c.basic )
    self.assertEquals( c._class_map[90], c.tx )
    self.assertEquals( deque([]), c._pending_events )
    self.assertEquals( deque([]), c._frame_buffer )
    assert_equals( set([]), c._open_listeners )
    assert_equals( set([]), c._close_listeners )
    assert_false( c._closed )
    assert_equals(
      {
        'reply_code'    : 0,
        'reply_text'    : 'first connect',
        'class_id'      : 0,
        'method_id'     : 0
      }, c._close_info )
    assert_true( c._active )

  def test_properties(self):
    connection = mock()
    connection.logger = 'logger'

    c = Channel(connection, 'id', {})
    c._closed = 'yes'
    c._close_info = 'ithappened'
    c._active = 'record'

    assertEquals( connection, c.connection )
    assertEquals( 'id', c.channel_id )
    assertEquals( 'logger', c.logger )
    assert_equals( 'yes', c.closed )
    assert_equals( 'ithappened', c.close_info )
    assert_equals( 'record', c.active )
    
    c._closed = False
    assert_equals( None, c.close_info )

  def test_add_open_listener(self):
    c = Channel(None,None,{})
    c.add_open_listener('foo')
    assert_equals( set(['foo']), c._open_listeners )

  def test_remove_open_listener(self):
    c = Channel(None,None,{})
    c.add_open_listener('foo')
    c.remove_open_listener('foo')
    c.remove_open_listener('bar')
    assert_equals( set([]), c._open_listeners )

  def test_notify_open_listeners(self):
    c = Channel(None,None,{})
    cb1 = mock()
    cb2 = mock()
    c._open_listeners = set([cb1,cb2])
    expect( cb1 ).args( c )
    expect( cb2 ).args( c )
    c._notify_open_listeners()

  def test_add_close_listener(self):
    c = Channel(None,None,{})
    c.add_close_listener('foo')
    assert_equals( set(['foo']), c._close_listeners )

  def test_remove_close_listener(self):
    c = Channel(None,None,{})
    c.add_close_listener('foo')
    c.remove_close_listener('foo')
    c.remove_close_listener('bar')
    assert_equals( set([]), c._close_listeners )

  def test_notify_close_listeners(self):
    c = Channel(None,None,{})
    cb1 = mock()
    cb2 = mock()
    c._close_listeners = set([cb1,cb2])
    expect( cb1 ).args( c )
    expect( cb2 ).args( c )
    c._notify_close_listeners()

  def test_open(self):
    c = Channel(None,None,{})
    expect( mock(c,'channel').open )
    c.open()
 
  def test_active(self):
    c = Channel(None, None,{})
    expect( mock(c,'channel').open )
    c.open()
    assertTrue(c.active)

  def test_close_with_no_args(self):
    c = Channel(None,None,{})
    expect( mock(c,'channel').close ).args( 0, '', 0, 0 )
    c.close()
  
  def test_close_with_args(self):
    c = Channel(None,None,{})
    expect( mock(c,'channel').close ).args(1, 'two', 3, 4)
    expect( c.channel.close ).args(1, 'two', 3, 4)

    c.close(1, 'two', 3, 4)
    c.close(reply_code=1, reply_text='two', class_id=3, method_id=4)

  def test_close_when_channel_attr_cleared(self):
    c = Channel(None,None,{})
    assert_false( hasattr(c, 'channel') )
    c.close()

  def test_publish(self):
    c = Channel(None,None,{})
    expect( mock(c,'basic').publish ).args( 'arg1', 'arg2', foo='bar' )
    c.publish( 'arg1', 'arg2', foo='bar' )

  def test_publish_synchronous(self):
    c = Channel(None,None,{})
    expect( mock(c,'tx').select )
    expect( mock(c,'basic').publish ).args( 'arg1', 'arg2', foo='bar' )
    expect( c.tx.commit ).args( cb='a_cb' )

    c.publish_synchronous( 'arg1', 'arg2', foo='bar', cb='a_cb' )

  def test_dispatch(self):
    c = Channel(None,None,{})
    frame = mock()
    frame.class_id = 32
    klass = mock()
    
    c._class_map[32] = klass
    expect( klass.dispatch ).args( frame )
    c.dispatch( frame )

    frame.class_id = 33
    assert_raises( Channel.InvalidClass, c.dispatch, frame )

  def test_buffer_frame(self):
    c = Channel(None,None,{})
    c.buffer_frame( 'f1' )
    c.buffer_frame( 'f2' )
    assertEquals( deque(['f1', 'f2']), c._frame_buffer )
  
  def test_process_frames_when_no_frames(self):
    # Not that this should ever happen, but to be sure
    c = Channel(None,None,{})
    stub( c.dispatch )
    c.process_frames()

  def test_process_frames_stops_when_buffer_is_empty(self):
    c = Channel(None,None,{})
    f0 = MethodFrame('ch_id', 'c_id', 'm_id')
    f1 = MethodFrame('ch_id', 'c_id', 'm_id')
    c._frame_buffer = deque([ f0, f1 ])

    expect( c.dispatch ).args( f0 )
    expect( c.dispatch ).args( f1 )

    c.process_frames()
    assert_equals( deque(), c._frame_buffer )

  def test_process_frames_stops_when_frameunderflow_raised(self):
    c = Channel(None,None,{})
    f0 = MethodFrame('ch_id', 'c_id', 'm_id')
    f1 = MethodFrame('ch_id', 'c_id', 'm_id')
    c._frame_buffer = deque([ f0, f1 ])

    expect( c.dispatch ).args( f0 ).raises( ProtocolClass.FrameUnderflow )

    c.process_frames()
    assertEquals( f1, c._frame_buffer[0] )
  
  def test_process_frames_logs_and_closes_when_dispatch_error_raised(self):
    c = Channel(None,None,{})
    c._connection = mock()
    c._connection.logger = mock()
    
    f0 = MethodFrame(20, 30, 40)
    f1 = MethodFrame('ch_id', 'c_id', 'm_id')
    c._frame_buffer = deque([ f0, f1 ])

    expect( c.dispatch ).args( f0 ).raises( RuntimeError("zomg it broked") )
    expect( c.close ).args( 500, 'Failed to dispatch %s'%(str(f0)) )

    assert_raises( RuntimeError, c.process_frames )
    assertEquals( f1, c._frame_buffer[0] )
  
  def test_process_frames_logs_and_closes_when_systemexit_raised(self):
    c = Channel(None,None,{})
    c._connection = mock()
    c._connection.logger = mock()
    
    f0 = MethodFrame(20, 30, 40)
    f1 = MethodFrame('ch_id', 'c_id', 'm_id')
    c._frame_buffer = deque([ f0, f1 ])

    expect( c.dispatch ).args( f0 ).raises( SystemExit() )
    stub( c.close )

    assert_raises( SystemExit, c.process_frames )
    assertEquals( f1, c._frame_buffer[0] )

  def test_next_frame_with_a_frame(self):
    c = Channel(None,None,{})
    ch_id, c_id, m_id = 0, 1, 2
    f0 = MethodFrame(ch_id, c_id, m_id)
    f1 = MethodFrame(ch_id, c_id, m_id)
    c._frame_buffer = deque([ f0, f1 ])
    assertEquals(c.next_frame(), f0)

  def test_next_frame_with_no_frames(self):
    c = Channel(None,None,{})
    c._frame_buffer = deque()
    assertEquals(c.next_frame(), None)

  def test_requeue_frames(self):
    c = Channel(None,None,{})
    ch_id, c_id, m_id = 0, 1, 2
    f = [MethodFrame(ch_id, c_id, m_id) for i in xrange(4)]
    c._frame_buffer = deque(f[:2])

    c.requeue_frames(f[2:])
    assertEquals(c._frame_buffer, deque([f[i] for i in [3, 2, 0, 1]]))
  
  def test_send_frame_when_not_closed_no_flow_control_no_pending_events(self):
    conn = mock()
    c = Channel(conn, 32, {})

    expect( conn.send_frame ).args('frame')
    
    c.send_frame( 'frame' )

  def test_send_frame_when_not_closed_no_flow_control_pending_event(self):
    conn = mock()
    c = Channel(conn, 32, {})
    c._pending_events.append( 'cb' )

    c.send_frame( 'frame' )
    self.assertEquals( deque(['cb','frame']), c._pending_events )

  def test_send_frame_when_not_closed_and_flow_control(self):
    conn = mock()
    c = Channel(conn, 32, {})
    c._active = False

    method = MethodFrame(1,2,3)
    heartbeat = HeartbeatFrame()
    header = HeaderFrame(1,2,3,4)
    content = ContentFrame(1,'foo')
    
    expect( conn.send_frame ).args( method )
    expect( conn.send_frame ).args( heartbeat )

    c.send_frame( method )
    c.send_frame( heartbeat )
    self.assertRaises( Channel.Inactive, c.send_frame, header )
    self.assertRaises( Channel.Inactive, c.send_frame, content )

  def test_send_frame_when_closed_for_a_reason(self):
    conn = mock()
    c = Channel(conn, 32, {})
    c._closed = True
    c._close_info = {'reply_code':42, 'reply_text':'bad'}

    assertRaises( ChannelClosed, c.send_frame, 'frame' )
  
  def test_send_frame_when_closed_for_no_reason(self):
    conn = mock()
    c = Channel(conn, 32, {})
    c._closed = True
    c._close_info = {'reply_code':42, 'reply_text':''}

    assertRaises( ChannelClosed, c.send_frame, 'frame' )

  def test_add_synchronous_cb_when_transport_asynchronous(self):
    conn = mock()
    conn.synchronous = False
    c = Channel(conn,None,{})

    assertEquals( deque([]), c._pending_events )
    c.add_synchronous_cb( 'foo' )
    assertEquals( deque(['foo']), c._pending_events )

  def test_add_synchronous_cb_when_transport_synchronous(self):
    conn = mock()
    conn.synchronous = True
    c = Channel(conn,None,{})

    wrapper = mock()
    wrapper._read = True
    wrapper._result = 'done'

    expect( channel.SyncWrapper ).args( 'foo' ).returns( wrapper )
    expect( conn.read_frames )
    expect( conn.read_frames ).side_effect(
      lambda: setattr(wrapper, '_read', False) )
    
    assertEquals( deque([]), c._pending_events )
    assert_equals( 'done', c.add_synchronous_cb('foo') )

    # This is technically cleared in runtime, but assert that it's not cleared
    # in this method
    assertEquals( deque([wrapper]), c._pending_events )

  def test_clear_synchronous_cb_when_no_pending(self):
    c = Channel(None,None,{})
    stub( c._flush_pending_events )

    assertEquals( deque([]), c._pending_events )
    assert_equals( 'foo', c.clear_synchronous_cb('foo') )

  def test_clear_synchronous_cb_when_pending_cb_matches(self):
    c = Channel(None,None,{})
    c._pending_events = deque(['foo'])

    expect( c._flush_pending_events )

    assert_equals( 'foo', c.clear_synchronous_cb('foo') )
    assertEquals( deque([]), c._pending_events )

  def test_clear_synchronous_cb_when_pending_cb_doesnt_match_but_isnt_in_list(self):
    c = Channel(None,None,{})
    c._pending_events = deque(['foo'])

    expect( c._flush_pending_events )

    assert_equals( 'bar', c.clear_synchronous_cb('bar') )
    assertEquals( deque(['foo']), c._pending_events )

  def test_clear_synchronous_cb_when_pending_cb_doesnt_match_but_isnt_in_list(self):
    c = Channel(None,None,{})
    stub( c._flush_pending_events )
    c._pending_events = deque(['foo', 'bar'])

    assertRaises( ChannelError, c.clear_synchronous_cb, 'bar' )
    assertEquals( deque(['foo','bar']), c._pending_events )

  def test_flush_pending_events_flushes_all_leading_frames(self):
    conn = mock()
    c = Channel(conn,42,{})
    f1 = MethodFrame(1,2,3)
    f2 = MethodFrame(1,2,3)
    f3 = MethodFrame(1,2,3)
    c._pending_events = deque([f1, f2, 'cb', f3])

    expect( conn.send_frame ).args( f1 )
    expect( conn.send_frame ).args( f2 )

    c._flush_pending_events()
    assertEquals( deque(['cb',f3]), c._pending_events )

  def test_closed_cb_without_final_frame(self):
    c = Channel('connection',None,{
      20: ChannelClass,
      40: ExchangeClass,
      50: QueueClass,
      60: BasicClass,
      90: TransactionClass,
    })
    c._pending_events = 'foo'
    c._frame_buffer = 'foo'
    
    for val in c._class_map.values():
      expect( val._cleanup )
    expect( c._notify_close_listeners )

    c._closed_cb()
    assert_equals( deque([]), c._pending_events )
    assert_equals( deque([]), c._frame_buffer )
    assert_equals( None, c._connection )
    assert_false( hasattr(c,'channel') )
    assert_false( hasattr(c,'exchange') )
    assert_false( hasattr(c,'queue') )
    assert_false( hasattr(c,'basic') )
    assert_false( hasattr(c,'tx') )
    assert_equals( None, c._class_map )
    assert_equals( set(), c._close_listeners )

  def test_closed_cb_with_final_frame(self):
    conn = mock()
    c = Channel(conn,None,{})

    expect( conn.send_frame ).args('final')    
    for val in c._class_map.values():
      expect( val._cleanup )

    c._closed_cb('final')
