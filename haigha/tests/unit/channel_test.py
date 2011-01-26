
import mox

from haigha.channel import Channel
from haigha.exceptions import ChannelError, ChannelClosed
from haigha.classes import *
from haigha.frames import *

class ChannelTest(mox.MoxTestBase):

  def test_init(self):
    c = Channel('connection', 'id')
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
    self.assertEquals( [], c._pending_events )
    self.assertEquals( [], c._frame_buffer )

  def test_properties(self):
    connection = self.create_mock_anything()
    connection.logger = 'logger'

    c = Channel(connection, 'id')
    c.channel = self.create_mock_anything()
    c.channel.closed = 'closed'
    c.channel.close_info = 'uwerebad'

    self.assertEquals( connection, c.connection )
    self.assertEquals( 'id', c.channel_id )
    self.assertEquals( 'logger', c.logger )
    self.assertEquals( 'closed', c.closed )
    self.assertEquals( 'uwerebad', c.close_info )

  def test_open(self):
    c = Channel(None,None)
    self.mock( c.channel, 'open' )
    c.channel.open()

    self.replay_all()
    c.open()

  def test_close_with_no_args(self):
    c = Channel(None,None)
    self.mock( c.channel, 'close' )
    c.channel.close(0, '', 0, 0)

    self.replay_all()
    c.close()
  
  def test_close_with_args(self):
    c = Channel(None,None)
    self.mock( c.channel, 'close' )
    c.channel.close(1, 'two', 3, 4)
    c.channel.close(1, 'two', 3, 4)

    self.replay_all()
    c.close(1, 'two', 3, 4)
    c.close(reply_code=1, reply_text='two', class_id=3, method_id=4)

  def test_publish(self):
    c = Channel(None,None)
    self.mock( c.basic, 'publish' )
    c.basic.publish( 'arg1', 'arg2', foo='bar' )

    self.replay_all()
    c.publish( 'arg1', 'arg2', foo='bar' )

  def test_publish_synchronous(self):
    c = Channel(None,None)
    self.mock( c.tx, 'select' )
    self.mock( c.basic, 'publish' )
    self.mock( c.tx, 'commit' )

    c.tx.select()
    c.basic.publish( 'arg1', 'arg2', foo='bar' )
    c.tx.commit( cb='a_cb' )

    self.replay_all()
    c.publish_synchronous( 'arg1', 'arg2', foo='bar', cb='a_cb' )

  def test_buffer_frame(self):
    c = Channel(None,None)
    self.replay_all()
    c.buffer_frame( 'f1' )
    c.buffer_frame( 'f2' )
    self.assertEquals( ['f1', 'f2'], c._frame_buffer )

  def test_process_frames_when_no_frames(self):
    # Not that this should ever happen, but to be sure
    c = Channel(None,None)
    self.mock( c, 'dispatch' )

    self.replay_all()
    c.process_frames()

  def test_process_frames_when_just_a_method_frame(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    frame = MethodFrame('ch_id', 'c_id', 'm_id')
    c._frame_buffer = [ frame ]

    c.dispatch( frame )

    self.replay_all()
    c.process_frames()
    self.assertEquals( [], c._frame_buffer )

  def test_process_frames_when_just_a_heartbeat_frame(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    frame = HeartbeatFrame()
    c._frame_buffer = [ frame ]

    c.dispatch( frame )

    self.replay_all()
    c.process_frames()
    self.assertEquals( [], c._frame_buffer )

  def test_process_frames_when_just_a_header_or_content_frame(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)
    self.mock( c, 'dispatch' )
    c._frame_buffer = [ HeaderFrame(1,2,3,4), ContentFrame(1, 'foo') ]

    conn.close(505, mox.IsA(str))
    conn.close(505, mox.IsA(str))

    self.replay_all()
    c.process_frames()
    c.process_frames()

  def test_process_frames_when_includes_just_a_header_frame(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    method_frame = MethodFrame('ch_id', 'c_id', 'm_id')
    header_frame = HeaderFrame('ch_id', 'c_id', 0, 5)
    c._frame_buffer = [ method_frame, header_frame ]

    self.replay_all()
    c.process_frames()
    self.assertEquals( [method_frame, header_frame], c._frame_buffer )

  def test_process_frames_when_includes_all_content(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    method_frame = MethodFrame('ch_id', 'c_id', 'm_id')
    header_frame = HeaderFrame('ch_id', 'c_id', 0, 5)
    content_frame = ContentFrame('ch_id', 'hello')
    c._frame_buffer = [ method_frame, header_frame, content_frame ]

    c.dispatch( method_frame, header_frame, content_frame )

    self.replay_all()
    c.process_frames()
    self.assertEquals( [], c._frame_buffer )

  def test_process_frames_when_partial_content(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    method_frame = MethodFrame('ch_id', 'c_id', 'm_id')
    header_frame = HeaderFrame('ch_id', 'c_id', 0, 50)
    content_frame = ContentFrame('ch_id', 'hello')
    c._frame_buffer = [ method_frame, header_frame, content_frame ]

    self.replay_all()
    c.process_frames()
    self.assertEquals( [method_frame, header_frame, content_frame], c._frame_buffer )

  def test_process_frames_when_partial_content_followed_by_another_frame(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)
    self.mock( c, 'dispatch' )
    method_frame = MethodFrame('ch_id', 'c_id', 'm_id')
    header_frame = HeaderFrame('ch_id', 'c_id', 0, 50)
    content_frame = ContentFrame('ch_id', 'hello')
    c._frame_buffer = [ method_frame, header_frame, content_frame, MethodFrame('a','b','c') ]

    conn.close( 505, mox.IsA(str) )

    self.replay_all()
    c.process_frames()

  def test_process_frames_processes_the_whole_buffer(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    method_frame = MethodFrame('ch_id', 'c_id', 'm_id')
    header_frame = HeaderFrame('ch_id', 'c_id', 0, 5)
    content_frame = ContentFrame('ch_id', 'hello')
    method2 = MethodFrame('a','b','c')
    heart = HeartbeatFrame()
    c._frame_buffer = [ method_frame, header_frame, content_frame, method2, heart ]

    c.dispatch( method_frame, header_frame, content_frame )
    c.dispatch( method2 )
    c.dispatch( heart )

    self.replay_all()
    c.process_frames()
    self.assertEquals( [], c._frame_buffer )

  def test_process_frames_handles_dispatch_error_by_closing_channel(self):
    conn = self.create_mock_anything()
    conn.logger = self.create_mock_anything()
    c = Channel(conn, 32)
    self.mock( c, 'dispatch' )
    self.mock( c, 'close' )
    frame = MethodFrame(32, 20, 40)
    c._frame_buffer = [ frame ]

    c.dispatch( frame ).AndRaise( Exception("failwhale") )
    conn.logger.error( mox.IsA(str), frame, None, exc_info=True )
    c.close( 500, mox.IsA(str) )

    self.replay_all()
    c.process_frames()
    self.assertEquals( [], c._frame_buffer )

  def test_send_frame_when_not_closed_no_flow_control_no_pending_events(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)

    conn.send_frame('frame')

    self.replay_all()
    c.send_frame( 'frame' )

  def test_send_frame_when_not_closed_no_flow_control_pending_event(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)
    c._pending_events.append( 'cb' )

    self.replay_all()
    c.send_frame( 'frame' )
    self.assertEquals( ['cb','frame'], c._pending_events )

  def test_send_frame_when_not_closed_and_flow_control(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)
    c.channel._active = False

    method = MethodFrame(1,2,3)
    heartbeat = HeartbeatFrame()
    header = HeaderFrame(1,2,3,4)
    content = ContentFrame(1,'foo')
    
    conn.send_frame( method )
    conn.send_frame( heartbeat )

    self.replay_all()
    c.send_frame( method )
    c.send_frame( heartbeat )
    self.assertRaises( Channel.Inactive, c.send_frame, header )
    self.assertRaises( Channel.Inactive, c.send_frame, content )

  def test_send_frame_when_closed_for_a_reason(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)
    c.channel._closed = True
    c.channel._close_info = {'reply_code':42, 'reply_text':'bad'}

    self.assertRaises( ChannelClosed, c.send_frame, 'frame' )
  
  def test_send_frame_when_closed_for_no_reason(self):
    conn = self.create_mock_anything()
    c = Channel(conn, 32)
    c.channel._closed = True
    c.channel._close_info = {'reply_code':42, 'reply_text':''}

    self.assertRaises( ChannelClosed, c.send_frame, 'frame' )

  def test_add_synchronous_cb(self):
    c = Channel(None,None)

    self.assertEquals( [], c._pending_events )
    c.add_synchronous_cb( 'foo' )
    self.assertEquals( ['foo'], c._pending_events )

  def test_clear_synchronous_cb_when_no_pending(self):
    c = Channel(None,None)
    self.mock( c, '_flush_pending_events' )

    self.replay_all()
    self.assertEquals( [], c._pending_events )
    c.clear_synchronous_cb( 'foo' )

  def test_clear_synchronous_cb_when_pending_cb_matches(self):
    c = Channel(None,None)
    self.mock( c, '_flush_pending_events' )
    c._pending_events = ['foo']

    c._flush_pending_events()

    self.replay_all()
    c.clear_synchronous_cb( 'foo' )
    self.assertEquals( [], c._pending_events )

  def test_clear_synchronous_cb_when_pending_cb_doesnt_match_but_isnt_in_list(self):
    c = Channel(None,None)
    self.mock( c, '_flush_pending_events' )
    c._pending_events = ['foo']

    c._flush_pending_events()

    self.replay_all()
    c.clear_synchronous_cb( 'bar' )
    self.assertEquals( ['foo'], c._pending_events )

  def test_clear_synchronous_cb_when_pending_cb_doesnt_match_but_isnt_in_list(self):
    c = Channel(None,None)
    self.mock( c, '_flush_pending_events' )
    c._pending_events = ['foo', 'bar']

    self.replay_all()
    self.assertRaises( ChannelError, c.clear_synchronous_cb, 'bar' )
    self.assertEquals( ['foo','bar'], c._pending_events )

  def test_flush_pending_events_flushes_all_leading_frames(self):
    conn = self.create_mock_anything()
    c = Channel(conn,42)
    f1 = MethodFrame(1,2,3)
    f2 = MethodFrame(1,2,3)
    f3 = MethodFrame(1,2,3)
    c._pending_events = [f1, f2, 'cb', f3]

    conn.send_frame( f1 )
    conn.send_frame( f2 )

    self.replay_all()
    c._flush_pending_events()
    self.assertEquals( ['cb',f3], c._pending_events )
