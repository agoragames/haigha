
import mox
from datetime import datetime
from cStringIO import StringIO
import event

from haigha.channel import Channel
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
    self.assertEquals( [], c._input_frame_buffer )
    self.assertEquals( None, c._input_event )

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

  def test_close(self):
    c = Channel(None,None)
    self.mock( c.channel, 'close' )
    c.channel.close()

    self.replay_all()
    c.close()

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
    self.mock(event, 'timeout')

    event.timeout( 0, c._process_frames ).AndReturn( 'pending_event' )

    self.replay_all()
    c.buffer_frame( 'f1' )
    c.buffer_frame( 'f2' )
    self.assertEquals( 'pending_event', c._input_event )
    self.assertEquals( ['f1', 'f2'], c._input_frame_buffer )

  def test_process_frames_when_no_frames(self):
    # Not that this should ever happen, but to be sure
    c = Channel(None,None)
    c._input_event = 'foo'
    self.mock( c, 'dispatch' )

    self.replay_all()
    c._process_frames()
    self.assertEquals( None, c._input_event )

  def test_process_frames_when_just_a_method_frame(self):
    c = Channel(None, None)
    self.mock( c, 'dispatch' )
    frame = MethodFrame('ch_id', 'c_id', 'm_id')
    c._input_frame_buffer = [ frame ]

    c.dispatch( frame )

    self.replay_all()
    c._process_frames()
    self.assertEquals( [], c._input_frame_buffer )

  #def test_process_frames_when_not_a_method_frame(self):
