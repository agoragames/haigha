'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai

from haigha.classes import channel_class, ProtocolClass, ChannelClass
from haigha.frames import MethodFrame
from haigha.writer import Writer

class ChannelClassTest(Chai):

  def setUp(self):
    super(ChannelClassTest,self).setUp()
    ch = mock()
    ch.channel_id = 42
    ch.logger = mock()
    self.klass = ChannelClass( ch )

  def test_init(self):
    expect(ProtocolClass.__init__).args('foo', a='b' )
    
    klass = ChannelClass.__new__(ChannelClass)
    klass.__init__('foo', a='b')

    assert_equals( 
      {
        11 : klass._recv_open_ok,
        20 : klass._recv_flow,
        21 : klass._recv_flow_ok,
        40 : klass._recv_close,
        41 : klass._recv_close_ok,
      }, klass.dispatch_map )
    assert_false( klass._closed )
    assert_equals(
      {
        'reply_code'    : 0,
        'reply_text'    : 'first connect',
        'class_id'      : 0,
        'method_id'     : 0
      }, klass._close_info )
    assert_true( klass._active )
    assert_equals( None, klass._flow_control_cb )

  def test_cleanup(self):
    self.klass._cleanup()
    assert_equals( None, self.klass._channel )
    assert_equals( None, self.klass.dispatch_map )

  def test_properties(self):
    self.klass._closed = 'yes'
    self.klass._close_info = 'ithappened'
    self.klass._active = 'record'
    assert_equals( 'yes', self.klass.closed )
    assert_equals( 'ithappened', self.klass.close_info )
    assert_equals( 'record', self.klass.active )

    self.klass._closed = False
    assert_equals( None, self.klass.close_info )

  def test_set_flow_cb(self):
    assert_equals( None, self.klass._flow_control_cb )
    self.klass.set_flow_cb( 'foo' )
    assert_equals( 'foo', self.klass._flow_control_cb )

  def test_open(self):
    writer = mock()
    expect( mock(channel_class, 'Writer') ).returns( writer )
    expect( writer.write_shortstr ).args( '' )
    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 10, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_open_ok )

    self.klass.open()

  def test_recv_open_ok(self):
    self.klass._recv_open_ok('methodframe')

  def test_activate_when_not_active(self):
    self.klass._active = False
    expect( self.klass._send_flow ).args( True )
    self.klass.activate()

  def test_activate_when_active(self):
    self.klass._active = True
    stub( self.klass._send_flow )
    self.klass.activate()

  def test_deactivate_when_not_active(self):
    self.klass._active = False
    stub( self.klass._send_flow )
    self.klass.deactivate()

  def test_deactivate_when_active(self):
    self.klass._active = True
    expect( self.klass._send_flow ).args( False )
    self.klass.deactivate()

  def test_send_flow(self):
    writer = mock()
    expect( mock(channel_class, 'Writer') ).returns( writer )
    expect( writer.write_bit ).args( 'active' )
    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 20, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_flow_ok )

    self.klass._send_flow('active')

  def test_recv_flow_no_cb(self):
    self.klass._flow_control_cb = None
    rframe = mock()
    writer = mock()
    expect( rframe.args.read_bit ).returns( 'active' )

    expect( mock(channel_class, 'Writer') ).returns( writer )
    expect( writer.write_bit ).args( 'active' )
    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 21, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )

    self.klass._recv_flow(rframe)
    assert_equals( 'active', self.klass._active )

  def test_recv_flow_with_cb(self):
    self.klass._flow_control_cb = mock()
    rframe = mock()
    writer = mock()
    expect( rframe.args.read_bit ).returns( 'active' )

    expect( mock(channel_class, 'Writer') ).returns( writer )
    expect( writer.write_bit ).args( 'active' )
    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 21, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass._flow_control_cb )

    self.klass._recv_flow(rframe)

  def test_recv_flow_ok_no_cb(self):
    self.klass._flow_control_cb = None
    rframe = mock()
    expect( rframe.args.read_bit ).returns( 'active' )

    self.klass._recv_flow_ok( rframe )
    assert_equals( 'active', self.klass._active )

  def test_recv_flow_ok_with_cb(self):
    self.klass._flow_control_cb = mock()
    rframe = mock()
    expect( rframe.args.read_bit ).returns( 'active' )
    expect( self.klass._flow_control_cb )

    self.klass._recv_flow_ok( rframe )
    assert_equals( 'active', self.klass._active )

  def test_close_when_not_closed(self):
    self.klass._closed = False
    writer = mock()
    expect( mock(channel_class, 'Writer') ).returns( writer )
    expect( writer.write_short ).args( 'rcode' )
    expect( writer.write_shortstr ).args( 'reason' )
    expect( writer.write_short ).args( 'cid' )
    expect( writer.write_short ).args( 'mid' )
    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 40, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' )
    expect( self.klass.channel.add_synchronous_cb ).args( self.klass._recv_close_ok )

    self.klass.close('rcode', 'reason', 'cid', 'mid')
    assert_true( self.klass._closed )
    assert_equals( {
        'reply_code'  : 'rcode',
        'reply_text'  : 'reason',
        'class_id'    : 'cid',
        'method_id'   : 'mid',
      }, self.klass._close_info )

  def test_close_when_closed(self):
    self.klass._closed = True
    stub( self.klass.send_frame )

    self.klass.close()

  def test_close_when_error_sending_frame(self):
    self.klass._closed = False
    writer = mock()
    expect( mock(channel_class, 'Writer') ).returns( writer )
    expect( writer.write_short ).args( 0 )
    expect( writer.write_shortstr ).args( '' )
    expect( writer.write_short ).args( 0 )
    expect( writer.write_short ).args( 0 )
    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 40, writer).returns( 'frame' )
    expect( self.klass.send_frame ).args( 'frame' ).raises( Exception('fail') )
    expect( self.klass.logger.error ).args( 
      'Failed to close channel %d', 42, exc_info=True )

    self.klass.close()
    assert_true( self.klass._closed )
    assert_equals( {
        'reply_code'  : 0,
        'reply_text'  : '',
        'class_id'    : 0,
        'method_id'   : 0,
      }, self.klass._close_info )

  def test_recv_close(self):
    rframe = mock()
    expect( rframe.args.read_short ).returns( 'rcode' )
    expect( rframe.args.read_shortstr ).returns( 'reason' )
    expect( rframe.args.read_short ).returns( 'cid' )
    expect( rframe.args.read_short ).returns( 'mid' )

    expect( mock(channel_class, 'MethodFrame') ).args(42, 20, 41).returns( 'frame' )
    expect( self.klass.channel._closed_cb ).args( final_frame='frame' )

    assert_false( self.klass._closed )
    self.klass._recv_close( rframe )
    assert_true( self.klass._closed )
    assert_equals( {
        'reply_code'  : 'rcode',
        'reply_text'  : 'reason',
        'class_id'    : 'cid',
        'method_id'   : 'mid',
      }, self.klass._close_info )

  def test_recv_close_ok(self):
    expect( self.klass.channel._closed_cb )

    self.klass._closed = False
    self.klass._recv_close_ok('frame')
    assert_true( self.klass._closed )
