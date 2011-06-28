'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import struct
import time
from datetime import datetime

from haigha.frames import method_frame
from haigha.frames.method_frame import MethodFrame
from haigha.reader import Reader
from haigha.writer import Writer

class MethodFrameTest(Chai):

  def test_type(self):
    assert_equals( 1, MethodFrame.type() )

  def test_properties(self):
    frame = MethodFrame('channel_id', 'class_id', 'method_id', 'args')
    assert_equals( 'channel_id', frame.channel_id )
    assert_equals( 'class_id', frame.class_id )
    assert_equals( 'method_id', frame.method_id )
    assert_equals( 'args', frame.args )

  def test_parse(self):
    reader = mock()
    expect( reader.read_short ).returns('class_id')
    expect( reader.read_short ).returns('method_id')
    frame = MethodFrame.parse( 42, reader )

    assert_equals( 42, frame.channel_id )
    assert_equals( 'class_id', frame.class_id )
    assert_equals( 'method_id', frame.method_id )
    assert_equals( reader, frame.args )

  def test_str(self):
    frame = MethodFrame( 42, 5, 6, Reader(bytearray('hello')) )
    assert_equals( 'MethodFrame[channel: 42, class_id: 5, method_id: 6, args: \\x68\\x65\\x6c\\x6c\\x6f]', str(frame) )
    
    frame = MethodFrame( 42, 5, 6 )
    assert_equals( 'MethodFrame[channel: 42, class_id: 5, method_id: 6, args: None]', str(frame) )

  def test_write_frame(self):
    args = mock()
    expect( args.buffer ).returns( 'hello' )

    frame = MethodFrame(42, 5, 6, args)
    buf = bytearray()
    frame.write_frame( buf )

    reader = Reader(buf)
    assert_equals( 1, reader.read_octet() )
    assert_equals( 42, reader.read_short() )
    size = reader.read_long()
    start_pos = reader.tell()
    assert_equals( 5, reader.read_short() )
    assert_equals( 6, reader.read_short() )
    args_pos = reader.tell()
    assert_equals( 'hello', reader.read( size-(args_pos-start_pos) ) )
    assert_equals( 0xce, reader.read_octet() )
