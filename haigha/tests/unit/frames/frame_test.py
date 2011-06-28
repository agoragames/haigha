'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from chai import Chai
import struct
from collections import deque

from haigha.frames import frame
from haigha.frames import Frame
from haigha.reader import Reader

class FrameTest(Chai):

  def test_register(self):
    class DummyFrame(Frame):
      @classmethod
      def type(self): return 42

    assertEquals( None, Frame._frame_type_map.get(42) )
    DummyFrame.register()
    assertEquals( DummyFrame, Frame._frame_type_map[42] )

  def test_type_raises_not_implemented(self):
    assertRaises( NotImplementedError, Frame.type )

  def test_read_frames_reads_until_buffer_underflow(self):
    reader = mock()

    expect( reader.tell ).returns( 0 )
    expect( Frame._read_frame ).args(reader).returns('frame1')

    expect( reader.tell ).returns( 2 )
    expect( Frame._read_frame ).args(reader).returns('frame2')

    expect( reader.tell ).returns( 3 )
    expect( Frame._read_frame ).args(reader).raises( Reader.BufferUnderflow )

    expect( reader.seek ).args( 3 )

    assertEquals( deque(['frame1','frame2']), Frame.read_frames(reader) )

  def test_read_frames_handles_reader_errors(self):
    reader = mock()
    self.mock( Frame, '_read_frame' )
    
    expect( reader.tell ).returns( 0 )
    expect( Frame._read_frame ).args(reader).raises( Reader.ReaderError("bad!") )

    assertRaises( Frame.FormatError, Frame.read_frames, reader )

  def test_read_frames_handles_struct_errors(self):
    reader = mock()
    self.mock( Frame, '_read_frame' )
    
    expect( reader.tell ).returns( 0 )
    expect( Frame._read_frame ).args(reader).raises( struct.error("bad!") )

    self.assertRaises( Frame.FormatError, Frame.read_frames, reader )

  def test_read_frame_on_full_frame(self):
    class FrameReader(Frame):
      @classmethod
      def type(self): return 45

      @classmethod
      def parse(self, channel_id, payload):
        return 'no_frame'
    FrameReader.register()

    self.mock( frame, 'Reader' )
    reader = self.mock()
    payload = self.mock()

    expect( reader.read_octet ).returns( 45 ) # frame type
    expect( reader.read_short ).returns( 32 ) # channel id
    expect( reader.read_long ).returns( 42 )  # size
    
    expect( reader.tell ).returns( 5 )
    expect( frame.Reader ).args(reader, 5, 42).returns( payload )
    expect( reader.seek ).args( 42, 1 )

    expect( reader.read_octet ).returns( 0xce )
    expect( FrameReader.parse ).args( 32, payload ).returns( 'a_frame' )

    assertEquals( 'a_frame', Frame._read_frame(reader) )

  def test_read_frame_raises_bufferunderflow_when_incomplete_payload(self):
    self.mock( frame, 'Reader' )
    reader = self.mock()

    expect( reader.read_octet ).returns( 45 ) # frame type
    expect( reader.read_short ).returns( 32 ) # channel id
    expect( reader.read_long ).returns( 42 )  # size
    
    expect( reader.tell ).returns( 5 )
    expect( frame.Reader ).args(reader, 5, 42).returns( 'payload' )
    expect( reader.seek ).args( 42, 1 )

    expect( reader.read_octet ).raises( Reader.BufferUnderflow )
    assert_raises( Reader.BufferUnderflow, Frame._read_frame, reader )

  def test_read_frame_raises_formaterror_if_bad_footer(self):
    self.mock( frame, 'Reader' )
    reader = self.mock()

    expect( reader.read_octet ).returns( 45 ) # frame type
    expect( reader.read_short ).returns( 32 ) # channel id
    expect( reader.read_long ).returns( 42 )  # size
    
    expect( reader.tell ).returns( 5 )
    expect( frame.Reader ).args(reader, 5, 42).returns( 'payload' )
    expect( reader.seek ).args( 42, 1 )
    expect( reader.read_octet ).returns( 0xff )

    assert_raises( Frame.FormatError, Frame._read_frame, reader )

  def test_read_frame_raises_invalidframetype_for_unregistered_frame_type(self):
    self.mock( frame, 'Reader' )
    reader = self.mock()
    payload = self.mock()

    expect( reader.read_octet ).returns( 54 ) # frame type
    expect( reader.read_short ).returns( 32 ) # channel id
    expect( reader.read_long ).returns( 42 )  # size
    
    expect( reader.tell ).returns( 5 )
    expect( frame.Reader ).args(reader, 5, 42).returns( payload )
    expect( reader.seek ).args( 42, 1 )

    expect( reader.read_octet ).returns( 0xce )

    assertRaises( Frame.InvalidFrameType, Frame._read_frame, reader )
  
  def test_parse_raises_not_implemented(self):
    assertRaises( NotImplementedError, Frame.parse, 'channel_id', 'payload' )

  def test_properties(self):
    frame = Frame('channel_id')
    assert_equals('channel_id', frame.channel_id)

  def test_str(self):
    frame = Frame(42)
    assert_equals('Frame[channel: 42]', str(frame))

  def test_repr(self):
    expect(Frame.__str__).returns('foo')
    frame = Frame(42)
    assert_equals('foo', repr(frame))

  def test_write_frame(self):
    frame = Frame(42)
    assert_raises( NotImplementedError, frame.write_frame, 'stream' )
