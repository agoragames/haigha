import mox
import struct

from haigha.frames import frame
from haigha.frames import Frame
from haigha.reader import Reader

class FrameTest(mox.MoxTestBase):

  def test_register(self):
    class DummyFrame(Frame):
      @classmethod
      def type(self): return 42

    self.assertEquals( None, Frame._frame_type_map.get(42) )
    DummyFrame.register()
    self.assertEquals( DummyFrame, Frame._frame_type_map[42] )

  def test_type_raises_not_implemented(self):
    self.assertRaises( NotImplementedError, Frame.type )

  def test_read_frames_reads_until_buffer_underflow(self):
    stream = self.create_mock_anything()
    self.mock( Frame, '_read_frame' )

    stream.tell().AndReturn( 0 )
    Frame._read_frame(stream).AndReturn('frame1')

    stream.tell().AndReturn( 2 )
    Frame._read_frame(stream).AndReturn('frame2')

    stream.tell().AndReturn( 3 )
    Frame._read_frame(stream).AndRaise( Reader.BufferUnderflow )

    stream.seek( 3 )

    self.replay_all()
    self.assertEquals( ['frame1','frame2'], Frame.read_frames(stream) )

  def test_read_frames_handles_reader_errors(self):
    stream = self.create_mock_anything()
    self.mock( Frame, '_read_frame' )
    
    stream.tell().AndReturn( 0 )
    Frame._read_frame(stream).AndRaise( Reader.ReaderError("bad!") )

    self.replay_all()
    self.assertRaises( Frame.FormatError, Frame.read_frames, stream )

  def test_read_frames_handles_struct_errors(self):
    stream = self.create_mock_anything()
    self.mock( Frame, '_read_frame' )
    
    stream.tell().AndReturn( 0 )
    Frame._read_frame(stream).AndRaise( struct.error("bad!") )

    self.replay_all()
    self.assertRaises( Frame.FormatError, Frame.read_frames, stream )

  def test_read_frame_on_full_frame(self):
    class FrameReader(Frame):
      @classmethod
      def type(self): return 45

      @classmethod
      def parse(self, channel_id, payload):
        return 'a_frame'
    FrameReader.register()

    self.mock( frame, 'Reader', use_mock_anything=True )
    reader = self.create_mock_anything()
    
    frame.Reader('stream').AndReturn( reader )
    reader.read_octet().AndReturn( 45 )
    reader.read_short().AndReturn( 32 )
    reader.read_long().AndReturn( 5 )
    reader.read(5).AndReturn( 'hello' )
    reader.read_octet().AndReturn( 0xce )

    self.replay_all()
    self.assertEquals( 'a_frame', Frame._read_frame('stream') )

  def test_read_frame_returns_none_when_incomplete_payload(self):
    self.mock( frame, 'Reader', use_mock_anything=True )
    reader = self.create_mock_anything()
    
    frame.Reader('stream').AndReturn( reader )
    reader.read_octet().AndReturn( 45 )
    reader.read_short().AndReturn( 32 )
    reader.read_long().AndReturn( 5 )
    reader.read(5).AndReturn( 'hell' )

    self.replay_all()
    self.assertEquals( None, Frame._read_frame('stream') )

  def test_read_frame_raises_formaterror_if_bad_footer(self):
    self.mock( frame, 'Reader', use_mock_anything=True )
    reader = self.create_mock_anything()
    
    frame.Reader('stream').AndReturn( reader )
    reader.read_octet().AndReturn( 45 )
    reader.read_short().AndReturn( 32 )
    reader.read_long().AndReturn( 5 )
    reader.read(5).AndReturn( 'hello' )
    reader.read_octet().AndReturn( 0xff )

    self.replay_all()
    self.assertRaises( Frame.FormatError, Frame._read_frame, 'stream' )

  def test_read_frame_raises_invalidframetype_for_unregistered_frame(self):
    self.mock( frame, 'Reader', use_mock_anything=True )
    reader = self.create_mock_anything()
    
    frame.Reader('stream').AndReturn( reader )
    reader.read_octet().AndReturn( 420 )
    reader.read_short().AndReturn( 32 )
    reader.read_long().AndReturn( 5 )
    reader.read(5).AndReturn( 'hello' )
    reader.read_octet().AndReturn( 0xce )

    self.replay_all()
    self.assertRaises( Frame.InvalidFrameType, Frame._read_frame, 'stream' )
