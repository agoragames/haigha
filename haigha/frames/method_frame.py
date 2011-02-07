import struct
from haigha.frames.frame import Frame
from haigha.reader import Reader
from haigha.writer import Writer
from cStringIO import StringIO

class MethodFrame(Frame):
  '''
  Frame which carries identifier for methods.
  '''

  @classmethod
  def type(cls):
    return 1
  
  @property
  def class_id(self):
    return self._class_id
  
  @property
  def method_id(self):
    return self._method_id
  
  @property
  def args(self):
    return self._args

  @classmethod
  def parse(self, channel_id, payload):
    class_id = payload.read_short()
    method_id = payload.read_short()
    return MethodFrame( channel_id, class_id, method_id, payload )

  def __init__(self, channel_id, class_id, method_id, args=None):
    Frame.__init__(self, channel_id)
    self._class_id = class_id
    self._method_id = method_id
    self._args = args

  def __str__(self):
    if isinstance(self.args, (Reader,Writer)):
      return "%s[channel: %d, class_id: %d, method_id: %d, args: %s]"%\
        (self.__class__.__name__, self.channel_id, self.class_id, self.method_id, str(self.args))
    else:
      return "%s[channel: %d, class_id : %d, method_id: %d, args: None]"%\
        ( self.__class__.__name__, self.channel_id, self.class_id, self.method_id)
  
  def write_frame(self, buf):
    writer = Writer(buf)
    writer.write_octet( self.type() )
    writer.write_short( self.channel_id )
    #writer.flush( stream )


    #stream_args_len_pos = stream.tell()
    stream_args_len_pos = len(buf)
    #writer = Writer()
    #writer.write_long(0)  # temporary storage of total length
    #writer.flush( stream )
    writer.write_long(0)

    # Mark the point in the stream where we start writing arguments, *including*
    # the class and method ids.
    #stream_method_pos = stream.tell()
    stream_method_pos = len(buf)

    #writer = Writer()
    writer.write_short(self.class_id)
    writer.write_short(self.method_id)
    #writer.flush(stream)
    #stream_end_args_pos = stream_end_method_pos = stream.tell()
    stream_end_args_pos = stream_end_method_pos = len(buf)

    # This is assuming that args is a Writer
    if self._args != None:
      #self._args.flush(stream)
      #stream_end_args_pos = stream.tell()
      writer.write( self._args.buffer() )
      stream_end_args_pos = len(buf)

    stream_len = stream_end_args_pos - stream_method_pos

    #stream.seek( stream_method_pos )
    
    # Seek all the way back to when we started writing the arguments and
    # write the total length of the bytes we wrote.
    #writer = Writer()
    #writer.write_long( stream_len )
    #stream.seek( stream_args_len_pos )
    #writer.flush( stream )
    writer.write_long_at( stream_len, stream_args_len_pos )

    # Seek to end and write the footer
    #stream.seek( 0, 2 )
    #stream.write('\xce')
    writer.write_octet(0xce)
    
  
MethodFrame.register()
