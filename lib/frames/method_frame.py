import struct
from haigha.lib.frames.frame import Frame
from haigha.lib.reader import Reader
from haigha.lib.writer import Writer
from cStringIO import StringIO      # TODO: find suitable alternative

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
    class_id, method_id = struct.unpack( '>HH', payload[:4] )
    args = Reader( payload[4:] )
    return MethodFrame( channel_id, class_id, method_id, args )

  def __init__(self, channel_id, class_id, method_id, args=None):
    Frame.__init__(self, channel_id)
    self._class_id = class_id
    self._method_id = method_id
    self._args = args

  def __str__(self):
    if isinstance(self.args, Reader):
      return "%s[channel: %d, class_id: %d, method_id: %d, args: %s]"%( self.__class__.__name__, \
          self.channel_id, self.class_id, self.method_id, self.args.input.getvalue().encode('string_escape'))
    elif self.args!=None:
      stream = StringIO()
      self.args.flush(stream)
      return "%s[channel: %d, class_id : %d, method_id: %d, args: %s]"%( self.__class__.__name__, \
          self.channel_id, self.class_id, self.method_id, stream.getvalue().encode('string_escape'))
    else:
      return "%s[channel: %d, class_id : %d, method_id: %d, args: None]"%( self.__class__.__name__, \
          self.channel_id, self.class_id, self.method_id)
  
  def write_frame(self, stream):
    writer = Writer()
    writer.write_octet( 1 )
    writer.write_short( self.channel_id )
    writer.flush( stream )


    stream_args_len_pos = stream.tell()
    writer = Writer()
    writer.write_long(0)
    writer.flush( stream )

    # Mark the point in the stream where we start writing arguments, *including*
    # the class and method ids.
    stream_method_pos = stream.tell()
    #print 'stream method at ', stream_method_pos

    #if args==None:
    #  pkt.write_long(4)
    #else:
    #  pkt.write_long(len(args)+4)  # 4 = length of class_id and method_id
                                     # in payload

    writer = Writer()
    writer.write_short(self.class_id)
    writer.write_short(self.method_id)
    writer.flush(stream)
    stream_end_args_pos = stream_end_method_pos = stream.tell()

    if self._args != None:
      self._args.flush(stream)
      stream_end_args_pos = stream.tell()

    stream_len = stream_end_args_pos - stream_method_pos
    #print 'stream arg length ', stream_len

    stream.seek( stream_method_pos )
    
    # Seek all the way back to when we started writing the arguments and
    # write the total length of the bytes we wrote.
    writer = Writer()
    writer.write_long( stream_len )
    stream.seek( stream_args_len_pos )
    writer.flush( stream )

    # Seek to end and write the footer
    stream.seek( 0, 2 )
    stream.write('\xce')
    
  
MethodFrame.register()
