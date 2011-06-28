'''
Copyright (c) 2011, Agora Games, LLC All rights reserved.

https://github.com/agoragames/haigha/blob/master/LICENSE.txt
'''

from haigha.frames.frame import Frame
from haigha.reader import Reader
from haigha.writer import Writer

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
      return "%s[channel: %d, class_id: %d, method_id: %d, args: None]"%\
        ( self.__class__.__name__, self.channel_id, self.class_id, self.method_id)
  
  def write_frame(self, buf):
    writer = Writer(buf)
    writer.write_octet( self.type() )
    writer.write_short( self.channel_id )

    # Write a temporary value for the total length of the frame
    stream_args_len_pos = len(buf)
    writer.write_long(0)

    # Mark the point in the stream where we start writing arguments, *including*
    # the class and method ids.
    stream_method_pos = len(buf)

    writer.write_short(self.class_id)
    writer.write_short(self.method_id)

    # This is assuming that args is a Writer
    if self._args != None:
      writer.write( self._args.buffer() )

    # Write the total length back at the position we allocated
    stream_len = len(buf) - stream_method_pos
    writer.write_long_at( stream_len, stream_args_len_pos )

    # Write the footer
    writer.write_octet(0xce)
    
  
MethodFrame.register()
