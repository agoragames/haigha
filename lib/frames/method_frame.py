import struct
from haigha.lib.frames.frame import Frame
from haigha.lib.reader import Reader
from haigha.lib.writer import Writer

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

  def __init__(self, channel_id, class_id, method_id, args):
    Frame.__init__(self, channel_id)
    self._class_id = class_id
    self._method_id = method_id
    self._args = args
  
  def write_frame(self, stream):
    writer = Writer()
    writer.write_short(self.method_id)
    writer.write_short(self.class_id)
    writer.flush(stream)
    self._args.flush(stream)
  
MethodFrame.register()
