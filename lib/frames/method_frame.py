import struct
from haigha.lib.frames.frame import Frame
from haigha.lib.reader import Reader

class MethodFrame(Frame):
  '''
  Frame which carries identifier for methods.
  '''

  @classmethod
  def type(cls):
    return 1


  def __init__(self, *args, **kwargs):
    Frame.__init__(self, *args, **kwargs)
    self.class_id, self.method_id = struct.unpack( '>HH', self.payload[:4] )
    self.args = Reader( self.payload[4:] )
  
MethodFrame.register()
