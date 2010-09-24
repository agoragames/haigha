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
  
  @property
  def class_id(self):
    return self._class_id
  
  @property
  def method_id(self):
    return self._method_id
  
  @property
  def args(self):
    return self._args

  def __init__(self, *args, **kwargs):
    Frame.__init__(self, *args, **kwargs)
    self._class_id, self._method_id = struct.unpack( '>HH', self.payload[:4] )
    self._args = Reader( self.payload[4:] )
  
MethodFrame.register()
