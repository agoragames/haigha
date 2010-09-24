
from haigha.lib.frames.frame import Frame

class HeaderFrame(Frame):
  '''
  Header frame for content.
  '''

  @classmethod
  def type(cls):
    return 2
  
  @property
  def class_id(self):
    return self._class_id
  
  @property
  def weight(self):
    return self._weight
  
  @property
  def size(self):
    return self._size
  
  @classmethod
  def parse(self, channel_id, payload):
    class_id, weight, size = struct.unpack( '>HHQ', payload[:12] )
    return HeaderFrame( channel_id, class_id, weight, size)
    
  def __init__(self, channel_id, class_id, weight, size):
    Frame.__init__(self, channel_id)
    self._class_id = class_id
    self._weight = weight
    self._size = size

HeaderFrame.register()
