
from haigha.lib.frames.frame import Frame

class ContentFrame(Frame):
  '''
  Frame for reading in content.
  '''

  @classmethod
  def type(cls):
    return 3
  
  @property
  def class_id(self):
    return self._class_id
  
  @property
  def weight(self):
    return self._weight
  
  @property
  def body_size(self):
    return self._body_size

  def __init__(self, *args, **kwargs):
    Frame.__init__(self, *args, **kwargs)
    self._class_id, self._weight, self._body_size = struct.unpack( '>HHQ', payload[:12] )

ContentFrame.register()

