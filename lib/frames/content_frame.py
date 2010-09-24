
from haigha.lib.frames.frame import Frame

class ContentFrame(Frame):
  '''
  Frame for reading in content.
  '''

  @classmethod
  def type(cls):
    return 3

  @property
  def payload(self):
    return self._payload

  @classmethod
  def parse(self, channel_id, payload):
    return ContentFrame( channel_id, payload)
    
  def __init__(self, channel_id, payload):
    Frame.__init__(self, channel_id)
    self._payload = payload

ContentFrame.register()

