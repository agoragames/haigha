
from haigha.lib.frames.frame import Frame

class HeaderFrame(Frame):
  '''
  Header frame for content.
  '''

  @classmethod
  def type(cls):
    return 2

HeaderFrame.register()
