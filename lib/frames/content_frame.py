
from haigha.lib.frames.frame import Frame

class ContentFrame(Frame):
  '''
  Frame for reading in content.
  '''

  @classmethod
  def type(cls):
    return 3

  def __init__(self, *args, **kwargs):
    Frame.__init__(self, *args, **kwargs)

ContentFrame.register()

