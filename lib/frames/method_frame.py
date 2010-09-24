
from haigha.lib.frames.frame import Frame

class MethodFrame(Frame):
  '''
  Frame which carries identifier for methods.
  '''

  @classmethod
  def type(cls):
    return 1


  def __init__(selfi):
    Frame.__init__(self, *args, **kwargs)

MethodFrame.register()
