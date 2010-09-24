
from haigha.frames.frame import Frame

class MethodFrame(Frame):
  '''
  Frame which carries identifier for methods.
  '''

  @classmethod
  def type(cls):
    return 1
  register()

  def __init__(self):
    pass

  
