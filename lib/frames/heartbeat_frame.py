
from haigha.frames.frame import Frame

class HeartbeatFrame(Frame):
  '''
  Frame for heartbeats.
  '''

  @classmethod
  def type():
    return 8
  register()

  def __init__(self):
    Frame.__init__(self, *args, **kwargs)
