
from haigha.frames.frame import Frame

class HeartbeatFrame(Frame):
  '''
  Frame for heartbeats.
  '''

  @classmethod
  def type():
    # NOTE: The PDF spec say this should be 4 but the xml spec say it should be 8
    # PDF spec: http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.pdf?version=1&modificationDate=1227526523000
    # XML spec: http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.xml?version=1&modificationDate=1227526672000
    return 8
  register()

  def __init__(self):
    Frame.__init__(self, *args, **kwargs)
