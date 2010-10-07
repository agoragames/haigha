import functools
from haigha.lib.classes import ProtocolClass
from haigha.lib.frames import MethodFrame
from haigha.lib.writer import Writer

class ChannelClass(ProtocolClass):
  '''
  Implements the AMQP Channel class
  '''
  
  def __init__(self, *args, **kwargs):
    super(ChannelClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_open_ok
    }
  
  def open(self):
    self._send_open()

  def flow(self):
    pass

  def flow_ok(self):
    pass

  def close(self):
    pass

  def close_ok(self):
    pass

  def _send_open(self):
    args = Writer()
    args.write_shortstr('')   # TODO: support out-of-band.  check on 0.9.1 compatability
    self.send_frame( MethodFrame(self.channel_id, 20, 10, args) )
    self.channel.add_synchronous_cb( self._recv_open_ok )

  def _recv_open_ok(self, method_frame):
    self.is_open = True
