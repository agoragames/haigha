import functools
from haigha.lib.classes import ProtocolClass


class ChannelClass(ProtocolClass):
  '''
  Implements the AMQP Channel class
  '''
  
  @ProtocolClass.register(10)
  def open(self):
    pass

  @ProtocolClass.register(11)
  def open_ok(self):
    pass
    
  @ProtocolClass.register(20)
  def flow(self):
    pass

  @ProtocolClass.register(21)
  def flow_ok(self):
    pass

  @ProtocolClass.register(40)
  def close(self):
    pass

  @ProtocolClass.register(41)
  def close_ok(self):
    pass
