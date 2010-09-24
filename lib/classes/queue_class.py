from haigha.lib.classes import ProtocolClass

class QueueClass(ProtocolClass):
  '''
  Implements the AMQP Queue class
  '''
  @ProtocolClass.register(10)
  def declare(self):
    pass

  @ProtocolClass.register(11)
  def declare_ok(self):
    pass
    
  @ProtocolClass.register(20)
  def bind(self):
    pass

  @ProtocolClass.register(21)
  def bind_ok(self):
    pass

  @ProtocolClass.register(50)
  def unbind(self):
    pass

  @ProtocolClass.register(51)
  def unbind_ok(self):
    pass
    
  @ProtocolClass.register(30)
  def purge(self):
    pass

  @ProtocolClass.register(31)
  def purge_ok(self):
    pass

  @ProtocolClass.register(40)
  def delete(self):
    pass

  @ProtocolClass.register(41)
  def delete_ok(self):
    pass
