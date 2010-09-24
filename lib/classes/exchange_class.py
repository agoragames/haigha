from haigha.lib.classes import ProtocolClass

class ExchangeClass(ProtocolClass):
  '''
  Implements the AMQP Exchange class
  '''
  @ProtocolClass.register(10)
  def declare(self):
    pass

  @ProtocolClass.register(11)
  def declare_ok(self):
    pass
    
  @ProtocolClass.register(20)
  def delete(self):
    pass

  @ProtocolClass.register(21)
  def delete_ok(self):
    pass
