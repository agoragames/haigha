from haigha.lib.classes import ProtocolClass

class TransactionClass(ProtocolClass):
  '''
  Implements the AMQP Transaction class
  '''
  
  @ProtocolClass.register(10)
  def select(self):
    pass

  @ProtocolClass.register(11)
  def select_ok(self):
    pass
    
  @ProtocolClass.register(20)
  def commit(self):
    pass

  @ProtocolClass.register(21)
  def commit_ok(self):
    pass

  @ProtocolClass.register(30)
  def rollback(self):
    pass

  @ProtocolClass.register(31)
  def rollback_ok(self):
    pass
