from haigha.lib.classes import ProtocolClass

class TransactionClass(ProtocolClass):
  '''
  Implements the AMQP Transaction class
  '''
  
  def select(self):
    pass

  def select_ok(self):
    pass
    
  def commit(self):
    pass

  def commit_ok(self):
    pass

  def rollback(self):
    pass

  def rollback_ok(self):
    pass
