from haigha.lib.classes import ProtocolClass

class QueueClass(ProtocolClass):
  '''
  Implements the AMQP Queue class
  '''
  def declare(self):
    pass

  def declare_ok(self):
    pass
    
  def bind(self):
    pass

  def bind_ok(self):
    pass

  def unbind(self):
    pass

  def unbind_ok(self):
    pass
    
  def purge(self):
    pass

  def purge_ok(self):
    pass

  def delete(self):
    pass

  def delete_ok(self):
    pass
