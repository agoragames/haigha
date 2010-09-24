from haigha.lib.classes import ProtocolClass

class BasicClass(ProtocolClass):
  '''
  Implements the AMQP Basic class
  '''
  
  @ProtocolClass.register(10)
  def qos(self):
    pass

  @ProtocolClass.register(11)
  def qos_ok(self):
    pass
    
  @ProtocolClass.register(20)
  def consume(self):
    pass

  @ProtocolClass.register(21)
  def consume_ok(self):
    pass

  @ProtocolClass.register(30)
  def cancel(self):
    pass

  @ProtocolClass.register(31)
  def cancel_ok(self):
    pass
    
  @ProtocolClass.register(40)
  def publish(self):
    pass

  @ProtocolClass.register(50)
  def basic_return(self):
    pass

  @ProtocolClass.register(60)
  def deliver(self):
    pass

  @ProtocolClass.register(70)
  def get(self):
    pass

  @ProtocolClass.register(71)
  def get_ok(self):
    pass

  @ProtocolClass.register(72)
  def get_empty(self):
    pass

  @ProtocolClass.register(80)
  def ack(self):
    pass
    
  @ProtocolClass.register(90)
  def reject(self):
    pass

  @ProtocolClass.register(100)
  def recover_async(self):
    pass

  @ProtocolClass.register(110)
  def recover(self):
    pass

  @ProtocolClass.register(111)
  def recover_ok(self):
    pass
