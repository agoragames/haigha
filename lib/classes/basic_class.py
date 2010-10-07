from haigha.lib.classes import ProtocolClass

class BasicClass(ProtocolClass):
  '''
  Implements the AMQP Basic class
  '''

  # TODO: Add a concept of number of pending transactions when we re-implement
  # public_synchronous.  May be something that goes into the Channel object,
  # or that it will walk all the pending frames in the Channel sync buffer and
  # determine how many transaction commits there are

  # TODO: Also include an optional callback method when a transaction is committed.
  
  def qos(self):
    pass

  def qos_ok(self):
    pass
    
  def consume(self):
    pass

  def consume_ok(self):
    pass

  def cancel(self):
    pass

  def cancel_ok(self):
    pass
    
  def publish(self):
    pass

  def basic_return(self):
    pass

  def deliver(self):
    pass

  def get(self):
    pass

  def get_ok(self):
    pass

  def get_empty(self):
    pass

  def ack(self):
    pass
    
  def reject(self):
    pass

  def recover_async(self):
    pass

  def recover(self):
    pass

  def recover_ok(self):
    pass
