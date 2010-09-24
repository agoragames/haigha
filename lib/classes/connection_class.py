from protocol_class import ProtocolClass

class ConnectionClass(ProtocolClass):
  '''
  Implements the AMQP Connection class
  '''

  # 10
  def start(self, method_frame):
    '''Called by broker when initialzing the connection.'''
    #self.channel.logger.info("DEBUG: start")
    #print method_frame
    # TODO: think about how to make this protected in Connection.  May need
    # to implement such that the channel it's on is private to Connection and
    # so we can get direct access.
    self.channel.connection.start()

  def start_ok(self, 
    '''Called by client to indicate that we're ready.'''
    

  dispatch_map = {
  }
