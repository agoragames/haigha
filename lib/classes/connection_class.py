
from haigha.writer import Writer
from haigha.frames import MethodFrame
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

  def start_ok(self, properties, login_method, login_response, locale):
    '''Called by client to indicate that we're ready.'''
    args = Writer()
    args.write_table(props)
    args.write_shortstr(mechanism)
    args.write_longstr(response)
    args.write_shortstr(locale)
    self.send_frame( MethodFrame(self.channel_id, 10, 11, args) )
