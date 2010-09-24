from haigha.lib.writer import Writer
from haigha.lib.frames import MethodFrame
from protocol_class import ProtocolClass

class ConnectionClass(ProtocolClass):
  '''
  Implements the AMQP Connection class
  '''

  @ProtocolClass.register(10)
  def start(self, method_frame):
    '''Called by broker when initialzing the connection.'''
    #self.channel.logger.info("DEBUG: start")
    #print method_frame
    # TODO: think about how to make this protected in Connection.  May need
    # to implement such that the channel it's on is private to Connection and
    # so we can get direct access.
    self.channel.connection.start()

  def start_ok(self, properties, login_method, login_response, locale):
    '''Called by connection to indicate that we're ready.  Connection must supply
    all this important data.'''
    # TODO: Figure out a better plan than that.
    args = Writer()
    args.write_table(props)
    args.write_shortstr(mechanism)
    args.write_longstr(response)
    args.write_shortstr(locale)
    self.send_frame( MethodFrame(self.channel_id, 10, 11, args) )

  @ProtocolClass.register(20)
  def secure(self, method_frame):
    '''Called by broker to setup security.'''
    self._send_open()
    #self.addFrameCallback( (10,41), self.handle_open_response )
    #self.addFrameCallback( (10,50), self.handle_redirect_response )
    #self.open( virtual_host=self.virtual_host, insist=self.insist )

  # TODO: figure out which of these the broker is sending, and which we're sending
  def secure_ok(self):
    pass

  @ProtocolClass.register(30)
  def tune(self, method_frame):
    # TODO: Send these off to the Connection
    channel_max = args.read_short() # or connection.channel_max
    frame_max = args.read_long() # or connection.frame_max

    # Note that 'is' test is required here!
    #if self.heartbeat is None:
    #  self.heartbeat = args.read_short()

    args = Writer()
    args.write_short( channel_max )
    args.write_long( frame_max )

    #args.write_short( self.heartbeat )
    args.write_short( 0 )
    self.send_frame( MethodFrame(self.channel_id, 10, 31, args) )

    self._send_open()

  @ProtocolClass.register(41)
  def open_ok(self, method_frame):
    '''Called by broker when connection is ready'''
    # 
    self.connected = True
    for (pkt,channel_id) in self.output_buffer:
      if channel_id in self.channels:
        self.writePacket( pkt, channel_id )
    self.output_buffer = []
    #self.addFrameCallback( (10,60), self.handle_close_command )

  @ProtocolClass.register(60) # TODO: 0.9.1 should be 50
  def _x_close(self, method_frame=None):
    '''Called by broker to close this connection'''
    # Ack with a close-ok
    self.send_frame( MethodFrame(self.channel_id, 10, 61, None) )

    # Tell the connection
    self.channel.connection.handle_close( method_frame.args )
    
  def close(self, reply_code=0, reply_text='', class_id=0, method_id=0):
    '''Send the close command.  If this was due to an error, class and method 
    ids should identify what method threw the error.'''
    args = Writer()
    args.write_short(reply_code)
    args.write_shortstr(reply_text)
    args.write_short(method_sig[0]) # class_id
    args.write_short(method_sig[1]) # method_id
    self.send_frame( MethodFrame(10, 60, args) )

  @ProtocolClass.register(61) # TODO: 0.9.1 should be 51
  def close_ok(self, method_frame):
    '''Called by broker to acknowledge that connection is done.'''
    self.channel.connection.handle_close_ok( method_frame.args )


  ###
  ### Private support methods
  ###
  def _send_open(self):
    # TODO: similar, figure out how to make this better in Connection
    args = Writer()
    args.write_shortstr( self.channel.connection._vhost )
    args.write_shortstr('') # capabilities
    args.write_bit(insist)  # TODO: 0.9.1 ???
    self.send_frame( MethodFrame(self.channel_id, 10, 40, args) )
