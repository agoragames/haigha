from haigha.writer import Writer
from haigha.classes import ProtocolClass
from haigha.frames import MethodFrame

class ExchangeClass(ProtocolClass):
  '''
  Implements the AMQP Exchange class
  '''

  def __init__(self, *args, **kwargs):
    super(ExchangeClass, self).__init__(*args, **kwargs)
    self.dispatch_map = {
      11 : self._recv_declare_ok,
      21 : self._recv_delete_ok,
    }


  def declare(self, exchange, type, passive=False, durable=False,\
      auto_delete=True, internal=False, nowait=True, arguments=None, ticket=None):
    """
    Declare the exchange.

    exchange - The name of the exchange to declare
    type - One of 
    """
    if arguments is None:
      arguments = {}

    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(exchange)
    args.write_shortstr(type)
    #args.write_bit(passive)
    #args.write_bit(durable)
    #args.write_bit(auto_delete)
    #args.write_bit(internal)
    #args.write_bit(nowait)
    args.write_bits(passive, durable, auto_delete, internal, nowait)
    args.write_table(arguments)
    self.send_frame( MethodFrame(self.channel_id, 40, 10, args) )

    if not nowait:
      self.channel.add_synchronous_cb( self._recv_declare_ok )
    
  def delete(self, exchange, if_unused=False, nowait=True, ticket=None):
    '''
    Delete an exchange.
    '''
    args = Writer()
    if ticket is not None:
      args.write_short(ticket)
    else:
      args.write_short(self.default_ticket)
    args.write_shortstr(exchange)
    #args.write_bit(if_unused)
    #args.write_bit(nowait)
    args.write_bits(if_unused, nowait)
    self.send_frame( MethodFrame(self.channel_id, 40, 20, args) )
    
    if not nowait:
      self.channel.add_synchronous_cb( self._recv_delete_ok )

  def _recv_declare_ok(self, method_frame):
    '''
    Confirmation that exchange was declared.
    '''
    # No arguments in method frame, nothing to do

  def _recv_delete_ok(self, method_frame):
    '''
    Confirmation that exchange was deleted.
    '''
    # No arguments in method frame, nothing to do
